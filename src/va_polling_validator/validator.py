"""Core validation logic using Playwright browser automation."""

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page, Browser
from rapidfuzz import fuzz
from rich.console import Console

from .models import (
    VoterRecord, 
    ValidationResult, 
    ValidationProgress,
    ValidatorConfig, 
    MatchStatus
)

console = Console()

VA_ELECTIONS_URL = "https://www.elections.virginia.gov/casting-a-ballot/polling-place-lookup/"


def normalize_name(name: str) -> str:
    """Normalize polling place name for comparison."""
    name = name.upper().strip()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name


def calculate_match_score(name1: str, name2: str) -> float:
    """Calculate fuzzy match score between two names."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    
    ratio = fuzz.ratio(n1, n2)
    partial = fuzz.partial_ratio(n1, n2)
    token_sort = fuzz.token_sort_ratio(n1, n2)
    token_set = fuzz.token_set_ratio(n1, n2)
    
    return max(ratio, partial, token_sort, token_set)


class VAPollingValidator:
    """Validates polling places against Virginia elections website."""
    
    def __init__(self, config: Optional[ValidatorConfig] = None):
        self.config = config or ValidatorConfig()
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._playwright = None
    
    async def start(self):
        """Initialize the browser."""
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(
            headless=self.config.headless
        )
        self.page = await self.browser.new_page()
        await self.page.goto(VA_ELECTIONS_URL)
        await self.page.wait_for_load_state("networkidle")
    
    async def stop(self):
        """Close the browser."""
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    async def lookup_polling_place(self, address: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Look up polling place for an address.
        
        Returns: (polling_place_name, polling_place_address, error_message)
        """
        if not self.page:
            raise RuntimeError("Browser not initialized. Call start() first.")
        
        try:
            await self.page.goto(VA_ELECTIONS_URL)
            await self.page.wait_for_load_state("networkidle")
            
            address_input = self.page.locator('input[placeholder="Enter your address"]')
            await address_input.fill(address)
            
            search_button = self.page.locator('button:has-text("search")').first
            await search_button.click()
            
            await asyncio.sleep(2)
            await self.page.wait_for_load_state("networkidle")
            
            no_info = await self.page.locator('text="We don\'t have any info"').count()
            if no_info > 0:
                return None, None, "No upcoming elections for this address"
            
            polling_location = self.page.locator('h3:has-text("Election Day Voting Site")').first
            try:
                await polling_location.wait_for(timeout=5000)
            except:
                return None, None, "Could not find polling location in results"
            
            parent = polling_location.locator('xpath=..')
            
            name_elem = parent.locator('text=/^[A-Z][A-Z\s\.\-\']+$/').first
            polling_name = None
            try:
                polling_name = await name_elem.text_content(timeout=2000)
            except:
                all_text = await parent.text_content()
                lines = [l.strip() for l in all_text.split('\n') if l.strip()]
                for line in lines:
                    if line.isupper() and len(line) > 3 and 'ELECTION' not in line and 'VOTING' not in line:
                        polling_name = line
                        break
            
            address_parts = []
            try:
                street = await parent.locator('text=/^\\d+.*(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Ct|Pl)/i').first.text_content(timeout=2000)
                address_parts.append(street.strip())
            except:
                pass
            
            try:
                city_state = await parent.locator('text=/^[A-Za-z]+,\\s*VA\\s*\\d{5}/').first.text_content(timeout=2000)
                address_parts.append(city_state.strip())
            except:
                pass
            
            polling_address = ", ".join(address_parts) if address_parts else None
            
            return polling_name, polling_address, None
            
        except Exception as e:
            return None, None, str(e)
    
    async def validate_record(self, record: VoterRecord) -> ValidationResult:
        """Validate a single voter record."""
        result = ValidationResult(
            row_index=record.row_index,
            input_address=record.full_address,
            input_polling_place=record.polling_place_name,
            input_polling_address=record.polling_place_address,
        )
        
        for attempt in range(self.config.max_retries):
            try:
                va_name, va_address, error = await self.lookup_polling_place(record.full_address)
                
                if error:
                    if "No upcoming elections" in error:
                        result.status = MatchStatus.NOT_FOUND
                        result.error_message = error
                        result.notes = "Address not covered by current election"
                    else:
                        result.status = MatchStatus.ERROR
                        result.error_message = error
                    break
                
                result.va_polling_place = va_name
                result.va_polling_address = va_address
                
                if va_name:
                    result.match_score = calculate_match_score(
                        record.polling_place_name, va_name
                    )
                    
                    if result.match_score >= self.config.match_threshold:
                        result.status = MatchStatus.MATCH
                        result.notes = f"Match score: {result.match_score:.1f}%"
                    else:
                        result.status = MatchStatus.MISMATCH
                        result.notes = f"Low match score: {result.match_score:.1f}%"
                else:
                    result.status = MatchStatus.NOT_FOUND
                    result.notes = "No polling place returned"
                
                break
                
            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.request_delay)
                    continue
                result.status = MatchStatus.ERROR
                result.error_message = str(e)
        
        result.validation_timestamp = datetime.now()
        await asyncio.sleep(self.config.request_delay)
        
        return result


class CheckpointManager:
    """Manage checkpoints for resumable validation."""
    
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def get_checkpoint_path(self, job_id: str) -> Path:
        return self.checkpoint_dir / f"{job_id}_checkpoint.json"
    
    def get_results_path(self, job_id: str) -> Path:
        return self.checkpoint_dir / f"{job_id}_results.json"
    
    def save_checkpoint(self, job_id: str, progress: ValidationProgress, results: list[ValidationResult]):
        """Save current progress and results."""
        checkpoint_data = {
            "progress": progress.model_dump(mode="json"),
            "completed_indices": [r.row_index for r in results],
        }
        
        with open(self.get_checkpoint_path(job_id), 'w') as f:
            json.dump(checkpoint_data, f, default=str)
        
        results_data = [r.model_dump(mode="json") for r in results]
        with open(self.get_results_path(job_id), 'w') as f:
            json.dump(results_data, f, default=str, indent=2)
    
    def load_checkpoint(self, job_id: str) -> tuple[Optional[ValidationProgress], list[ValidationResult], set[int]]:
        """Load checkpoint if it exists."""
        checkpoint_path = self.get_checkpoint_path(job_id)
        results_path = self.get_results_path(job_id)
        
        if not checkpoint_path.exists():
            return None, [], set()
        
        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)
        
        progress = ValidationProgress(**checkpoint_data["progress"])
        completed_indices = set(checkpoint_data["completed_indices"])
        
        results = []
        if results_path.exists():
            with open(results_path, 'r') as f:
                results_data = json.load(f)
            results = [ValidationResult(**r) for r in results_data]
        
        return progress, results, completed_indices
    
    def clear_checkpoint(self, job_id: str):
        """Remove checkpoint files."""
        for path in [self.get_checkpoint_path(job_id), self.get_results_path(job_id)]:
            if path.exists():
                path.unlink()
