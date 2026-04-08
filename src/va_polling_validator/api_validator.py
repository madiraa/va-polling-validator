"""Fast validation using Google Civic Information API."""

import asyncio
import os
from datetime import datetime
from typing import Optional, Callable
from urllib.parse import urlencode

import aiohttp
from rich.console import Console

from .models import VoterRecord, ValidationResult, ValidationProgress, ValidatorConfig, MatchStatus
from .validator import calculate_match_score

console = Console()

CIVIC_API_BASE = "https://www.googleapis.com/civicinfo/v2"


class CivicAPIError(Exception):
    """Error from Google Civic Information API."""
    pass


class CivicAPIValidator:
    """Fast validation using Google Civic Information API."""
    
    def __init__(
        self,
        api_key: str,
        config: Optional[ValidatorConfig] = None,
        requests_per_second: float = 10.0,
    ):
        self.api_key = api_key
        self.config = config or ValidatorConfig()
        self.delay = 1.0 / requests_per_second
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def start(self):
        """Initialize HTTP session."""
        self.session = aiohttp.ClientSession()
    
    async def stop(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    async def get_elections(self) -> list[dict]:
        """Get list of available elections."""
        if not self.session:
            raise RuntimeError("Session not initialized. Call start() first.")
        
        url = f"{CIVIC_API_BASE}/elections?key={self.api_key}"
        
        async with self.session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise CivicAPIError(f"API error {resp.status}: {text}")
            
            data = await resp.json()
            return data.get("elections", [])
    
    async def lookup_polling_place(
        self, 
        address: str,
        election_id: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Look up polling place for an address.
        
        Returns: (polling_place_name, polling_place_address, error_message)
        """
        if not self.session:
            raise RuntimeError("Session not initialized. Call start() first.")
        
        params = {
            "key": self.api_key,
            "address": address,
        }
        if election_id:
            params["electionId"] = election_id
        
        url = f"{CIVIC_API_BASE}/voterinfo?{urlencode(params)}"
        
        try:
            async with self.session.get(url) as resp:
                data = await resp.json()
                
                if resp.status == 400:
                    error = data.get("error", {}).get("message", "Bad request")
                    return None, None, error
                
                if resp.status != 200:
                    return None, None, f"API error {resp.status}"
                
                status = data.get("status", "")
                
                if status == "noStreetSegmentFound":
                    return None, None, "No polling place found for this address"
                
                if status == "addressUnparseable":
                    return None, None, "Address could not be parsed"
                
                polling_locations = data.get("pollingLocations", [])
                
                if not polling_locations:
                    early_sites = data.get("earlyVoteSites", [])
                    if early_sites:
                        polling_locations = early_sites
                
                if not polling_locations:
                    return None, None, "No polling locations in response"
                
                location = polling_locations[0]
                addr = location.get("address", {})
                
                name = addr.get("locationName", "")
                
                address_parts = []
                if addr.get("line1"):
                    address_parts.append(addr["line1"])
                if addr.get("city"):
                    address_parts.append(addr["city"])
                if addr.get("state"):
                    address_parts.append(addr["state"])
                if addr.get("zip"):
                    address_parts.append(addr["zip"])
                
                full_address = ", ".join(address_parts) if address_parts else None
                
                return name, full_address, None
                
        except aiohttp.ClientError as e:
            return None, None, f"Network error: {e}"
    
    async def validate_record(self, record: VoterRecord) -> ValidationResult:
        """Validate a single voter record."""
        result = ValidationResult(
            row_index=record.row_index,
            input_address=record.full_address,
            input_polling_place=record.polling_place_name,
            input_polling_address=record.polling_place_address,
        )
        
        va_name, va_address, error = await self.lookup_polling_place(record.full_address)
        
        if error:
            if "No polling place" in error or "No polling locations" in error:
                result.status = MatchStatus.NOT_FOUND
                result.notes = error
            else:
                result.status = MatchStatus.ERROR
                result.error_message = error
        elif va_name:
            result.va_polling_place = va_name
            result.va_polling_address = va_address
            
            result.match_score = calculate_match_score(record.polling_place_name, va_name)
            
            if result.match_score >= self.config.match_threshold:
                result.status = MatchStatus.MATCH
                result.notes = f"Match score: {result.match_score:.1f}%"
            else:
                result.status = MatchStatus.MISMATCH
                result.notes = f"Low match score: {result.match_score:.1f}%"
        else:
            result.status = MatchStatus.NOT_FOUND
            result.notes = "No polling place returned"
        
        result.validation_timestamp = datetime.now()
        
        await asyncio.sleep(self.delay)
        
        return result
    
    async def validate_batch(
        self,
        records: list[VoterRecord],
        progress_callback: Optional[Callable[[ValidationProgress], None]] = None,
        concurrency: int = 5,
    ) -> list[ValidationResult]:
        """
        Validate multiple records with controlled concurrency.
        
        Args:
            records: List of voter records to validate
            progress_callback: Callback for progress updates
            concurrency: Number of concurrent requests (be respectful, keep low)
        """
        results: list[ValidationResult] = []
        semaphore = asyncio.Semaphore(concurrency)
        
        progress = ValidationProgress(
            job_id="api_validation",
            total_records=len(records),
        )
        
        async def validate_with_semaphore(record: VoterRecord) -> ValidationResult:
            async with semaphore:
                result = await self.validate_record(record)
                
                results.append(result)
                progress.completed_records = len(results)
                
                if result.status == MatchStatus.MATCH:
                    progress.matched += 1
                elif result.status == MatchStatus.MISMATCH:
                    progress.mismatched += 1
                elif result.status == MatchStatus.NOT_FOUND:
                    progress.not_found += 1
                else:
                    progress.errors += 1
                
                progress.last_updated = datetime.now()
                
                if progress_callback:
                    progress_callback(progress)
                
                return result
        
        tasks = [validate_with_semaphore(record) for record in records]
        await asyncio.gather(*tasks)
        
        return sorted(results, key=lambda r: r.row_index)


async def run_api_validation(
    records: list[VoterRecord],
    api_key: str,
    config: Optional[ValidatorConfig] = None,
    requests_per_second: float = 10.0,
    concurrency: int = 5,
    progress_callback: Optional[Callable[[ValidationProgress], None]] = None,
) -> list[ValidationResult]:
    """
    Run validation using Google Civic Information API.
    
    Args:
        records: List of voter records to validate
        api_key: Google Civic Information API key
        config: Validator configuration
        requests_per_second: Max requests per second (default 10, be respectful)
        concurrency: Number of concurrent requests
        progress_callback: Callback for progress updates
    
    Returns:
        List of validation results
    """
    config = config or ValidatorConfig()
    
    console.print(f"\n[bold blue]VA Polling Validator (API Mode)[/bold blue]")
    console.print(f"Records: {len(records)} | Rate: {requests_per_second} req/s | Concurrency: {concurrency}")
    console.print(f"Estimated time: {len(records) / requests_per_second / 60:.1f} minutes\n")
    
    async with CivicAPIValidator(api_key, config, requests_per_second) as validator:
        try:
            elections = await validator.get_elections()
            console.print(f"[green]API connected. {len(elections)} elections available.[/green]\n")
        except CivicAPIError as e:
            console.print(f"[red]API Error: {e}[/red]")
            raise
        
        results = await validator.validate_batch(
            records,
            progress_callback=progress_callback,
            concurrency=concurrency,
        )
    
    return results


def get_api_key() -> Optional[str]:
    """Get API key from environment or config."""
    return os.environ.get("GOOGLE_CIVIC_API_KEY") or os.environ.get("CIVIC_API_KEY")
