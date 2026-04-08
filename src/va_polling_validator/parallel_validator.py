"""Parallel validation with multiple browser instances and precinct caching."""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable
from collections import defaultdict

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from .models import VoterRecord, ValidationResult, ValidationProgress, ValidatorConfig, MatchStatus
from .validator import VAPollingValidator, CheckpointManager, calculate_match_score

console = Console()


class ParallelValidator:
    """Run multiple browser instances in parallel with rate limiting."""
    
    def __init__(
        self, 
        config: ValidatorConfig,
        num_workers: int = 3,
        stagger_delay: float = 2.0,
    ):
        self.config = config
        self.num_workers = num_workers
        self.stagger_delay = stagger_delay
        self.validators: list[VAPollingValidator] = []
        
    async def start(self):
        """Initialize all browser instances with staggered starts."""
        console.print(f"[cyan]Starting {self.num_workers} parallel browsers...[/cyan]")
        
        for i in range(self.num_workers):
            validator = VAPollingValidator(self.config)
            await validator.start()
            self.validators.append(validator)
            if i < self.num_workers - 1:
                await asyncio.sleep(self.stagger_delay)
        
        console.print(f"[green]All {self.num_workers} browsers ready[/green]")
    
    async def stop(self):
        """Close all browser instances."""
        for validator in self.validators:
            await validator.stop()
        self.validators = []
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    async def validate_batch(
        self,
        records: list[VoterRecord],
        progress_callback: Optional[Callable[[int, ValidationResult], None]] = None,
    ) -> list[ValidationResult]:
        """Validate records using worker pool."""
        results: list[ValidationResult] = []
        queue = asyncio.Queue()
        
        for record in records:
            await queue.put(record)
        
        async def worker(worker_id: int, validator: VAPollingValidator):
            while True:
                try:
                    record = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                result = await validator.validate_record(record)
                results.append(result)
                
                if progress_callback:
                    progress_callback(worker_id, result)
                
                queue.task_done()
        
        workers = [
            asyncio.create_task(worker(i, v)) 
            for i, v in enumerate(self.validators)
        ]
        
        await asyncio.gather(*workers)
        
        return sorted(results, key=lambda r: r.row_index)


def group_by_precinct(records: list[VoterRecord]) -> dict[str, list[VoterRecord]]:
    """Group records by precinct code."""
    groups = defaultdict(list)
    for record in records:
        key = record.precinct_code or f"no_precinct_{record.row_index}"
        groups[key].append(record)
    return dict(groups)


async def run_parallel_validation(
    records: list[VoterRecord],
    config: Optional[ValidatorConfig] = None,
    num_workers: int = 3,
    use_precinct_cache: bool = True,
    progress_callback: Optional[Callable[[ValidationProgress], None]] = None,
) -> list[ValidationResult]:
    """
    Run validation with parallel browsers and optional precinct caching.
    
    Args:
        records: List of voter records to validate
        config: Validator configuration
        num_workers: Number of parallel browser instances
        use_precinct_cache: If True, only validate one address per precinct
        progress_callback: Callback for progress updates
    """
    config = config or ValidatorConfig()
    config.request_delay = max(config.request_delay * num_workers, 4.0)
    
    all_results: list[ValidationResult] = []
    
    if use_precinct_cache:
        precinct_groups = group_by_precinct(records)
        console.print(f"[cyan]Found {len(precinct_groups)} unique precincts from {len(records)} records[/cyan]")
        
        representative_records = []
        for precinct, group in precinct_groups.items():
            representative_records.append(group[0])
        
        records_to_validate = representative_records
    else:
        records_to_validate = records
    
    progress = ValidationProgress(
        job_id="parallel",
        total_records=len(records),
        completed_records=0,
    )
    
    validated_count = 0
    
    def on_result(worker_id: int, result: ValidationResult):
        nonlocal validated_count
        validated_count += 1
        
        if use_precinct_cache:
            record = next(r for r in records if r.row_index == result.row_index)
            precinct = record.precinct_code or f"no_precinct_{record.row_index}"
            precinct_groups_local = group_by_precinct(records)
            group = precinct_groups_local.get(precinct, [record])
            
            for other_record in group:
                if other_record.row_index == result.row_index:
                    all_results.append(result)
                else:
                    derived_result = ValidationResult(
                        row_index=other_record.row_index,
                        input_address=other_record.full_address,
                        input_polling_place=other_record.polling_place_name,
                        input_polling_address=other_record.polling_place_address,
                        va_polling_place=result.va_polling_place,
                        va_polling_address=result.va_polling_address,
                        status=result.status if result.status in [MatchStatus.NOT_FOUND, MatchStatus.ERROR] else MatchStatus.PENDING,
                        match_score=0.0,
                        validation_timestamp=datetime.now(),
                        notes="Derived from precinct representative",
                    )
                    
                    if result.va_polling_place and derived_result.status == MatchStatus.PENDING:
                        derived_result.match_score = calculate_match_score(
                            other_record.polling_place_name,
                            result.va_polling_place
                        )
                        if derived_result.match_score >= config.match_threshold:
                            derived_result.status = MatchStatus.MATCH
                        else:
                            derived_result.status = MatchStatus.MISMATCH
                    
                    all_results.append(derived_result)
            
            progress.completed_records = len(all_results)
        else:
            all_results.append(result)
            progress.completed_records = validated_count
        
        progress.matched = sum(1 for r in all_results if r.status == MatchStatus.MATCH)
        progress.mismatched = sum(1 for r in all_results if r.status == MatchStatus.MISMATCH)
        progress.not_found = sum(1 for r in all_results if r.status == MatchStatus.NOT_FOUND)
        progress.errors = sum(1 for r in all_results if r.status == MatchStatus.ERROR)
        progress.last_updated = datetime.now()
        
        if progress_callback:
            progress_callback(progress)
    
    async with ParallelValidator(config, num_workers=num_workers) as validator:
        await validator.validate_batch(records_to_validate, on_result)
    
    return sorted(all_results, key=lambda r: r.row_index)
