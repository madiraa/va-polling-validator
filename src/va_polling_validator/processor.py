"""CSV processing and validation orchestration."""

import asyncio
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.table import Table

from .models import VoterRecord, ValidationResult, ValidationProgress, ValidatorConfig, MatchStatus
from .validator import VAPollingValidator, CheckpointManager

console = Console()


def generate_job_id(file_path: Path) -> str:
    """Generate a unique job ID based on file path and modification time."""
    stat = file_path.stat()
    content = f"{file_path.absolute()}_{stat.st_mtime}_{stat.st_size}"
    return hashlib.md5(content.encode()).hexdigest()[:12]


def load_csv(file_path: Path) -> tuple[pd.DataFrame, list[VoterRecord]]:
    """Load CSV and convert to VoterRecord objects."""
    df = pd.read_csv(file_path)
    
    column_mapping = {
        'reg_address': ['reg_address', 'address', 'street_address', 'voter_address'],
        'reg_city': ['reg_city', 'city'],
        'reg_state': ['reg_state', 'state'],
        'reg_zip': ['reg_zip', 'zip', 'zipcode', 'zip_code'],
        'polling_place_name': ['polling_place_name', 'polling_place', 'poll_place', 'polling_location'],
        'polling_place_address': ['polling_place_address_full', 'polling_place_address', 'poll_address'],
        'precinct_code': ['ts_vb_vf_national_precinct_code', 'precinct_code', 'precinct'],
    }
    
    normalized_cols = {}
    for target, sources in column_mapping.items():
        for source in sources:
            matching = [c for c in df.columns if c.lower() == source.lower()]
            if matching:
                normalized_cols[target] = matching[0]
                break
    
    required = ['reg_address', 'reg_city', 'reg_zip', 'polling_place_name']
    missing = [r for r in required if r not in normalized_cols]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Available: {list(df.columns)}")
    
    records = []
    for idx, row in df.iterrows():
        record = VoterRecord(
            row_index=idx,
            precinct_code=str(row.get(normalized_cols.get('precinct_code', ''), '')) or None,
            reg_address=str(row[normalized_cols['reg_address']]),
            reg_city=str(row[normalized_cols['reg_city']]),
            reg_state=str(row.get(normalized_cols.get('reg_state', ''), 'VA')) or 'VA',
            reg_zip=str(row[normalized_cols['reg_zip']]),
            polling_place_name=str(row[normalized_cols['polling_place_name']]),
            polling_place_address=str(row.get(normalized_cols.get('polling_place_address', ''), '')) or None,
        )
        records.append(record)
    
    return df, records


def save_results(
    original_df: pd.DataFrame,
    results: list[ValidationResult],
    output_path: Path
):
    """Save validation results to CSV."""
    df = original_df.copy()
    
    results_by_idx = {r.row_index: r for r in results}
    
    df['va_polling_place_returned'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).va_polling_place
    )
    df['va_polling_address_returned'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).va_polling_address
    )
    df['matches_va'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).matches_va
    )
    df['match_score'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).match_score
    )
    df['validation_status'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).status.value
    )
    df['validation_timestamp'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).validation_timestamp.isoformat()
    )
    df['validation_notes'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).notes
    )
    df['validation_error'] = df.index.map(
        lambda i: results_by_idx.get(i, ValidationResult(row_index=i, input_address='', input_polling_place='')).error_message
    )
    
    df.to_csv(output_path, index=False)
    return df


async def run_validation(
    input_path: Path,
    output_path: Optional[Path] = None,
    config: Optional[ValidatorConfig] = None,
    checkpoint_dir: Optional[Path] = None,
    resume: bool = True,
    progress_callback: Optional[Callable[[ValidationProgress], None]] = None,
) -> tuple[list[ValidationResult], ValidationProgress]:
    """
    Run validation on a CSV file.
    
    Args:
        input_path: Path to input CSV
        output_path: Path for output CSV (default: input_validated.csv)
        config: Validator configuration
        checkpoint_dir: Directory for checkpoints (default: .va_validator_checkpoints)
        resume: Whether to resume from checkpoint if available
        progress_callback: Optional callback for progress updates
    
    Returns:
        Tuple of (results list, final progress)
    """
    config = config or ValidatorConfig()
    input_path = Path(input_path)
    
    if output_path is None:
        output_path = input_path.parent / f"{input_path.stem}_validated.csv"
    
    if checkpoint_dir is None:
        checkpoint_dir = input_path.parent / ".va_validator_checkpoints"
    
    checkpoint_mgr = CheckpointManager(checkpoint_dir)
    job_id = generate_job_id(input_path)
    
    console.print(f"\n[bold blue]VA Polling Place Validator[/bold blue]")
    console.print(f"Job ID: {job_id}")
    console.print(f"Input: {input_path}")
    console.print(f"Output: {output_path}\n")
    
    df, records = load_csv(input_path)
    total_records = len(records)
    
    results: list[ValidationResult] = []
    completed_indices: set[int] = set()
    
    if resume:
        saved_progress, saved_results, completed_indices = checkpoint_mgr.load_checkpoint(job_id)
        if saved_progress and saved_results:
            console.print(f"[yellow]Resuming from checkpoint: {saved_progress.completed_records}/{total_records} completed[/yellow]\n")
            results = saved_results
    
    progress = ValidationProgress(
        job_id=job_id,
        total_records=total_records,
        completed_records=len(completed_indices),
        matched=sum(1 for r in results if r.status == MatchStatus.MATCH),
        mismatched=sum(1 for r in results if r.status == MatchStatus.MISMATCH),
        not_found=sum(1 for r in results if r.status == MatchStatus.NOT_FOUND),
        errors=sum(1 for r in results if r.status == MatchStatus.ERROR),
    )
    
    pending_records = [r for r in records if r.row_index not in completed_indices]
    
    if not pending_records:
        console.print("[green]All records already validated![/green]")
        save_results(df, results, output_path)
        return results, progress
    
    async with VAPollingValidator(config) as validator:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress_bar:
            task = progress_bar.add_task(
                f"Validating {len(pending_records)} records...",
                total=len(pending_records)
            )
            
            for i, record in enumerate(pending_records):
                result = await validator.validate_record(record)
                results.append(result)
                completed_indices.add(record.row_index)
                
                progress.completed_records += 1
                progress.last_updated = datetime.now()
                
                if result.status == MatchStatus.MATCH:
                    progress.matched += 1
                elif result.status == MatchStatus.MISMATCH:
                    progress.mismatched += 1
                elif result.status == MatchStatus.NOT_FOUND:
                    progress.not_found += 1
                else:
                    progress.errors += 1
                
                if progress_callback:
                    progress_callback(progress)
                
                if (i + 1) % config.checkpoint_interval == 0:
                    checkpoint_mgr.save_checkpoint(job_id, progress, results)
                
                progress_bar.update(task, advance=1)
    
    checkpoint_mgr.save_checkpoint(job_id, progress, results)
    
    result_df = save_results(df, results, output_path)
    
    console.print("\n[bold green]Validation Complete![/bold green]\n")
    
    table = Table(title="Validation Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    table.add_column("Percentage", style="green")
    
    table.add_row("Total Records", str(progress.total_records), "100%")
    table.add_row("Matched", str(progress.matched), f"{progress.matched/progress.total_records*100:.1f}%")
    table.add_row("Mismatched", str(progress.mismatched), f"{progress.mismatched/progress.total_records*100:.1f}%")
    table.add_row("Not Found", str(progress.not_found), f"{progress.not_found/progress.total_records*100:.1f}%")
    table.add_row("Errors", str(progress.errors), f"{progress.errors/progress.total_records*100:.1f}%")
    
    console.print(table)
    console.print(f"\n[blue]Results saved to:[/blue] {output_path}")
    
    if progress.mismatched > 0:
        console.print(f"\n[yellow]⚠ {progress.mismatched} mismatches found. Review the output CSV.[/yellow]")
        
        mismatch_table = Table(title="Mismatched Records (first 5)")
        mismatch_table.add_column("Row", style="cyan")
        mismatch_table.add_column("Input Polling Place", style="red")
        mismatch_table.add_column("VA Returns", style="green")
        mismatch_table.add_column("Score", style="yellow")
        
        mismatches = [r for r in results if r.status == MatchStatus.MISMATCH][:5]
        for r in mismatches:
            mismatch_table.add_row(
                str(r.row_index),
                r.input_polling_place[:30] + "..." if len(r.input_polling_place) > 30 else r.input_polling_place,
                (r.va_polling_place[:30] + "..." if r.va_polling_place and len(r.va_polling_place) > 30 else r.va_polling_place) or "N/A",
                f"{r.match_score:.0f}%"
            )
        
        console.print(mismatch_table)
    
    return results, progress
