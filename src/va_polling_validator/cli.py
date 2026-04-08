"""Command-line interface for VA Polling Validator."""

import asyncio
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.table import Table

from .models import ValidatorConfig, MatchStatus
from .processor import run_validation, load_csv, save_results

app = typer.Typer(
    name="va-validate",
    help="Validate Virginia polling place assignments against official VA elections data.",
    add_completion=False,
)

console = Console()


@app.command()
def validate(
    input_file: Path = typer.Argument(
        ...,
        help="Path to input CSV file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Path for output CSV file (default: input_validated.csv)",
    ),
    match_threshold: float = typer.Option(
        85.0,
        "--threshold", "-t",
        help="Minimum fuzzy match score (0-100) to consider a match",
    ),
    delay: float = typer.Option(
        2.0,
        "--delay", "-d",
        help="Delay between requests in seconds (be respectful to VA servers)",
    ),
    no_resume: bool = typer.Option(
        False,
        "--no-resume",
        help="Start fresh instead of resuming from checkpoint",
    ),
    headless: bool = typer.Option(
        True,
        "--headless/--no-headless",
        help="Run browser in headless mode",
    ),
    checkpoint_interval: int = typer.Option(
        10,
        "--checkpoint-interval",
        help="Save checkpoint every N records",
    ),
    parallel: int = typer.Option(
        1,
        "--parallel", "-p",
        help="Number of parallel browser instances (2-4 recommended for speed)",
    ),
    precinct_cache: bool = typer.Option(
        False,
        "--precinct-cache/--no-precinct-cache",
        help="Only validate one address per precinct (much faster for large datasets)",
    ),
    use_api: bool = typer.Option(
        False,
        "--api",
        help="Use Google Civic Information API (10x faster, requires API key)",
    ),
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        envvar="GOOGLE_CIVIC_API_KEY",
        help="Google Civic Information API key (or set GOOGLE_CIVIC_API_KEY env var)",
    ),
    rate_limit: float = typer.Option(
        10.0,
        "--rate-limit",
        help="API requests per second (default 10, max recommended 25)",
    ),
):
    """
    Validate polling places in a CSV against Virginia elections website.
    
    The input CSV must have columns: reg_address, reg_city, reg_zip, polling_place_name
    
    Examples:
    
        # Browser-based (slower but no API key needed)
        va-validate voters.csv
        
        # API-based (10x faster, requires API key)
        va-validate voters.csv --api --api-key YOUR_KEY
        
        # Or set env var and just use --api
        export GOOGLE_CIVIC_API_KEY=your_key
        va-validate voters.csv --api
        
        # Browser with parallel workers
        va-validate large_dataset.csv --parallel 3 --precinct-cache
    """
    config = ValidatorConfig(
        match_threshold=match_threshold,
        request_delay=delay,
        headless=headless,
        checkpoint_interval=checkpoint_interval,
    )
    
    try:
        if use_api:
            if not api_key:
                api_key = os.environ.get("GOOGLE_CIVIC_API_KEY") or os.environ.get("CIVIC_API_KEY")
            
            if not api_key:
                console.print("[red]Error: API key required. Use --api-key or set GOOGLE_CIVIC_API_KEY env var[/red]")
                console.print("\nTo get an API key:")
                console.print("1. Go to https://console.cloud.google.com/apis/credentials")
                console.print("2. Create a project (if needed)")
                console.print("3. Enable 'Google Civic Information API'")
                console.print("4. Create an API key")
                raise typer.Exit(1)
            
            from .api_validator import run_api_validation
            
            df, records = load_csv(input_file)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress_bar:
                task = progress_bar.add_task(f"Validating {len(records)} records...", total=len(records))
                
                def update_progress(prog):
                    progress_bar.update(task, completed=prog.completed_records)
                
                results = asyncio.run(
                    run_api_validation(
                        records=records,
                        api_key=api_key,
                        config=config,
                        requests_per_second=rate_limit,
                        concurrency=min(5, int(rate_limit)),
                        progress_callback=update_progress,
                    )
                )
            
            out_path = output_file or input_file.parent / f"{input_file.stem}_validated.csv"
            save_results(df, results, out_path)
            
            matched = sum(1 for r in results if r.status == MatchStatus.MATCH)
            mismatched = sum(1 for r in results if r.status == MatchStatus.MISMATCH)
            not_found = sum(1 for r in results if r.status == MatchStatus.NOT_FOUND)
            errors = sum(1 for r in results if r.status == MatchStatus.ERROR)
            
            console.print("\n[bold green]Validation Complete![/bold green]\n")
            
            table = Table(title="Results Summary")
            table.add_column("Status", style="cyan")
            table.add_column("Count", style="magenta")
            table.add_column("Percentage", style="green")
            
            total = len(results)
            table.add_row("Matched", str(matched), f"{matched/total*100:.1f}%")
            table.add_row("Mismatched", str(mismatched), f"{mismatched/total*100:.1f}%")
            table.add_row("Not Found", str(not_found), f"{not_found/total*100:.1f}%")
            table.add_row("Errors", str(errors), f"{errors/total*100:.1f}%")
            
            console.print(table)
            console.print(f"\n[blue]Results saved to:[/blue] {out_path}")
            
        elif parallel > 1 or precinct_cache:
            from .parallel_validator import run_parallel_validation
            
            console.print(f"\n[bold blue]VA Polling Place Validator (Parallel Mode)[/bold blue]")
            console.print(f"Workers: {parallel} | Precinct Cache: {precinct_cache}\n")
            
            df, records = load_csv(input_file)
            
            results = asyncio.run(
                run_parallel_validation(
                    records=records,
                    config=config,
                    num_workers=parallel,
                    use_precinct_cache=precinct_cache,
                )
            )
            
            out_path = output_file or input_file.parent / f"{input_file.stem}_validated.csv"
            save_results(df, results, out_path)
            console.print(f"\n[green]Results saved to:[/green] {out_path}")
        else:
            asyncio.run(
                run_validation(
                    input_path=input_file,
                    output_path=output_file,
                    config=config,
                    resume=not no_resume,
                )
            )
    except KeyboardInterrupt:
        console.print("\n[yellow]Validation interrupted. Progress has been saved.[/yellow]")
        console.print("Run the same command to resume from checkpoint.")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def clear_checkpoints(
    input_file: Path = typer.Argument(
        ...,
        help="Path to input CSV file (to identify which checkpoints to clear)",
        exists=True,
    ),
):
    """Clear checkpoint files for a specific input file."""
    from .processor import generate_job_id
    from .validator import CheckpointManager
    
    job_id = generate_job_id(input_file)
    checkpoint_dir = input_file.parent / ".va_validator_checkpoints"
    
    if checkpoint_dir.exists():
        mgr = CheckpointManager(checkpoint_dir)
        mgr.clear_checkpoint(job_id)
        console.print(f"[green]Cleared checkpoints for job {job_id}[/green]")
    else:
        console.print("[yellow]No checkpoints found[/yellow]")


@app.command()
def info():
    """Show information about the validator."""
    from . import __version__
    
    console.print(f"\n[bold blue]VA Polling Place Validator v{__version__}[/bold blue]\n")
    console.print("Validates Virginia polling place assignments against the official")
    console.print("Virginia Department of Elections website.\n")
    console.print("[bold]Features:[/bold]")
    console.print("  • Fuzzy matching for polling place names")
    console.print("  • Checkpointing for large datasets (resume on interruption)")
    console.print("  • Progress tracking and detailed reports")
    console.print("  • Rate limiting to respect VA servers\n")
    console.print("[bold]Required CSV columns:[/bold]")
    console.print("  • reg_address - Street address")
    console.print("  • reg_city - City")
    console.print("  • reg_zip - ZIP code")
    console.print("  • polling_place_name - Expected polling place name\n")
    console.print("[bold]Optional columns:[/bold]")
    console.print("  • reg_state - State (defaults to VA)")
    console.print("  • polling_place_address_full - Full polling place address")
    console.print("  • ts_vb_vf_national_precinct_code - Precinct code\n")


if __name__ == "__main__":
    app()
