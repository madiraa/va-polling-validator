"""Command-line interface for VA Polling Validator."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from .models import ValidatorConfig
from .processor import run_validation

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
):
    """
    Validate polling places in a CSV against Virginia elections website.
    
    The input CSV must have columns: reg_address, reg_city, reg_zip, polling_place_name
    
    Examples:
    
        va-validate voters.csv
        
        va-validate voters.csv -o results.csv --threshold 90
        
        va-validate large_dataset.csv --delay 3 --checkpoint-interval 5
    """
    config = ValidatorConfig(
        match_threshold=match_threshold,
        request_delay=delay,
        headless=headless,
        checkpoint_interval=checkpoint_interval,
    )
    
    try:
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
