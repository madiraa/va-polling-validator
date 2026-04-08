# VA Polling Place Validator

Validate Virginia polling place assignments against official Virginia Department of Elections data.

## Features

- **Browser Automation**: Uses Playwright to query the official VA elections website
- **Fuzzy Matching**: Handles variations in polling place names (e.g., "CHINCOTEAGUE CENTER" vs "Chincoteague Center")
- **Checkpointing**: Automatically saves progress - resume interrupted validations
- **Large Dataset Support**: Process thousands of records with rate limiting and progress tracking
- **Web Interface**: Drag-and-drop CSV upload with real-time progress
- **Detailed Reports**: CSV output with match status, confidence scores, and timestamps

## Quick Start

### Installation

```bash
cd va_polling_validator

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e ".[webapp]"

# Install Playwright browsers
playwright install chromium
```

### CLI Usage

```bash
# Basic validation
va-validate your_data.csv

# With custom settings
va-validate your_data.csv -o results.csv --threshold 90 --delay 3

# Resume interrupted validation (automatic)
va-validate your_data.csv

# Clear checkpoints and start fresh
va-validate your_data.csv --no-resume

# Show help
va-validate --help
```

### Web App Usage

```bash
# Start the backend server
cd webapp/backend
python main.py

# Open frontend in browser
open webapp/frontend/index.html
# Or serve it: python -m http.server 3000 -d webapp/frontend
```

Then navigate to `http://localhost:3000` and drag-drop your CSV file.

## Input CSV Format

Your CSV must have these columns (case-insensitive):

| Column | Required | Description |
|--------|----------|-------------|
| `reg_address` | Yes | Street address (e.g., "7390 RACING MOON LNDG") |
| `reg_city` | Yes | City (e.g., "CHINCOTEAGUE") |
| `reg_zip` | Yes | ZIP code (e.g., "23336") |
| `polling_place_name` | Yes | Expected polling place name |
| `reg_state` | No | State (defaults to "VA") |
| `polling_place_address_full` | No | Full polling place address |

## Output CSV

The validated CSV includes all original columns plus:

| Column | Description |
|--------|-------------|
| `va_polling_place_returned` | Polling place name returned by VA website |
| `va_polling_address_returned` | Address returned by VA website |
| `matches_va` | 1 = match, 0 = mismatch, -1 = not found/error |
| `match_score` | Fuzzy match confidence (0-100) |
| `validation_status` | `match`, `mismatch`, `not_found`, or `error` |
| `validation_timestamp` | When the record was validated |
| `validation_notes` | Additional details |

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--threshold` | 85 | Minimum fuzzy match score (0-100) to consider a match |
| `--delay` | 2.0 | Seconds between requests (respect VA servers) |
| `--checkpoint-interval` | 10 | Save progress every N records |
| `--headless/--no-headless` | headless | Run browser visibly for debugging |

## Handling Large Datasets

For datasets with thousands of records:

1. **Checkpointing**: Progress is saved every 10 records (configurable). If interrupted, just run the same command to resume.

2. **Rate Limiting**: Default 2-second delay between requests. Increase for very large datasets to avoid overwhelming VA servers.

3. **Estimated Time**: With 2-second delays, expect ~3 records/minute, or ~180 records/hour.

```bash
# For 10,000 records, estimate ~55 hours
# Consider running overnight or in batches
va-validate large_dataset.csv --delay 3 --checkpoint-interval 50
```

## Troubleshooting

### "No upcoming elections for this address"
The VA website only shows polling places during active election periods. This is expected for many addresses.

### Low match scores
The fuzzy matcher handles variations, but very different names may score low. Review mismatches manually in the output CSV.

### Timeouts or errors
- Check your internet connection
- Try increasing the delay: `--delay 5`
- Run with visible browser for debugging: `--no-headless`

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy src/
```

## License

MIT
