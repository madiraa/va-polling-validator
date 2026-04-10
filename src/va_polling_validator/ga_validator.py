"""Georgia polling place validator using Playwright to automate the GA MVP portal.

The GA Secretary of State's My Voter Page (https://mvp.sos.ga.gov/s/mvp-landing-page)
requires a person-based lookup: first initial, last name, county, and date of birth.
There is no public API, so we drive the form with a headless Playwright browser.
"""

import asyncio
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Callable, Any

from rapidfuzz import fuzz


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class GAVoterRecord:
    first_initial: str
    last_name: str
    reg_county: str
    date_of_birth: str          # YYYY-MM-DD (as stored in the CSV)
    polling_place_name: str     # expected value from CSV to validate against
    polling_place_address_full: Optional[str] = None
    row_index: int = 0


@dataclass
class GAValidationResult:
    row_index: int
    first_initial: str
    last_name: str
    reg_county: str
    polling_place_name: str     # expected (from CSV)
    ga_polling_place_returned: Optional[str] = None   # actual from site
    ga_polling_address_returned: Optional[str] = None
    status: str = "error"       # match | mismatch | not_found | error
    match_score: float = 0.0
    notes: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# County name helpers
# ---------------------------------------------------------------------------

def _county_candidates(county_raw: str) -> list[str]:
    """Return candidate strings to try when selecting the county dropdown."""
    upper = county_raw.strip().upper()
    title = county_raw.strip().title()
    return [upper, title, upper + " COUNTY", title + " County"]


# ---------------------------------------------------------------------------
# Shadow DOM text extraction
# ---------------------------------------------------------------------------

_SHADOW_TEXT_JS = """
() => {
    const texts = [];
    function traverse(root) {
        if (!root) return;
        if (root.shadowRoot) traverse(root.shadowRoot);
        root.childNodes.forEach(node => {
            if (node.nodeType === 3) {           // TEXT_NODE
                const t = node.textContent.trim();
                if (t) texts.push(t);
            } else if (node.nodeType === 1) {   // ELEMENT_NODE
                traverse(node);
            }
        });
    }
    traverse(document.body);
    return texts;
}
"""


async def _extract_polling_place_from_page(page) -> Optional[dict]:
    """
    Extract the election-day polling place name and address from the result page.

    The GA MVP result page is a Salesforce LWC app, so useful text lives inside
    shadow roots.  We traverse every shadow root, collect ordered text nodes, then
    look for the section that follows recognisable headers.
    """
    try:
        texts: list[str] = await page.evaluate(_SHADOW_TEXT_JS)
    except Exception:
        return None

    # Normalised list (skip short noise tokens)
    clean = [t.strip() for t in texts if len(t.strip()) > 2]

    # Keyword triggers for the polling-place section
    TRIGGERS = {
        "election day polling location",
        "polling location",
        "polling place",
        "poll location",
        "election day location",
    }

    for i, token in enumerate(clean):
        if token.lower() in TRIGGERS:
            # Grab the next non-trivial tokens as name + address
            candidates = [t for t in clean[i + 1 : i + 8] if t.lower() not in TRIGGERS]
            if candidates:
                name = candidates[0]
                # Address heuristic: contains digits + road/st/ave etc.
                address = None
                for c in candidates[1:4]:
                    if re.search(r"\d", c):
                        address = c
                        break
                return {"name": name, "address": address}

    return None


# ---------------------------------------------------------------------------
# Single-record validation
# ---------------------------------------------------------------------------

async def _validate_one(page, record: GAVoterRecord, match_threshold: int) -> GAValidationResult:
    result = GAValidationResult(
        row_index=record.row_index,
        first_initial=record.first_initial,
        last_name=record.last_name,
        reg_county=record.reg_county,
        polling_place_name=record.polling_place_name,
    )

    try:
        await page.goto(
            "https://mvp.sos.ga.gov/s/mvp-landing-page",
            wait_until="networkidle",
            timeout=30_000,
        )

        # Convert date: YYYY-MM-DD → MM/DD/YYYY
        dob_date = datetime.strptime(record.date_of_birth, "%Y-%m-%d")
        dob_formatted = dob_date.strftime("%m/%d/%Y")

        # --- Fill form (Playwright auto-pierces shadow DOM via get_by_label) ---
        await page.get_by_label("First Initial").fill(record.first_initial.strip()[:1])
        await page.get_by_label("Last Name").fill(record.last_name.strip())

        # County dropdown — try multiple name formats
        county_sel = page.get_by_label("County")
        last_exc: Optional[Exception] = None
        for candidate in _county_candidates(record.reg_county):
            try:
                await county_sel.select_option(candidate, timeout=3_000)
                last_exc = None
                break
            except Exception as e:
                last_exc = e

        if last_exc is not None:
            # Fallback: try clicking and typing the county name
            try:
                await county_sel.click()
                await page.keyboard.type(record.reg_county.strip().title())
                await page.keyboard.press("Enter")
            except Exception:
                raise last_exc

        # Date of birth
        dob_field = page.get_by_label("Date of Birth")
        await dob_field.fill(dob_formatted)
        # Some date pickers need a Tab to confirm
        await dob_field.press("Tab")

        # Submit
        await page.get_by_role("button", name="SUBMIT").click()
        await page.wait_for_load_state("networkidle", timeout=20_000)

        # --- Check for "not found" ---
        page_text_lower = (await page.inner_text("body")).lower()
        NOT_FOUND_SIGNALS = [
            "not found", "no record", "no voter", "could not find",
            "information not found", "no match",
        ]
        if any(s in page_text_lower for s in NOT_FOUND_SIGNALS):
            result.status = "not_found"
            result.notes = "Voter not found in GA MVP portal"
            return result

        # --- Extract polling place ---
        place = await _extract_polling_place_from_page(page)

        if not place:
            result.status = "not_found"
            result.notes = "Result page loaded but could not locate polling place info"
            return result

        result.ga_polling_place_returned = place["name"]
        result.ga_polling_address_returned = place.get("address")

        # --- Fuzzy compare ---
        score = fuzz.token_sort_ratio(
            record.polling_place_name.upper(),
            place["name"].upper(),
        )
        result.match_score = float(score)

        if score >= match_threshold:
            result.status = "match"
            result.notes = f"Match score: {score}%"
        else:
            result.status = "mismatch"
            result.notes = (
                f"Expected: '{record.polling_place_name}' | "
                f"Got: '{place['name']}' | Score: {score}%"
            )

    except Exception as exc:
        result.status = "error"
        result.error = str(exc)

    return result


# ---------------------------------------------------------------------------
# Progress proxy
# ---------------------------------------------------------------------------

class _Progress:
    def __init__(self, completed, matched, mismatched, not_found, errors):
        self.completed_records = completed
        self.matched = matched
        self.mismatched = mismatched
        self.not_found = not_found
        self.errors = errors


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

async def run_ga_validation(
    records: list[GAVoterRecord],
    match_threshold: int = 85,
    requests_per_second: float = 1.0,
    progress_callback: Optional[Callable[[Any], None]] = None,
) -> list[GAValidationResult]:
    """
    Validate all records against the GA MVP portal.

    Uses a single persistent browser page so we don't pay the browser-launch
    overhead per record.  Rate is controlled by `requests_per_second`.
    """
    from playwright.async_api import async_playwright

    results: list[GAValidationResult] = []
    delay = max(0.5, 1.0 / requests_per_second)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        for i, record in enumerate(records):
            result = await _validate_one(page, record, match_threshold)
            results.append(result)

            if progress_callback:
                matched = sum(1 for r in results if r.status == "match")
                mismatched = sum(1 for r in results if r.status == "mismatch")
                not_found = sum(1 for r in results if r.status == "not_found")
                errors = sum(1 for r in results if r.status == "error")
                progress_callback(
                    _Progress(i + 1, matched, mismatched, not_found, errors)
                )

            if i < len(records) - 1:
                await asyncio.sleep(delay)

        await browser.close()

    return results


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_ga_csv(path) -> tuple:
    """
    Load a GA voter CSV and return (raw_df, list[GAVoterRecord]).

    Required columns: first_initial, last_name, reg_county, date_of_birth,
                      polling_place_name
    Optional column:  polling_place_address_full
    """
    import pandas as pd
    from pathlib import Path

    df = pd.read_csv(Path(path))

    required = {"first_initial", "last_name", "reg_county", "date_of_birth", "polling_place_name"}
    missing = required - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"GA CSV is missing required columns: {missing}")

    # Normalise column names to lowercase
    df.columns = df.columns.str.lower()

    records: list[GAVoterRecord] = []
    for idx, row in df.iterrows():
        records.append(
            GAVoterRecord(
                first_initial=str(row["first_initial"]).strip(),
                last_name=str(row["last_name"]).strip(),
                reg_county=str(row["reg_county"]).strip(),
                date_of_birth=str(row["date_of_birth"]).strip(),
                polling_place_name=str(row["polling_place_name"]).strip(),
                polling_place_address_full=str(row.get("polling_place_address_full", "")).strip() or None,
                row_index=int(idx),
            )
        )

    return df, records


def save_ga_results(raw_df, results: list[GAValidationResult], output_path) -> "pd.DataFrame":
    """Merge validation results back onto the original dataframe and save."""
    import pandas as pd
    from pathlib import Path

    result_map = {r.row_index: r for r in results}

    cols = [
        "ga_polling_place_returned",
        "ga_polling_address_returned",
        "validation_status",
        "match_score",
        "validation_notes",
        "validation_error",
        "matches_ga",
    ]
    for col in cols:
        raw_df[col] = None

    for idx, row in raw_df.iterrows():
        r = result_map.get(idx)
        if r is None:
            continue
        raw_df.at[idx, "ga_polling_place_returned"] = r.ga_polling_place_returned
        raw_df.at[idx, "ga_polling_address_returned"] = r.ga_polling_address_returned
        raw_df.at[idx, "validation_status"] = r.status
        raw_df.at[idx, "match_score"] = r.match_score
        raw_df.at[idx, "validation_notes"] = r.notes
        raw_df.at[idx, "validation_error"] = r.error
        raw_df.at[idx, "matches_ga"] = 1 if r.status == "match" else 0

    output_path = Path(output_path)
    raw_df.to_csv(output_path, index=False)
    return raw_df
