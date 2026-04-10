"""Georgia polling place validator.

Two validation paths — chosen automatically per record:

  API path  (fast, ~0.1 s/record)
    Requires: reg_address column in CSV + Google Civic API key.
    Uses the same Google Civic voterInfoQuery endpoint as the VA validator.
    Falls back to browser if the API returns no polling place (e.g. outside
    an active election window or address not matched).

  Browser path  (slow, ~20 s/record)
    Always available.  Playwright automates the GA My Voter Page portal
    (https://mvp.sos.ga.gov/s/mvp-landing-page) using first_initial,
    last_name, reg_county, date_of_birth.

Notes:
  • Run WITHOUT a VPN — VPN IPs get low reCAPTCHA scores on the browser path.
  • Results carry a `validation_method` field: "api" or "browser".
"""

import asyncio
import platform
import re
import subprocess
import threading
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Callable, Any

from rapidfuzz import fuzz


# ---------------------------------------------------------------------------
# macOS: keep Playwright's Chromium window minimised in the background
# ---------------------------------------------------------------------------

def _start_chromium_minimizer() -> Optional[threading.Event]:
    """
    On macOS, spin up a background thread that continuously minimises every
    window belonging to the "Chromium" process (Playwright's bundled binary,
    distinct from the user's "Google Chrome").  Returns a stop-event so the
    caller can halt the thread cleanly, or None on non-macOS systems.
    """
    if platform.system() != "Darwin":
        return None

    # "set visible … false" hides the whole app (equivalent to Cmd+H) —
    # no Dock animation, no visible flash. Much cleaner than miniaturized.
    # Playwright's bundled Chromium appears as "Google Chrome for Testing"
    # in macOS System Events (NOT "Chromium").
    _SCRIPT = (
        'tell application "System Events"\n'
        '    repeat with p in (processes whose name is "Google Chrome for Testing")\n'
        '        try\n'
        '            set visible of p to false\n'
        '        end try\n'
        '    end repeat\n'
        'end tell'
    )

    stop_evt = threading.Event()

    def _loop():
        # No initial sleep — start hiding immediately so the window never
        # has a chance to appear on screen.
        while not stop_evt.is_set():
            try:
                subprocess.run(["osascript", "-e", _SCRIPT],
                               capture_output=True, timeout=8)
            except Exception:
                pass
            time.sleep(0.3)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return stop_evt


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class GAVoterRecord:
    first_initial: str
    last_name: str
    reg_county: str
    date_of_birth: str              # YYYY-MM-DD (as stored in the CSV)
    polling_place_name: str         # expected value to validate
    polling_place_address_full: Optional[str] = None
    reg_address: Optional[str] = None   # voter residential address (enables API path)
    row_index: int = 0


@dataclass
class GAValidationResult:
    row_index: int
    first_initial: str
    last_name: str
    reg_county: str
    polling_place_name: str
    ga_polling_place_returned: Optional[str] = None
    ga_polling_address_returned: Optional[str] = None
    status: str = "error"           # match | mismatch | not_found | error
    match_score: float = 0.0
    notes: str = ""
    error: str = ""
    validation_method: str = "browser"  # "api" or "browser"


# ---------------------------------------------------------------------------
# Shadow DOM text extraction JS
# ---------------------------------------------------------------------------

_SHADOW_TEXT_JS = """
() => {
    const texts = [];
    function traverse(root) {
        if (!root) return;
        if (root.shadowRoot) traverse(root.shadowRoot);
        root.childNodes.forEach(node => {
            if (node.nodeType === 3) {
                const t = node.textContent.trim();
                if (t.length > 2 && !t.startsWith('(function') && !t.includes('=>')) {
                    texts.push(t);
                }
            } else if (node.nodeType === 1) {
                traverse(node);
            }
        });
    }
    traverse(document.body);
    return texts;
}
"""


# ---------------------------------------------------------------------------
# Polling place extraction from the voter dashboard
# ---------------------------------------------------------------------------

def _extract_from_tokens(tokens: list[str]) -> Optional[dict]:
    """
    Parse the ordered shadow-DOM text tokens from the voter dashboard.

    The relevant section looks like:
        "Election Day Polling Place"
        "707 SPRING BRANCH CH RD BAXLEY, GA 31513 0000"
        ...
    And just before it:
        "Precinct Name"
        "SPRING BRANCH BAPTIST CHURCH (1B)"

    We strip the "(1B)" precinct suffix from the name.
    """
    PLACE_TRIGGERS = {
        "election day polling place",
        "election day polling location",
        "polling place",
        "poll location",
    }
    NAME_TRIGGERS = {"precinct name"}

    place_name: Optional[str] = None
    place_address: Optional[str] = None

    for i, tok in enumerate(tokens):
        low = tok.lower().strip()

        # Precinct name line
        if low in NAME_TRIGGERS:
            for j in range(i + 1, min(i + 5, len(tokens))):
                candidate = tokens[j].strip()
                if len(candidate) > 4 and candidate.lower() not in NAME_TRIGGERS:
                    # Strip "(1B)" style suffix
                    place_name = re.sub(r'\s*\(\w+\)\s*$', '', candidate).strip()
                    break

        # Address line (comes after the trigger)
        if low in PLACE_TRIGGERS:
            for j in range(i + 1, min(i + 6, len(tokens))):
                candidate = tokens[j].strip()
                if re.search(r'\d', candidate) and len(candidate) > 10:
                    place_address = candidate
                    break

    if place_name:
        return {"name": place_name, "address": place_address}
    return None


# ---------------------------------------------------------------------------
# API path — Google Civic Information API (fast, requires reg_address)
# ---------------------------------------------------------------------------

async def _validate_one_api(
    validator,          # CivicAPIValidator instance (already started)
    record: GAVoterRecord,
    match_threshold: int,
) -> GAValidationResult:
    """Validate one GA record via the Google Civic voterInfoQuery endpoint."""
    result = GAValidationResult(
        row_index=record.row_index,
        first_initial=record.first_initial,
        last_name=record.last_name,
        reg_county=record.reg_county,
        polling_place_name=record.polling_place_name,
        validation_method="api",
    )

    name, address, error = await validator.lookup_polling_place(record.reg_address)

    if error:
        result.status = "error"
        result.error = f"API: {error}"
        return result

    if not name:
        result.status = "not_found"
        result.notes = "API returned no polling place (outside election window or address not matched)"
        return result

    result.ga_polling_place_returned = name
    result.ga_polling_address_returned = address

    score = fuzz.token_sort_ratio(
        record.polling_place_name.upper(),
        name.upper(),
    )
    result.match_score = float(score)
    if score >= match_threshold:
        result.status = "match"
        result.notes = f"Match score: {score}% (API)"
    else:
        result.status = "mismatch"
        result.notes = (
            f"Expected: '{record.polling_place_name}' | "
            f"Got: '{name}' | Score: {score}% (API)"
        )

    return result


# ---------------------------------------------------------------------------
# Single-record validation (reuses a shared page)
# ---------------------------------------------------------------------------

async def _validate_one(
    page,
    record: GAVoterRecord,
    match_threshold: int,
) -> GAValidationResult:
    """
    Validate one record using an already-open, already-stealthed page.

    We navigate to the landing page fresh each time and clear storage so
    session state from the previous voter doesn't bleed through.  Re-using
    one page (rather than creating a new browser context per record) means
    macOS never opens a second window — the single window is hidden once at
    startup and stays hidden for the whole batch.
    """
    result = GAValidationResult(
        row_index=record.row_index,
        first_initial=record.first_initial,
        last_name=record.last_name,
        reg_county=record.reg_county,
        polling_place_name=record.polling_place_name,
    )

    try:
        # Hard-reset between records: blank page first so the Salesforce LWC
        # teardown completes before the new load, preventing stale overlays.
        try:
            await page.goto("about:blank", timeout=5_000)
            await page.evaluate(
                "() => { try { localStorage.clear(); sessionStorage.clear(); } catch(e){} }"
            )
        except Exception:
            pass

        # Navigate to GA MVP landing page; wait for first input to be ready
        # rather than a fixed sleep — exits as soon as the LWC is interactive.
        await page.goto(
            "https://mvp.sos.ga.gov/s/mvp-landing-page",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        await page.wait_for_selector("input", state="visible", timeout=15_000)
        # Brief pause so LWC event-listeners are fully wired up (was 1.5 s)
        await asyncio.sleep(0.5)

        # Convert date: YYYY-MM-DD → MM/DD/YYYY
        dob_date = datetime.strptime(record.date_of_birth, "%Y-%m-%d")
        dob_formatted = dob_date.strftime("%m/%d/%Y")

        # ---- Fill form (tighter delays — still human-like) ----
        await page.locator("input").nth(0).fill(record.first_initial.strip()[:1])
        await asyncio.sleep(0.15)
        await page.locator("input").nth(1).fill(record.last_name.strip())
        await asyncio.sleep(0.15)

        # County: Salesforce LIGHTNING-COMBOBOX (not a native <select>)
        await page.get_by_role("combobox").click(timeout=5_000)
        await asyncio.sleep(0.3)
        county_upper = record.reg_county.strip().upper()
        county_title = record.reg_county.strip().title()
        clicked_county = False
        for candidate in [county_upper, county_title]:
            try:
                await page.get_by_role("option", name=candidate).click(timeout=3_000)
                clicked_county = True
                break
            except Exception:
                pass
        if not clicked_county:
            result.status = "error"
            result.error = f"Could not select county '{record.reg_county}' from dropdown"
            return result

        await asyncio.sleep(0.15)
        await page.locator("input").nth(2).fill(dob_formatted)
        await asyncio.sleep(0.15)

        # ---- Human-like pause before submit (was 1.0 s) ----
        for x, y in [(300, 400), (500, 350), (400, 550)]:
            await page.mouse.move(x, y)
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        # ---- Submit, then wait for navigation rather than a fixed sleep ----
        await page.locator("button", has_text="SUBMIT").click()

        # Wait for either the dashboard URL or a reCAPTCHA iframe to appear.
        # This exits as soon as the page transitions — usually 1–3 s, not always 3 s.
        try:
            await page.wait_for_url(
                lambda url: "mvp-dashboard" in url or "mvp-landing-page" not in url,
                timeout=12_000,
            )
        except Exception:
            pass  # fall through to reCAPTCHA / not-found checks below

        # ---- Handle reCAPTCHA v2 checkbox if it appears ----
        anchor_frames = [f for f in page.frames if "anchor" in f.url and "recaptcha" in f.url]
        if anchor_frames:
            cf = anchor_frames[0]
            for sel in ["#recaptcha-anchor", ".recaptcha-checkbox", ".rc-anchor-center-container"]:
                try:
                    el = cf.locator(sel)
                    if await el.count() > 0:
                        await el.first.click(timeout=6_000)
                        # Must wait for reCAPTCHA image challenge to resolve
                        await asyncio.sleep(8)
                        break
                except Exception:
                    pass

        # Check if we're still on the landing page (not found / failed)
        if "mvp-landing-page" in page.url or "mvp-dashboard" not in page.url:
            page_text = (await page.inner_text("body")).lower()
            if "recaptcha" in page_text and "failed" in page_text:
                result.status = "error"
                result.error = "reCAPTCHA blocked — try running without a VPN"
                return result
            result.status = "not_found"
            result.notes = "Voter not found or form submission failed"
            return result

        # ---- We're on the dashboard — wait for content, then extract ----
        try:
            voting_tab = page.get_by_role("link", name="My Voting Location")
            if await voting_tab.count() > 0:
                await voting_tab.click(timeout=5_000)
        except Exception:
            pass

        # Wait for the precinct name token to appear rather than sleeping
        try:
            await page.wait_for_function(
                """() => {
                    const texts = [];
                    function t(n) {
                        if (n.shadowRoot) t(n.shadowRoot);
                        n.childNodes.forEach(c => {
                            if (c.nodeType === 3) texts.push(c.textContent.toLowerCase());
                            else if (c.nodeType === 1) t(c);
                        });
                    }
                    t(document.body);
                    return texts.some(s => s.includes('precinct name'));
                }""",
                timeout=10_000,
            )
        except Exception:
            pass  # extract anyway; _extract_from_tokens handles missing data

        tokens: list[str] = await page.evaluate(_SHADOW_TEXT_JS)
        place = _extract_from_tokens(tokens)

        if not place:
            result.status = "not_found"
            result.notes = "Dashboard loaded but could not locate polling place info in page text"
            return result

        result.ga_polling_place_returned = place["name"]
        result.ga_polling_address_returned = place.get("address")

        # ---- Fuzzy compare ----
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
        result.error = f"{type(exc).__name__}: {exc}"

    return result


# ---------------------------------------------------------------------------
# Progress helper
# ---------------------------------------------------------------------------

class _Progress:
    def __init__(self, completed=0, matched=0, mismatched=0, not_found=0, errors=0):
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
    api_key: Optional[str] = None,
    progress_callback: Optional[Callable[[Any], None]] = None,
) -> list[GAValidationResult]:
    """Validate all GA records.

    Phase 1 — Google Civic API (fast, ~0.1 s/record):
        Runs for records that have a reg_address AND an api_key is supplied.
        Records where the API returns nothing are passed to Phase 2.

    Phase 2 — Browser automation (slow, ~20 s/record):
        Runs for all remaining records (no address, no key, or API miss).
        reCAPTCHA Enterprise detects headless Chrome, so on macOS we run
        non-headless and hide the window immediately via AppleScript.
    """
    from playwright.async_api import async_playwright

    results_map: dict[int, GAValidationResult] = {}
    browser_records: list[GAVoterRecord] = []
    delay = max(2.0, 1.0 / requests_per_second)

    def _emit_progress(completed_so_far: int):
        if not progress_callback:
            return
        all_so_far = list(results_map.values())
        progress_callback(_Progress(
            completed=completed_so_far,
            matched=sum(1 for r in all_so_far if r.status == "match"),
            mismatched=sum(1 for r in all_so_far if r.status == "mismatch"),
            not_found=sum(1 for r in all_so_far if r.status == "not_found"),
            errors=sum(1 for r in all_so_far if r.status == "error"),
        ))

    # ------------------------------------------------------------------
    # Phase 1: API
    # ------------------------------------------------------------------
    if api_key:
        from va_polling_validator.api_validator import CivicAPIValidator

        api_eligible = [r for r in records if r.reg_address]
        no_addr = [r for r in records if not r.reg_address]

        if api_eligible:
            async with CivicAPIValidator(api_key, requests_per_second=10.0) as validator:
                for i, record in enumerate(api_eligible):
                    result = await _validate_one_api(validator, record, match_threshold)
                    results_map[record.row_index] = result
                    _emit_progress(len(results_map))

                    # If API couldn't find it, queue for browser fallback
                    if result.status in ("not_found", "error"):
                        browser_records.append(record)

        browser_records.extend(no_addr)
    else:
        browser_records = list(records)

    # ------------------------------------------------------------------
    # Phase 2: Browser (only launched if needed)
    # ------------------------------------------------------------------
    if browser_records:

        is_mac = platform.system() == "Darwin"
        headless = not is_mac

        minimizer_stop: Optional[threading.Event] = None
        if is_mac:
            minimizer_stop = _start_chromium_minimizer()

        async with async_playwright() as pw:
            from playwright_stealth import Stealth

            browser = await pw.chromium.launch(
                headless=headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1280,900",
                ],
            )

            ctx = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = await ctx.new_page()
            await Stealth(navigator_webdriver=True).apply_stealth_async(page)

            # One-time warm-up visit to seed the reCAPTCHA session score.
            try:
                await page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=10_000)
                await asyncio.sleep(1.0)
            except Exception:
                pass

            try:
                for i, record in enumerate(browser_records):
                    result = await _validate_one(page, record, match_threshold)
                    results_map[record.row_index] = result
                    _emit_progress(len(results_map))

                    if i < len(browser_records) - 1:
                        await asyncio.sleep(delay)
            finally:
                await ctx.close()
                await browser.close()
                if minimizer_stop:
                    minimizer_stop.set()

    # Return results in original row order
    return [results_map[r.row_index] for r in records if r.row_index in results_map]


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_ga_csv(path) -> tuple:
    """Load GA voter CSV → (raw_df, list[GAVoterRecord])."""
    import pandas as pd
    from pathlib import Path

    df = pd.read_csv(Path(path))
    df.columns = df.columns.str.lower()

    # Accept reg_address_full as a synonym for reg_address
    if "reg_address_full" in df.columns and "reg_address" not in df.columns:
        df = df.rename(columns={"reg_address_full": "reg_address"})

    # reg_county can be derived from the TargetSmart precinct code (GA_COUNTY_PRECINCT)
    if "reg_county" not in df.columns:
        precinct_col = next(
            (c for c in df.columns if "precinct_code" in c or "national_precinct" in c), None
        )
        if precinct_col:
            df["reg_county"] = df[precinct_col].str.split("_").str[1]

    required = {"first_initial", "last_name", "reg_county", "date_of_birth", "polling_place_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"GA CSV is missing required columns: {missing}")

    # reg_address is optional — enables faster Google Civic API path
    has_address = "reg_address" in df.columns

    records: list[GAVoterRecord] = []
    for idx, row in df.iterrows():
        records.append(GAVoterRecord(
            first_initial=str(row["first_initial"]).strip(),
            last_name=str(row["last_name"]).strip(),
            reg_county=str(row["reg_county"]).strip(),
            date_of_birth=str(row["date_of_birth"]).strip(),
            polling_place_name=str(row["polling_place_name"]).strip(),
            polling_place_address_full=str(row.get("polling_place_address_full", "")).strip() or None,
            reg_address=str(row["reg_address"]).strip() if has_address else None,
            row_index=int(idx),
        ))

    return df, records


def save_ga_results(raw_df, results: list[GAValidationResult], output_path) -> "pd.DataFrame":
    """Merge results back onto the original dataframe and save."""
    import pandas as pd
    from pathlib import Path

    result_map = {r.row_index: r for r in results}
    for col in ["ga_polling_place_returned", "ga_polling_address_returned",
                "validation_status", "match_score", "validation_notes",
                "validation_error", "matches_ga", "validation_method"]:
        raw_df[col] = None

    for idx in raw_df.index:
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
        raw_df.at[idx, "validation_method"] = r.validation_method

    raw_df.to_csv(Path(output_path), index=False)
    return raw_df
