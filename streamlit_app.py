"""Polling Place Validator — Virginia & Georgia"""

import asyncio
import pandas as pd
import streamlit as st
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from va_polling_validator.models import ValidatorConfig, MatchStatus
from va_polling_validator.processor import load_csv, save_results
from va_polling_validator.api_validator import run_api_validation
from va_polling_validator.ga_validator import (
    load_ga_csv,
    save_ga_results,
    run_ga_validation,
)

st.set_page_config(
    page_title="Polling Place Validator",
    page_icon="🗳️",
    layout="centered",
)

# ---------------------------------------------------------------------------
# Password gate
# ---------------------------------------------------------------------------

def get_app_password() -> str | None:
    try:
        return st.secrets["app_password"]
    except Exception:
        return None


def check_password() -> bool:
    app_password = get_app_password()
    if not app_password:
        return True
    if st.session_state.get("password_correct"):
        return True

    st.markdown("## 🔐 Access Required")
    entered = st.text_input("Enter password to access the validator:", type="password")
    if st.button("Submit", type="primary"):
        if entered == app_password:
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("❌ Incorrect password. Please try again.")
    st.markdown("*Contact your administrator for access.*")
    return False


if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    .stApp { max-width: 900px; margin: 0 auto; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# State selector — this sits at the very top of the app
# ---------------------------------------------------------------------------

st.title("🗳️ Polling Place Validator")

state = st.selectbox(
    "Select state to validate",
    ["Virginia (VA)", "Georgia (GA)"],
    help="Choose which state's official voter portal to validate against",
)
is_va = state.startswith("Virginia")
is_ga = state.startswith("Georgia")

st.divider()

# ===========================================================================
# VIRGINIA FLOW
# ===========================================================================

if is_va:
    st.markdown("Validating against **Virginia Department of Elections** via Google Civic API.")

    # --- API Key ---
    st.header("🔑 API Key")
    if "api_key_input" not in st.session_state:
        st.session_state["api_key_input"] = ""

    api_key = st.text_input(
        "Google Civic API Key",
        type="password",
        key="api_key_input",
        help="Get a free key at console.cloud.google.com/apis/credentials",
    )

    if not api_key:
        st.info("👆 Enter your API key to enable fast validation")
        with st.expander("How to get a free API key (takes 2 minutes)"):
            st.markdown("""
1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
2. Create a project (or select existing)
3. Click **"+ CREATE CREDENTIALS"** → **"API key"**
4. Copy the key and paste above
5. [Enable Civic Info API](https://console.cloud.google.com/apis/library/civicinfo.googleapis.com)

**Free tier:** 25,000 requests/day
            """)

    st.divider()

    # --- Settings ---
    st.header("⚙️ Settings")
    col1, col2 = st.columns(2)
    with col1:
        match_threshold = st.slider("Match Threshold (%)", 50, 100, 85,
            help="Minimum fuzzy match score to consider a match")
    with col2:
        rate_limit = st.slider("API Rate Limit (req/sec)", 1, 25, 10,
            help="Requests per second (higher = faster but may hit limits)")

    st.divider()

    # --- Upload ---
    st.header("📁 Upload CSV File")
    uploaded_file = st.file_uploader(
        "Drop your Virginia CSV file here",
        type=["csv"],
        help="Required columns: reg_address_full (or reg_address + reg_city + reg_zip) and polling_place_name",
    )

    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)
            st.success(f"✅ Loaded **{len(df)}** records from `{uploaded_file.name}`")
            with st.expander("Preview data"):
                st.dataframe(df.head(10), use_container_width=True)

            if api_key:
                est_seconds = len(df) / rate_limit
                est_time = (
                    f"~{int(est_seconds)} seconds"
                    if est_seconds < 60
                    else f"~{est_seconds / 60:.1f} minutes"
                )
                st.info(f"⚡ **API Mode:** Estimated time: {est_time}")
            else:
                st.warning("⚠️ No API key provided. Please add one above to run validation.")

            if api_key and st.button("🚀 Start Validation", type="primary", use_container_width=True):
                temp_path = Path(f"/tmp/{uploaded_file.name}")
                temp_path.write_bytes(uploaded_file.getvalue())
                try:
                    df_loaded, records = load_csv(temp_path)
                    config = ValidatorConfig(match_threshold=match_threshold)

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    stats_container = st.empty()
                    progress_data = {"completed": 0, "matched": 0, "mismatched": 0, "not_found": 0, "errors": 0}

                    def update_progress(progress):
                        progress_data["completed"] = progress.completed_records
                        progress_data["matched"] = progress.matched
                        progress_data["mismatched"] = progress.mismatched
                        progress_data["not_found"] = progress.not_found
                        progress_data["errors"] = progress.errors

                    status_text.text(f"Validating {len(records)} records...")

                    async def run_va():
                        return await run_api_validation(
                            records=records,
                            api_key=api_key,
                            config=config,
                            requests_per_second=rate_limit,
                            concurrency=min(5, rate_limit),
                            progress_callback=update_progress,
                        )

                    loop = asyncio.new_event_loop()
                    task = loop.create_task(run_va())
                    while not task.done():
                        loop.run_until_complete(asyncio.sleep(0.5))
                        pct = progress_data["completed"] / len(records) if records else 0
                        progress_bar.progress(pct)
                        status_text.text(f"Processing: {progress_data['completed']}/{len(records)} records...")
                        with stats_container.container():
                            cols = st.columns(4)
                            cols[0].metric("✅ Matched", progress_data["matched"])
                            cols[1].metric("❌ Mismatched", progress_data["mismatched"])
                            cols[2].metric("⚠️ Not Found", progress_data["not_found"])
                            cols[3].metric("🔴 Errors", progress_data["errors"])

                    results = task.result()
                    loop.close()

                    progress_bar.progress(1.0)
                    status_text.text("✅ Validation complete!")

                    output_path = Path(f"/tmp/{temp_path.stem}_validated.csv")
                    result_df = save_results(df_loaded, results, output_path)

                    st.divider()
                    st.header("📊 Results")
                    total = len(results)
                    matched = sum(1 for r in results if r.status == MatchStatus.MATCH)
                    mismatched = sum(1 for r in results if r.status == MatchStatus.MISMATCH)
                    not_found = sum(1 for r in results if r.status == MatchStatus.NOT_FOUND)
                    errors = sum(1 for r in results if r.status == MatchStatus.ERROR)

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("✅ Matched", matched, f"{matched/total*100:.1f}%")
                    c2.metric("❌ Mismatched", mismatched, f"{mismatched/total*100:.1f}%")
                    c3.metric("⚠️ Not Found", not_found, f"{not_found/total*100:.1f}%")
                    c4.metric("🔴 Errors", errors, f"{errors/total*100:.1f}%")

                    if mismatched > 0:
                        st.subheader("🔍 Mismatched Records")
                        mismatch_df = result_df[result_df["validation_status"] == "mismatch"][[
                            "reg_address", "reg_city", "polling_place_name",
                            "va_polling_place_returned", "match_score",
                        ]]
                        st.dataframe(mismatch_df, use_container_width=True)

                    if errors > 0:
                        st.subheader("🚨 Error Rows")
                        error_cols = [
                            c for c in ["reg_address", "reg_city", "polling_place_name",
                                        "validation_status", "validation_notes", "validation_error"]
                            if c in result_df.columns
                        ]
                        st.dataframe(
                            result_df[result_df["validation_status"] == "error"][error_cols],
                            use_container_width=True,
                        )

                    st.divider()
                    csv_data = output_path.read_text()
                    st.download_button(
                        label="📥 Download Results CSV",
                        data=csv_data,
                        file_name=f"{temp_path.stem}_validated.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary",
                    )
                    with st.expander("Preview validated data"):
                        st.dataframe(result_df.head(20), use_container_width=True)

                except Exception as e:
                    st.error(f"Error during validation: {e}")
                    raise e
                finally:
                    if temp_path.exists():
                        temp_path.unlink()

        except Exception as e:
            st.error(f"Error loading CSV: {e}")

    # VA footer
    st.divider()
    st.markdown("""
<div style="text-align:center;color:#666;font-size:0.9em;">
Data validated against
<a href="https://www.elections.virginia.gov" target="_blank">Virginia Department of Elections</a>
via <a href="https://developers.google.com/civic-information" target="_blank">Google Civic Information API</a>
</div>
""", unsafe_allow_html=True)


# ===========================================================================
# GEORGIA FLOW
# ===========================================================================

elif is_ga:
    st.markdown(
        "Validating against **Georgia My Voter Page** "
        "([mvp.sos.ga.gov](https://mvp.sos.ga.gov/s/mvp-landing-page)) "
        "via browser automation. No API key required."
    )
    st.info(
        "**Note:** Georgia validation uses browser automation (Playwright) rather than "
        "an API, so it runs at ~1 record/second. For large datasets, expect a longer wait."
    )

    # --- Settings ---
    st.header("⚙️ Settings")
    col1, col2 = st.columns(2)
    with col1:
        ga_match_threshold = st.slider(
            "Match Threshold (%)", 50, 100, 85,
            key="ga_match_threshold",
            help="Minimum fuzzy match score to consider a match",
        )
    with col2:
        ga_rate_limit = st.slider(
            "Rate Limit (req/sec)", 1, 3, 1,
            key="ga_rate_limit",
            help="Requests per second — keep low to avoid being blocked",
        )

    st.divider()

    # --- Upload ---
    st.header("📁 Upload CSV File")
    st.caption(
        "Required columns: `first_initial`, `last_name`, `reg_county`, "
        "`date_of_birth` (YYYY-MM-DD), `polling_place_name`"
    )

    ga_uploaded = st.file_uploader(
        "Drop your Georgia CSV file here",
        type=["csv"],
        key="ga_uploader",
        help="Required: first_initial, last_name, reg_county, date_of_birth, polling_place_name",
    )

    if ga_uploaded is not None:
        try:
            df_preview = pd.read_csv(ga_uploaded)
            ga_uploaded.seek(0)
            st.success(f"✅ Loaded **{len(df_preview)}** records from `{ga_uploaded.name}`")

            with st.expander("Preview data"):
                st.dataframe(df_preview.head(10), use_container_width=True)

            est_seconds = len(df_preview) / ga_rate_limit
            est_time = (
                f"~{int(est_seconds)} seconds"
                if est_seconds < 60
                else f"~{est_seconds / 60:.1f} minutes"
            )
            st.info(f"🕐 **Browser Mode:** Estimated time: {est_time}")

            if st.button("🚀 Start GA Validation", type="primary", use_container_width=True):
                temp_path = Path(f"/tmp/{ga_uploaded.name}")
                temp_path.write_bytes(ga_uploaded.getvalue())
                try:
                    df_loaded, ga_records = load_ga_csv(temp_path)

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    stats_container = st.empty()
                    ga_progress = {"completed": 0, "matched": 0, "mismatched": 0, "not_found": 0, "errors": 0}

                    def update_ga_progress(prog):
                        ga_progress["completed"] = prog.completed_records
                        ga_progress["matched"] = prog.matched
                        ga_progress["mismatched"] = prog.mismatched
                        ga_progress["not_found"] = prog.not_found
                        ga_progress["errors"] = prog.errors

                    status_text.text(f"Launching browser and validating {len(ga_records)} records...")

                    async def run_ga():
                        return await run_ga_validation(
                            records=ga_records,
                            match_threshold=ga_match_threshold,
                            requests_per_second=ga_rate_limit,
                            headless=False,  # visible browser passes reCAPTCHA more reliably
                            progress_callback=update_ga_progress,
                        )

                    loop = asyncio.new_event_loop()
                    task = loop.create_task(run_ga())
                    while not task.done():
                        loop.run_until_complete(asyncio.sleep(1.0))
                        n = len(ga_records)
                        pct = ga_progress["completed"] / n if n else 0
                        progress_bar.progress(pct)
                        status_text.text(
                            f"Processing: {ga_progress['completed']}/{n} records..."
                        )
                        with stats_container.container():
                            cols = st.columns(4)
                            cols[0].metric("✅ Matched", ga_progress["matched"])
                            cols[1].metric("❌ Mismatched", ga_progress["mismatched"])
                            cols[2].metric("⚠️ Not Found", ga_progress["not_found"])
                            cols[3].metric("🔴 Errors", ga_progress["errors"])

                    ga_results = task.result()
                    loop.close()

                    progress_bar.progress(1.0)
                    status_text.text("✅ GA Validation complete!")

                    output_path = Path(f"/tmp/{temp_path.stem}_validated.csv")
                    result_df = save_ga_results(df_loaded, ga_results, output_path)

                    st.divider()
                    st.header("📊 Results")
                    total = len(ga_results)
                    matched = sum(1 for r in ga_results if r.status == "match")
                    mismatched = sum(1 for r in ga_results if r.status == "mismatch")
                    not_found = sum(1 for r in ga_results if r.status == "not_found")
                    errors = sum(1 for r in ga_results if r.status == "error")

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("✅ Matched", matched, f"{matched/total*100:.1f}%")
                    c2.metric("❌ Mismatched", mismatched, f"{mismatched/total*100:.1f}%")
                    c3.metric("⚠️ Not Found", not_found, f"{not_found/total*100:.1f}%")
                    c4.metric("🔴 Errors", errors, f"{errors/total*100:.1f}%")

                    if mismatched > 0:
                        st.subheader("🔍 Mismatched Records")
                        mismatch_cols = [
                            c for c in [
                                "first_initial", "last_name", "reg_county",
                                "polling_place_name", "ga_polling_place_returned", "match_score",
                            ] if c in result_df.columns
                        ]
                        st.dataframe(
                            result_df[result_df["validation_status"] == "mismatch"][mismatch_cols],
                            use_container_width=True,
                        )

                    if errors > 0:
                        st.subheader("🚨 Error Rows")
                        error_cols = [
                            c for c in [
                                "first_initial", "last_name", "reg_county",
                                "polling_place_name", "validation_status",
                                "validation_notes", "validation_error",
                            ] if c in result_df.columns
                        ]
                        st.dataframe(
                            result_df[result_df["validation_status"] == "error"][error_cols],
                            use_container_width=True,
                        )

                    st.divider()
                    csv_data = output_path.read_text()
                    st.download_button(
                        label="📥 Download Results CSV",
                        data=csv_data,
                        file_name=f"{temp_path.stem}_validated.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary",
                    )
                    with st.expander("Preview validated data"):
                        st.dataframe(result_df.head(20), use_container_width=True)

                except Exception as e:
                    st.error(f"Error during GA validation: {e}")
                    raise e
                finally:
                    if temp_path.exists():
                        temp_path.unlink()

        except Exception as e:
            st.error(f"Error loading CSV: {e}")

    # GA footer
    st.divider()
    st.markdown("""
<div style="text-align:center;color:#666;font-size:0.9em;">
Data validated against
<a href="https://mvp.sos.ga.gov/s/mvp-landing-page" target="_blank">Georgia My Voter Page</a>
— Georgia Secretary of State
</div>
""", unsafe_allow_html=True)
