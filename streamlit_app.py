"""VA Polling Place Validator - Streamlit App"""

import asyncio
import pandas as pd
import streamlit as st
from pathlib import Path
import sys
import time
from streamlit_local_storage import LocalStorage

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from va_polling_validator.models import ValidatorConfig, MatchStatus
from va_polling_validator.processor import load_csv, save_results
from va_polling_validator.api_validator import run_api_validation

st.set_page_config(
    page_title="VA Polling Place Validator",
    page_icon="🗳️",
    layout="centered",
)

# --- PASSWORD PROTECTION ---
def get_app_password() -> str | None:
    """Return the configured app password, or None when local auth is disabled."""
    try:
        return st.secrets["app_password"]
    except Exception:
        return None


def check_password() -> bool:
    """Show password gate. Returns True only when access is granted."""
    app_password = get_app_password()

    if not app_password:
        return True

    if st.session_state.get("password_correct"):
        return True

    st.markdown("## 🔐 Access Required")
    entered = st.text_input(
        "Enter password to access the validator:",
        type="password",
    )

    if st.button("Submit", type="primary"):
        if entered == app_password:
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("❌ Incorrect password. Please try again.")

    st.markdown("*Contact your administrator for access.*")
    return False


# Check password before showing anything else
if not check_password():
    st.stop()

# Custom CSS
st.markdown("""
<style>
    .stApp {
        max-width: 900px;
        margin: 0 auto;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1fae5;
        border: 1px solid #10b981;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fee2e2;
        border: 1px solid #ef4444;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.title("🗳️ VA Polling Place Validator")
st.markdown("Validate Virginia polling place assignments against official VA elections data.")

# API Key Section
st.header("🔑 API Key")

local_storage = LocalStorage()

if "api_key_input" not in st.session_state:
    st.session_state["api_key_input"] = ""
if "remember_api_key" not in st.session_state:
    st.session_state["remember_api_key"] = False

stored_api_key = local_storage.getItem("va_validator_api_key", key="load_va_validator_api_key")
if stored_api_key and not st.session_state["api_key_input"]:
    st.session_state["api_key_input"] = stored_api_key
    st.session_state["remember_api_key"] = True

api_key = st.text_input(
    "Google Civic API Key",
    type="password",
    key="api_key_input",
    help="Get a free key at console.cloud.google.com/apis/credentials"
)

remember_api_key = st.checkbox(
    "Remember this API key on this device",
    key="remember_api_key",
    help="Stores the key in this browser only. It is not stored on the app server."
)

save_col, clear_col = st.columns(2)
with save_col:
    if st.button("Save Key", use_container_width=True):
        if st.session_state["api_key_input"]:
            local_storage.setItem(
                "va_validator_api_key",
                st.session_state["api_key_input"],
                key="save_va_validator_api_key",
            )
            st.session_state["remember_api_key"] = True
            st.success("API key saved to this browser.")
        else:
            st.warning("Enter an API key before saving it.")
with clear_col:
    if st.button("Forget Saved Key", use_container_width=True):
        local_storage.setItem(
            "va_validator_api_key",
            "",
            key="clear_va_validator_api_key",
        )
        st.session_state["api_key_input"] = ""
        st.session_state["remember_api_key"] = False
        st.success("Saved API key cleared from this browser.")

if not api_key:
    st.info("👆 Enter your API key to enable fast validation")
    
    with st.expander("How to get a free API key (takes 2 minutes)"):
        st.markdown("""
        1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
        2. Create a project (or select existing)
        3. Click **"+ CREATE CREDENTIALS"** → **"API key"**
        4. Copy the key and paste it above
        5. [Enable Civic Info API](https://console.cloud.google.com/apis/library/civicinfo.googleapis.com)
        
        **Free tier:** 25,000 requests/day
        """)

st.divider()

# Settings Section
st.header("⚙️ Settings")

col1, col2 = st.columns(2)

with col1:
    match_threshold = st.slider(
        "Match Threshold (%)",
        min_value=50,
        max_value=100,
        value=85,
        help="Minimum fuzzy match score to consider a match"
    )

with col2:
    rate_limit = st.slider(
        "API Rate Limit (req/sec)",
        min_value=1,
        max_value=25,
        value=10,
        help="Requests per second (higher = faster but may hit limits)"
    )

st.divider()

# Upload Section
st.header("📁 Upload CSV File")

uploaded_file = st.file_uploader(
    "Drop your CSV file here",
    type=['csv'],
    help="Required columns: reg_address_full and polling_place_name. Older split address columns are still supported."
)

if uploaded_file is not None:
    # Load and preview data
    try:
        df = pd.read_csv(uploaded_file)
        uploaded_file.seek(0)  # Reset for later use
        
        st.success(f"✅ Loaded **{len(df)}** records from `{uploaded_file.name}`")
        
        with st.expander("Preview data"):
            st.dataframe(df.head(10), use_container_width=True)
        
        # Estimate time
        if api_key:
            if remember_api_key:
                local_storage.setItem(
                    "va_validator_api_key",
                    api_key,
                    key="persist_va_validator_api_key",
                )
            est_seconds = len(df) / rate_limit
            est_time = f"~{int(est_seconds)} seconds" if est_seconds < 60 else f"~{est_seconds/60:.1f} minutes"
            st.info(f"⚡ **API Mode:** Estimated time: {est_time}")
        else:
            st.warning("⚠️ No API key provided. Please add one above to run validation.")
        
        # Validate button
        if api_key:
            if st.button("🚀 Start Validation", type="primary", use_container_width=True):
                
                # Save uploaded file temporarily
                temp_path = Path(f"/tmp/{uploaded_file.name}")
                temp_path.write_bytes(uploaded_file.getvalue())
                
                try:
                    # Load records
                    df_loaded, records = load_csv(temp_path)
                    
                    config = ValidatorConfig(
                        match_threshold=match_threshold,
                    )
                    
                    # Progress tracking
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    stats_container = st.empty()
                    
                    # Track progress
                    progress_data = {"completed": 0, "matched": 0, "mismatched": 0, "not_found": 0, "errors": 0}
                    
                    def update_progress(progress):
                        progress_data["completed"] = progress.completed_records
                        progress_data["matched"] = progress.matched
                        progress_data["mismatched"] = progress.mismatched
                        progress_data["not_found"] = progress.not_found
                        progress_data["errors"] = progress.errors
                    
                    # Run validation
                    status_text.text(f"Validating {len(records)} records...")
                    
                    # Run async validation
                    async def run_validation():
                        return await run_api_validation(
                            records=records,
                            api_key=api_key,
                            config=config,
                            requests_per_second=rate_limit,
                            concurrency=min(5, rate_limit),
                            progress_callback=update_progress,
                        )
                    
                    # Progress update loop
                    loop = asyncio.new_event_loop()
                    task = loop.create_task(run_validation())
                    
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
                    
                    # Save results
                    output_path = Path(f"/tmp/{temp_path.stem}_validated.csv")
                    result_df = save_results(df_loaded, results, output_path)
                    
                    # Final stats
                    st.divider()
                    st.header("📊 Results")
                    
                    total = len(results)
                    matched = sum(1 for r in results if r.status == MatchStatus.MATCH)
                    mismatched = sum(1 for r in results if r.status == MatchStatus.MISMATCH)
                    not_found = sum(1 for r in results if r.status == MatchStatus.NOT_FOUND)
                    errors = sum(1 for r in results if r.status == MatchStatus.ERROR)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("✅ Matched", matched, f"{matched/total*100:.1f}%")
                    col2.metric("❌ Mismatched", mismatched, f"{mismatched/total*100:.1f}%")
                    col3.metric("⚠️ Not Found", not_found, f"{not_found/total*100:.1f}%")
                    col4.metric("🔴 Errors", errors, f"{errors/total*100:.1f}%")
                    
                    # Show mismatches
                    if mismatched > 0:
                        st.subheader("🔍 Mismatched Records")
                        mismatch_df = result_df[result_df['validation_status'] == 'mismatch'][
                            ['reg_address', 'reg_city', 'polling_place_name', 'va_polling_place_returned', 'match_score']
                        ]
                        st.dataframe(mismatch_df, use_container_width=True)

                    if errors > 0:
                        st.subheader("🚨 Error Rows")
                        error_columns = [
                            'reg_address',
                            'reg_city',
                            'polling_place_name',
                            'validation_status',
                            'validation_notes',
                            'validation_error',
                        ]
                        available_error_columns = [
                            column for column in error_columns if column in result_df.columns
                        ]
                        error_df = result_df[result_df['validation_status'] == 'error'][
                            available_error_columns
                        ]
                        st.dataframe(error_df, use_container_width=True)
                    
                    # Download button
                    st.divider()
                    
                    csv_data = output_path.read_text()
                    st.download_button(
                        label="📥 Download Results CSV",
                        data=csv_data,
                        file_name=f"{temp_path.stem}_validated.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary"
                    )
                    
                    # Preview results
                    with st.expander("Preview validated data"):
                        st.dataframe(result_df.head(20), use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Error during validation: {e}")
                    raise e
                
                finally:
                    # Cleanup
                    if temp_path.exists():
                        temp_path.unlink()
        
    except Exception as e:
        st.error(f"Error loading CSV: {e}")

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.9em;">
    Data validated against <a href="https://www.elections.virginia.gov" target="_blank">Virginia Department of Elections</a> 
    via <a href="https://developers.google.com/civic-information" target="_blank">Google Civic Information API</a>
</div>
""", unsafe_allow_html=True)
