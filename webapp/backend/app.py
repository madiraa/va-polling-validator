"""Combined FastAPI app serving both API and frontend."""

import asyncio
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from va_polling_validator.models import ValidatorConfig, ValidationProgress, MatchStatus
from va_polling_validator.processor import run_validation, load_csv

app = FastAPI(
    title="VA Polling Place Validator",
    description="Validate Virginia polling place assignments against official VA elections data",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Directories
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
RESULTS_DIR = BASE_DIR / "results"
FRONTEND_DIR = BASE_DIR.parent / "frontend"

UPLOAD_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

# Job storage
jobs: dict[str, dict] = {}


class JobConfig(BaseModel):
    match_threshold: float = 85.0
    request_delay: float = 2.0
    use_api: bool = False
    api_key: Optional[str] = None
    rate_limit: float = 10.0


class JobStatus(BaseModel):
    job_id: str
    status: str
    total_records: int = 0
    completed_records: int = 0
    matched: int = 0
    mismatched: int = 0
    not_found: int = 0
    errors: int = 0
    progress_pct: float = 0.0
    start_time: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    result_file: Optional[str] = None
    error_message: Optional[str] = None


def update_job_progress(job_id: str, progress: ValidationProgress):
    """Callback to update job progress."""
    if job_id in jobs:
        jobs[job_id]["progress"] = progress
        jobs[job_id]["status"] = "running"


async def run_validation_job(job_id: str, input_path: Path, config: ValidatorConfig, job_config: JobConfig):
    """Background task to run validation."""
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["start_time"] = datetime.now()
        
        input_stem = input_path.stem
        output_path = RESULTS_DIR / f"{input_stem}_validated.csv"
        
        def progress_callback(progress: ValidationProgress):
            update_job_progress(job_id, progress)
        
        if job_config.use_api and job_config.api_key:
            from va_polling_validator.api_validator import run_api_validation
            
            df, records = load_csv(input_path)
            
            results = await run_api_validation(
                records=records,
                api_key=job_config.api_key,
                config=config,
                requests_per_second=job_config.rate_limit,
                concurrency=min(5, int(job_config.rate_limit)),
                progress_callback=progress_callback,
            )
            
            from va_polling_validator.processor import save_results
            save_results(df, results, output_path)
            
            final_progress = ValidationProgress(
                job_id=job_id,
                total_records=len(records),
                completed_records=len(results),
                matched=sum(1 for r in results if r.status == MatchStatus.MATCH),
                mismatched=sum(1 for r in results if r.status == MatchStatus.MISMATCH),
                not_found=sum(1 for r in results if r.status == MatchStatus.NOT_FOUND),
                errors=sum(1 for r in results if r.status == MatchStatus.ERROR),
            )
        else:
            results, final_progress = await run_validation(
                input_path=input_path,
                output_path=output_path,
                config=config,
                progress_callback=progress_callback,
            )
        
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["progress"] = final_progress
        jobs[job_id]["result_file"] = str(output_path)
        
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error_message"] = str(e)


# Serve frontend
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main frontend page."""
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text(), status_code=200)
    return HTMLResponse(content="<h1>Frontend not found</h1>", status_code=404)


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload a CSV file for validation."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "File must be a CSV")
    
    job_id = str(uuid.uuid4())[:8]
    file_path = UPLOAD_DIR / f"{job_id}_{file.filename}"
    
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    
    try:
        df, records = load_csv(file_path)
        total_records = len(records)
    except Exception as e:
        file_path.unlink()
        raise HTTPException(400, f"Invalid CSV format: {e}")
    
    jobs[job_id] = {
        "job_id": job_id,
        "status": "uploaded",
        "file_path": str(file_path),
        "filename": file.filename,
        "total_records": total_records,
        "progress": None,
        "result_file": None,
        "error_message": None,
        "start_time": None,
    }
    
    return {
        "job_id": job_id,
        "filename": file.filename,
        "total_records": total_records,
        "message": "File uploaded successfully.",
    }


@app.post("/api/validate/{job_id}")
async def start_validation(
    job_id: str,
    config: JobConfig,
    background_tasks: BackgroundTasks,
):
    """Start validation for an uploaded file."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    job = jobs[job_id]
    if job["status"] not in ["uploaded", "failed"]:
        raise HTTPException(400, f"Job is already {job['status']}")
    
    validator_config = ValidatorConfig(
        match_threshold=config.match_threshold,
        request_delay=config.request_delay,
        headless=True,
    )
    
    mode = "API" if config.use_api and config.api_key else "Browser"
    jobs[job_id]["mode"] = mode
    
    background_tasks.add_task(
        run_validation_job,
        job_id,
        Path(job["file_path"]),
        validator_config,
        config,
    )
    
    return {"job_id": job_id, "status": "started", "mode": mode}


@app.get("/api/status/{job_id}")
async def get_status(job_id: str) -> JobStatus:
    """Get the status of a validation job."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    job = jobs[job_id]
    progress = job.get("progress")
    
    return JobStatus(
        job_id=job_id,
        status=job["status"],
        total_records=progress.total_records if progress else job.get("total_records", 0),
        completed_records=progress.completed_records if progress else 0,
        matched=progress.matched if progress else 0,
        mismatched=progress.mismatched if progress else 0,
        not_found=progress.not_found if progress else 0,
        errors=progress.errors if progress else 0,
        progress_pct=progress.progress_pct if progress else 0.0,
        start_time=job.get("start_time"),
        last_updated=progress.last_updated if progress else None,
        result_file=job.get("result_file"),
        error_message=job.get("error_message"),
    )


@app.get("/api/download/{job_id}")
async def download_results(job_id: str):
    """Download the validation results CSV."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    job = jobs[job_id]
    if job["status"] != "completed":
        raise HTTPException(400, "Validation not yet completed")
    
    result_file = job.get("result_file")
    if not result_file or not Path(result_file).exists():
        raise HTTPException(404, "Result file not found")
    
    original_filename = job.get("filename", "results.csv")
    stem = Path(original_filename).stem
    download_filename = f"{stem}_validated.csv"
    
    return FileResponse(
        result_file,
        media_type="text/csv",
        filename=download_filename,
    )


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    return [
        {
            "job_id": jid,
            "status": job["status"],
            "filename": job.get("filename"),
            "total_records": job.get("total_records", 0),
        }
        for jid, job in jobs.items()
    ]


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its files."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    
    job = jobs[job_id]
    
    file_path = job.get("file_path")
    if file_path and Path(file_path).exists():
        Path(file_path).unlink()
    
    result_file = job.get("result_file")
    if result_file and Path(result_file).exists():
        Path(result_file).unlink()
    
    del jobs[job_id]
    
    return {"message": "Job deleted"}


if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
