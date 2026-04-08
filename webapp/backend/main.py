"""FastAPI backend for VA Polling Validator web application."""

import asyncio
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
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

UPLOAD_DIR = Path(__file__).parent / "uploads"
RESULTS_DIR = Path(__file__).parent / "results"
UPLOAD_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

jobs: dict[str, dict] = {}


class JobConfig(BaseModel):
    match_threshold: float = 85.0
    request_delay: float = 2.0


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


async def run_validation_job(job_id: str, input_path: Path, config: ValidatorConfig):
    """Background task to run validation."""
    try:
        jobs[job_id]["status"] = "running"
        jobs[job_id]["start_time"] = datetime.now()
        
        output_path = RESULTS_DIR / f"{job_id}_validated.csv"
        
        def progress_callback(progress: ValidationProgress):
            update_job_progress(job_id, progress)
        
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
        "message": "File uploaded successfully. Call /api/validate/{job_id} to start validation.",
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
    
    background_tasks.add_task(
        run_validation_job,
        job_id,
        Path(job["file_path"]),
        validator_config,
    )
    
    return {"job_id": job_id, "status": "started"}


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
    
    return FileResponse(
        result_file,
        media_type="text/csv",
        filename=f"{job_id}_validated.csv",
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
