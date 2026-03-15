import os
import uuid
import asyncio
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
import uvicorn
from dotenv import load_dotenv

from services.rag_engine import process_documents_and_score

load_dotenv()

app = FastAPI(
    title="Anupalan Mitra API",
    description="Backend engine for AI-powered ISO Compliance Assessment using Gemini",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store (use Redis/DB in production)
JOB_STORE: dict[str, dict] = {}


class AssessmentResponse(BaseModel):
    job_id: str
    status: str
    message: str


@app.get("/")
def health_check():
    return {"status": "operational", "service": "Anupalan Mitra", "version": "2.0"}


@app.post("/api/v1/assess", response_model=AssessmentResponse)
async def create_assessment(
    background_tasks: BackgroundTasks,
    framework: str = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Receives company policy PDFs + target ISO framework.
    Saves files, initiates async Gemini RAG assessment, returns a job_id.
    """
    job_id = str(uuid.uuid4())
    temp_dir = f"./temp/{job_id}"
    os.makedirs(temp_dir, exist_ok=True)

    saved_files = []
    for file in files:
        path = f"{temp_dir}/{file.filename}"
        with open(path, "wb+") as f:
            f.write(await file.read())
        saved_files.append(path)

    JOB_STORE[job_id] = {"status": "processing", "result": None}

    async def run_pipeline():
        result = await process_documents_and_score(job_id, framework, saved_files)
        JOB_STORE[job_id] = {"status": "completed", "result": result}

    background_tasks.add_task(run_pipeline)

    return AssessmentResponse(
        job_id=job_id,
        status="processing",
        message=f"Assessment started. Poll /api/v1/results/{job_id} for progress."
    )


@app.get("/api/v1/results/{job_id}")
def get_results(job_id: str):
    """
    Returns the structured assessment result.
    Frontend should poll this until status == 'completed'.
    """
    job = JOB_STORE.get(job_id)
    if not job:
        return {"status": "not_found", "detail": "Unknown job ID"}
    if job["status"] == "processing":
        return {"status": "processing", "detail": "AI assessment in progress..."}
    return job["result"]


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
