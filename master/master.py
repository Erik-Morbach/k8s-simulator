from contextlib import asynccontextmanager
import datetime
import random
import threading
import asyncio
from queue import Queue
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
import psutil
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse


@dataclass
class Job:
    job_id: str
    filename: str
    payload: bytes
    status: str = "queued"
    queued_at: float = field(default_factory=time.time)
    allocation_type: str = "any"
    transaction_id: Optional[str] = None
    assigned_at: Optional[float] = None
    completed_at: Optional[datetime.datetime] = None
    worker_url: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class WorkerInfo:
    url: str
    healthy: bool = False
    cpu: Optional[float] = None
    memory: Optional[int] = None
    disk: Optional[int] = None
    latency_ms: Optional[float] = None
    last_seen: Optional[float] = None
    assigned_jobs: List[str] = field(default_factory=list)
    executed_ok: int = 0
    executed_error: int = 0
    pending: int = 0
    error: Optional[str] = None


def calculate_worker_score(job: Job, worker: WorkerInfo):
    match(job.allocation_type):
        case "any":
            return random.randint(1, 100)
        case "memory":
            return -worker.memory
        case "latency":
            return -worker.latency_ms
        case "start":
            return -worker.pending

WORKER_URLS = [url.strip() for url in os.environ.get("WORKER_URLS", "").split(",") if url.strip()]

job_queue: Queue[Job] = Queue()
jobs: Dict[str, Job] = {}
workers: Dict[str, WorkerInfo] = {url: WorkerInfo(url=url) for url in WORKER_URLS}

round_robin_index = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=worker_status_loop).start()
    threading.Thread(target=dispatch_loop).start()
    yield

app = FastAPI(title="Master API", lifespan=lifespan)

@app.post("/schedule")
async def schedule_job(file: UploadFile = File(...), allocation_type: str="any"):
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are accepted.")

    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    job_id = uuid.uuid4().hex
    job = Job(job_id=job_id, filename=file.filename, payload=payload, allocation_type=allocation_type)
    jobs[job_id] = job

    job_queue.put(job)

    return JSONResponse(
        status_code=202,
        content={"job_id": job_id, "status": job.status, "queued_at": job.queued_at},
    )


@app.get("/status")
async def status():
    currentJobs = jobs.values()
    master_cpu = psutil.cpu_percent(interval=0.1)
    master_memory = psutil.virtual_memory()
    master_disk = psutil.disk_usage("/")

    queued_jobs = [job for job in currentJobs if job.status == "queued"]
    assigned_jobs = [job for job in currentJobs if job.status == "assigned"]

    return {
        "master": {
            "cpu": master_cpu,
            "memory": {
                "total": f"%.3f Gb" % (master_memory.total/(1024**3)),
                "available": f"%.3f Gb" % (master_memory.available/(1024**3)),
                "used": f"%.3f Gb" % (master_memory.used/(1024**3)),
                "percent": master_memory.percent,
            },
            "disk": {
                "total": f"%.3f Gb" % (master_disk.total/(1024**3)),
                "used": f"%.3f Gb" % (master_disk.used/(1024**3)),
                "free": f"%.3f Gb" % (master_disk.free/(1024**3)),
                "percent": master_disk.percent,
            },
            "queue": {
                "pending_jobs": len(queued_jobs),
                "assigned_jobs": len(assigned_jobs),
                "total_jobs": len(jobs),
            },
        },
        "workers": [
            {
                "url": worker.url,
                "healthy": worker.healthy,
                "latency_ms": worker.latency_ms,
                "cpu": worker.cpu,
                "memory": worker.memory,
                "disk": worker.disk,
                "error": worker.error,
                "assigned_jobs": worker.assigned_jobs,
                "executed_correctly": worker.executed_ok,
                "executed_error": worker.executed_error,
                "pending": worker.pending,
            } for worker in workers.values()
        ],
        "jobs": [
            {
                "job_id": job.job_id,
                "transaction_id": job.transaction_id,
                "filename": job.filename,
                "status": job.status,
                "worker_url": job.worker_url,
                "queued_at": job.queued_at,
                "assigned_at": job.assigned_at,
                "completed_at": job.completed_at,
                "error": job.error,
            }
            for job in currentJobs
        ],
    }


def worker_status_loop() -> None:
    with httpx.Client(timeout=10.0) as client:
        while True:
            for worker in workers.values():
                update_worker_status(client, worker)
                update_worker_job_status(client, worker)
            time.sleep(1)


def update_worker_status(client: httpx.Client, worker: WorkerInfo) -> None:
    try:
        start = time.perf_counter()
        response = client.get(f"{worker.url}/status")
        elapsed = (time.perf_counter() - start) * 1000.0
        response.raise_for_status()

        data = response.json()
        worker.healthy = True
        worker.latency_ms = elapsed
        worker.last_seen = time.time()
        worker.cpu = data.get("cpu", {}).get("percent")
        worker.memory = int(data.get("memory", {}).get("used"))
        worker.disk = int(data.get("disk", {}).get("free"))
        worker.error = None
        worker.pending = data.get("tasks", {}).get("pending")
    except Exception as exc:
        worker.healthy = False
        worker.error = str(exc)
        worker.latency_ms = None
        worker.cpu = None
        worker.memory = None
        worker.disk = None

def update_worker_job_status(client: httpx.Client, worker: WorkerInfo) -> None:
    try:
        to_be_deleted = []
        for job_id in worker.assigned_jobs:
            job = jobs[job_id]
            response = client.get(f"{worker.url}/response/{job.transaction_id}")
            response = response.json()
            completed = response["completed"]
            if not completed:
                continue
            if len(response["stderr"]) != 0:
                worker.executed_error+=1
            else:
                worker.executed_ok+=1
            to_be_deleted += [job_id]
            completed_at = datetime.datetime.fromisoformat(response["startDate"])
            completed_at = completed_at.timestamp() + float(response["duration"])
            job.completed_at = datetime.datetime.fromtimestamp(completed_at)
            job.result = response
        for id in to_be_deleted:
            worker.assigned_jobs.remove(id)
    except Exception as exc:
        pass



def dispatch_loop() -> None:
    global round_robin_index

    with httpx.Client(timeout=60.0) as client:
        while True:
            job = job_queue.get()

            healthy_workers = [worker for worker in workers.values() if worker.healthy]
            if not healthy_workers:
                job.error = "No healthy workers available"
                job.status = "queued"
                job_queue.put(job)
                time.sleep(5)
                continue

            targetWorker = None
            currentScore = 0
            for i in range(len(healthy_workers)):
                worker = healthy_workers[i]
                score = calculate_worker_score(job, worker)
                print("score ", i, score)
                if targetWorker is None:
                    targetWorker = worker
                    currentScore = score
                    continue
                if score > currentScore:
                    targetWorker = worker
                    currentScore = score
                
            if targetWorker is None:
                job.error = "Failed to assign job to any worker"
                job.status = "failed"
                job.completed_at = time.time()
                continue

            for i in range(1, 10):
                try:
                    assign_job_to_worker(client, job, targetWorker)
                    break
                except Exception as exc:
                    print("Error on retry:",i)
                    targetWorker.healthy = False
                    targetWorker.error = str(exc)
                    time.sleep(i/5)


def assign_job_to_worker(client: httpx.Client, job: Job, worker: WorkerInfo) -> None:
    files = {"file": (job.filename, job.payload, "text/x-python")}
    response = client.post(f"{worker.url}/execute", files=files)
    job.transaction_id = str(response.json()["id"])
    job.status = "assigned"
    job.worker_url = worker.url
    job.assigned_at = time.time()
    worker.assigned_jobs.append(job.job_id)


@app.post("/config")
def config_notice(worker_url: str):
    global WORKER_URLS 
    worker_url = worker_url.strip()
    WORKER_URLS += [worker_url]
    global workers
    workers[worker_url] = WorkerInfo(url=worker_url)
    return {"msg": f"{worker_url} configured"}
