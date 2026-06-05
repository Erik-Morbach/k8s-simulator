import threading
import os
import datetime
from dataclasses import dataclass
import time
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
import subprocess
import sys
import psutil
from queue import Queue

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse


@dataclass
class Response:
    id: str
    completed: bool
    startDate: datetime.date
    duration: float
    err: str
    out: str


def worker_task_execution_loop(taskQueue):
    global responseData
    global taskMapper
    while True:
        task_id = taskQueue.get()
        t0 = time.time()
        responseData[task_id] = Response(id=task_id,
            out="[Executing]",
            err="",
            completed=False,
            startDate=datetime.datetime.fromtimestamp(t0),
            duration=-1
        )
        target_path = taskMapper[task_id]
        try:
            result = subprocess.run(
                [sys.executable, str(target_path)],
                capture_output=True,
                text=True,
                timeout=15,
                cwd="/tmp",
            )
            t1 = time.time()
            responseData[task_id] = Response(id=task_id,
                out=result.stdout,
                err=result.stderr,
                startDate=datetime.datetime.fromtimestamp(t0),
                duration=t1-t0,
                completed=True
            )
        except Exception as err:
            responseData[task_id] = Response(id=task_id,
                out="",
                err=err,
                startDate=datetime.datetime.fromtimestamp(t0),
                duration=-1,
                completed=True
            )

@asynccontextmanager
async def lifespan(app: FastAPI):
    global taskQueue
    global responseData
    global taskMapper
    responseData = {}
    taskMapper = {}
    taskQueue = Queue()
    task = threading.Thread(target=worker_task_execution_loop, args=(taskQueue,))
    task.start()
    yield


app = FastAPI(title="Worker API", lifespan=lifespan)

@app.post("/execute")
async def execute_python_file(file: UploadFile = File(...)):
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are accepted.")

    global taskQueue
    global taskMapper
    target_path = Path("/tmp") / file.filename
    contents = await file.read()
    target_path.write_bytes(contents)
    id = str(uuid.uuid4().hex)
    taskMapper[id] = str(target_path)
    taskQueue.put(id)
    responseData[id] = Response(id=id,
        out="[inQueue]",
        err="",
        completed=False,
        startDate=datetime.datetime.fromtimestamp(time.time()),
        duration=-1
    )
    return JSONResponse(content={
        "id": id,
        "filepath": str(target_path)
    })

@app.get("/response/{id}")
async def get_response_of_execution(id):
    global responseData
    response = responseData[id]
    return JSONResponse(content={
        "id": response.id,
        "completed": response.completed,
        "startDate": str(response.startDate),
        "duration": response.duration,
        "stderr": response.err,
        "stdout": response.out
    })

@app.get("/status")
def status():
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    latency = int(os.environ.get("LAT", ""))
    time.sleep(latency/1000)

    return {
        "cpu": {
            "percent": cpu_percent,
        },
        "memory": {
            "total": memory.total,
            "available": memory.available,
            "used": memory.used,
            "percent": memory.percent,
        },
        "disk": {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent,
        },
        "tasks": {
            "pending": taskQueue.qsize(),
        }
    }