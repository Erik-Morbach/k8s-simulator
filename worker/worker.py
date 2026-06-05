import threading
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
import subprocess
import sys
import psutil
from queue import Queue

from fastapi import FastAPI, File, HTTPException, UploadFile

def worker_task_execution_loop(taskQueue):
    global responseData
    global taskMapper
    while True:
        task_id = taskQueue.get()
        target_path = taskMapper[task_id]
        try:
            result = subprocess.run(
                [sys.executable, str(target_path)],
                capture_output=True,
                text=True,
                timeout=15,
                cwd="/tmp",
            )
            responseData[task_id] = result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            responseData[task_id] = "", "Timeout error"


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
    id = str(uuid.uuid4())
    taskMapper[id] = target_path
    taskQueue.put(id)
    return {
        "id": id,
        "filepath": target_path
    }

@app.get("/response/{id}")
async def get_response_of_execution(id):
    global responseData
    out, err = responseData[id]
    return {
        "id": id,
        "stdout": out,
        "stderr": err,
    }

@app.get("/status")
def status():
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

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