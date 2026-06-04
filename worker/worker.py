from pathlib import Path
from contextlib import asynccontextmanager
import subprocess
import sys
import tempfile
import psutil
from queue import Queue

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

taskQueue = Queue()

def worker_task_execution_loop():
    while True:
        target_path = taskQueue.get()
        try:
            result = subprocess.run(
                [sys.executable, str(target_path)],
                capture_output=True,
                text=True,
                timeout=15,
                cwd=tmpdir,
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="Script execution timed out.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_task_execution_loop())
    yield


app = FastAPI(title="Worker API", lifespan=lifespan)

@app.post("/execute")
async def execute_python_file(file: UploadFile = File(...)):
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are accepted.")

    with tempfile.TemporaryDirectory() as tmpdir:
        target_path = Path(tmpdir) / file.filename
        contents = await file.read()
        target_path.write_bytes(contents)
        taskQueue.put(target_path)
    return JSONResponse(
        content={
            "filename": file.filename,
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    )

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

if __name__ == "__main__":
    app.run()