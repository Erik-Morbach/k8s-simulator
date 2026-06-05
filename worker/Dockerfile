FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir "fastapi[standard]" psutil
WORKDIR /app
COPY worker.py .
CMD ["python", "-m", "fastapi", "run", "worker.py"]