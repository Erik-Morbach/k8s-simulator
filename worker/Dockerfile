FROM python:3.13-slim
WORKDIR /app
COPY worker.py .
RUN pip install --no-cache-dir fastapi uvicorn[standard] psutil
EXPOSE 8000
CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8000"]