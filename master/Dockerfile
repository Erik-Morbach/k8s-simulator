FROM python:3.13-slim
WORKDIR /app
COPY master.py .
RUN pip install --no-cache-dir fastapi uvicorn[standard] psutil httpx
EXPOSE 8000
CMD ["uvicorn", "master:app", "--host", "0.0.0.0", "--port", "8000"]
