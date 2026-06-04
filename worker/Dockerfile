FROM python:3.13-slim
WORKDIR /app
COPY worker.py .
RUN pip install flask
CMD ["python", "worker.py"]