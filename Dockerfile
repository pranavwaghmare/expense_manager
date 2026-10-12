FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py worker_server.py load_test.py worker_pb2.py worker_pb2_grpc.py ./

ENV RUN_ENV=docker
ENV PYTHONUNBUFFERED=1

CMD ["python", "worker_server.py", "60201"]