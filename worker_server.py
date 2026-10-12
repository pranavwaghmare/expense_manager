"""Variable-duration backend server for the least-connections experiment."""

from __future__ import annotations

import random
import sys
import time
from concurrent import futures

import grpc

import worker_pb2
import worker_pb2_grpc


class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    def __init__(self, name: str) -> None:
        self.name = name

    def HandleRequest(self, request, context):
        work_time = random.uniform(0.5, 2.5)
        print(
            f"[{self.name}] Handling request {request.request_id} "
            f"(will take {work_time:.1f}s)",
            flush=True,
        )
        time.sleep(work_time)
        print(f"[{self.name}] Finished request {request.request_id}", flush=True)
        return worker_pb2.WorkReply(
            request_id=request.request_id,
            handled_by=self.name,
            status="done",
        )


def serve(port: int) -> None:
    name = f"Worker-{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServicer(name), server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"{name} listening on 0.0.0.0:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python worker_server.py PORT")
    serve(int(sys.argv[1]))