"""Concurrent gRPC load client for local and Kubernetes deployments."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

import worker_pb2
import worker_pb2_grpc


TARGET_ADDR = os.environ.get("TARGET_ADDR", "localhost:60201")
NUM_REQUESTS = int(os.environ.get("NUM_REQUESTS", "20"))


def grpc_target(address: str) -> str:
    """Use the DNS resolver when targeting a Kubernetes Service."""
    if address.startswith("dns:///"):
        return address
    return f"dns:///{address}"


def send_request(stub: worker_pb2_grpc.WorkerServiceStub, request_id: int):
    return stub.HandleRequest(worker_pb2.WorkRequest(request_id=request_id))


def main() -> None:
    target = grpc_target(TARGET_ADDR)
    options = [("grpc.lb_policy_name", "round_robin")]
    print(f"[LOAD] Sending {NUM_REQUESTS} requests to {target}", flush=True)

    with grpc.insecure_channel(target, options=options) as channel:
        grpc.channel_ready_future(channel).result(timeout=30)
        stub = worker_pb2_grpc.WorkerServiceStub(channel)
        with ThreadPoolExecutor(max_workers=NUM_REQUESTS) as executor:
            futures = [
                executor.submit(send_request, stub, request_id)
                for request_id in range(1, NUM_REQUESTS + 1)
            ]
            successes = 0
            for future in as_completed(futures):
                reply = future.result()
                successes += reply.status == "done"
                print(
                    f"[LOAD] Request {reply.request_id} -> {reply.handled_by} "
                    f"({reply.status})",
                    flush=True,
                )

    print(f"[LOAD] Completed {successes}/{NUM_REQUESTS} requests", flush=True)


if __name__ == "__main__":
    main()