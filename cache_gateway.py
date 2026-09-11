"""Cache-aside gateway for the existing TransactionService gRPC backend."""

from __future__ import annotations

import json
import sys
from typing import Any

import grpc
import redis

import service_pb2
import service_pb2_grpc


REDIS_HOST = "localhost"
REDIS_PORT = 6379
CACHE_TTL_SECONDS = 10
BACKEND_SERVICE_ADDR = "localhost:50051"
FRESH_KEY_PREFIX = "fresh:status:"
STALE_KEY_PREFIX = "stale:status:"


def get_redis() -> redis.Redis:
    """Create a short-lived Redis client that fails fast when Redis is down."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        socket_connect_timeout=1,
        socket_timeout=1,
    )


def _status_to_dict(response: service_pb2.StatusResponse) -> dict[str, Any]:
    return {
        "healthy": response.healthy,
        "message": response.message,
        "active_records": response.active_records,
        "lamport_timestamp": response.lamport_timestamp,
    }


def fetch_from_backend(service_name: str) -> dict[str, Any]:
    """Fetch a status response directly from the existing gRPC backend."""
    with grpc.insecure_channel(BACKEND_SERVICE_ADDR) as channel:
        stub = service_pb2_grpc.TransactionServiceStub(channel)
        response = stub.GetStatus(
            service_pb2.StatusRequest(service_name=service_name),
            timeout=2,
        )
    return _status_to_dict(response)


def get_status(service_name: str) -> tuple[dict[str, Any], str]:
    """Return status from fresh cache, backend, or an indefinitely kept stale copy."""
    fresh_key = f"{FRESH_KEY_PREFIX}{service_name}"
    stale_key = f"{STALE_KEY_PREFIX}{service_name}"
    client = None

    try:
        client = get_redis()
        cached = client.get(fresh_key)
        if cached:
            print(f"[Gateway] CACHE HIT for {service_name}")
            return json.loads(cached), "cache-hit"
        print(f"[Gateway] CACHE MISS for {service_name} -- querying backend")
    except redis.exceptions.RedisError as exc:
        print(f"[Gateway] REDIS UNAVAILABLE ({exc}) -- falling back to backend directly")

    try:
        data = fetch_from_backend(service_name)
        print(f"[Gateway] Fetched {service_name} from BACKEND")
        if client is not None:
            try:
                encoded = json.dumps(data)
                client.set(fresh_key, encoded, ex=CACHE_TTL_SECONDS)
                client.set(stale_key, encoded)
            except redis.exceptions.RedisError:
                print("[Gateway] Redis write failed, continuing without caching this result")
        return data, "backend"
    except grpc.RpcError as exc:
        print(f"[Gateway] BACKEND UNAVAILABLE ({exc.code()}) -- checking for stale cache")
        if client is not None:
            try:
                stale = client.get(stale_key)
                if stale:
                    print(f"[Gateway] Serving STALE cached copy for {service_name}")
                    return json.loads(stale), "stale-fallback"
            except redis.exceptions.RedisError:
                pass
        raise RuntimeError(
            f"Status for {service_name} unavailable: both backend and cache failed"
        ) from exc


def main() -> None:
    service_name = sys.argv[1] if len(sys.argv) > 1 else "ExpenseManagerClient"
    data, source = get_status(service_name)
    print(f"\nResult (source={source}): {data}")


if __name__ == "__main__":
    main()