import json
import unittest
from unittest.mock import patch

import grpc

import cache_gateway


class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.writes = []

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value
        self.writes.append((key, value, ex))


class BackendUnavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class CacheGatewayTests(unittest.TestCase):
    def test_fresh_cache_hit_skips_backend(self):
        cached = {"healthy": True, "message": "cached", "active_records": 2}
        client = FakeRedis({"fresh:status:api": json.dumps(cached)})

        with patch("cache_gateway.get_redis", return_value=client), patch(
            "cache_gateway.fetch_from_backend"
        ) as fetch:
            data, source = cache_gateway.get_status("api")

        self.assertEqual((data, source), (cached, "cache-hit"))
        fetch.assert_not_called()

    def test_redis_failure_still_reads_backend(self):
        with patch("cache_gateway.get_redis", side_effect=cache_gateway.redis.exceptions.RedisError), patch(
            "cache_gateway.fetch_from_backend", return_value={"healthy": True}
        ) as fetch:
            data, source = cache_gateway.get_status("api")

        self.assertEqual((data, source), ({"healthy": True}, "backend"))
        fetch.assert_called_once_with("api")

    def test_backend_failure_uses_stale_copy(self):
        stale = {"healthy": True, "message": "old", "active_records": 2}
        client = FakeRedis({"stale:status:api": json.dumps(stale)})
        backend_error = BackendUnavailable()

        with patch("cache_gateway.get_redis", return_value=client), patch(
            "cache_gateway.fetch_from_backend", side_effect=backend_error
        ):
            data, source = cache_gateway.get_status("api")

        self.assertEqual((data, source), (stale, "stale-fallback"))


if __name__ == "__main__":
    unittest.main()