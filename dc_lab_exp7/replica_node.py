from __future__ import annotations

import sys
import threading
import time
from concurrent import futures

import grpc

import replica_pb2
import replica_pb2_grpc

ALL_REPLICAS = {
    "A": "localhost:60301",
    "B": "localhost:60302",
    "C": "localhost:60303",
}


class ReplicaNode(replica_pb2_grpc.ReplicaServiceServicer):
    def __init__(self, name: str) -> None:
        self.name = name
        self.peers = {n: addr for n, addr in ALL_REPLICAS.items() if n != name}
        self.clock = 0
        self.lock = threading.Lock()
        self.store: dict[str, tuple[str, int, str]] = {}

    def log(self, msg: str) -> None:
        print(f"[{self.name} t={self.clock:>2}] {msg}", flush=True)

    def tick(self) -> int:
        with self.lock:
            self.clock += 1
            return self.clock

    def update_clock(self, received_ts: int) -> int:
        with self.lock:
            self.clock = max(self.clock, received_ts) + 1
            return self.clock

    def _apply_if_newer(self, key: str, content: str, ts: int, origin: str) -> bool:
        with self.lock:
            current = self.store.get(key)
            if current is None or (ts, origin) > (current[1], current[2]):
                self.store[key] = (content, ts, origin)
                self.log(
                    f"APPLIED '{key}' = \"{content}\" (ts={ts}, origin={origin})"
                )
                return True

            self.log(
                f"IGNORED stale update for '{key}' "
                f"(incoming ts={ts}/{origin} <= current ts={current[1]}/{current[2]})"
            )
            return False

    def _gossip(self, key: str, content: str, ts: int, origin: str) -> None:
        for peer_name, addr in self.peers.items():
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = replica_pb2_grpc.ReplicaServiceStub(channel)
                    stub.SyncUpdate(
                        replica_pb2.ValueUpdate(
                            key=key,
                            content=content,
                            lamport_timestamp=ts,
                            origin_replica=origin,
                        )
                    )
                    self.log(f"Gossiped '{key}' -> {peer_name}")
            except grpc.RpcError as exc:
                self.log(
                    f"Gossip to {peer_name} failed (will catch up later): {exc.code()}"
                )

    def SaveValue(self, request, context):
        ts = self.tick()
        self._apply_if_newer(request.key, request.content, ts, self.name)
        threading.Thread(
            target=self._gossip,
            args=(request.key, request.content, ts, self.name),
            daemon=True,
        ).start()
        return replica_pb2.SaveAck(
            accepted=True,
            replica=self.name,
            lamport_timestamp=ts,
        )

    def SyncUpdate(self, request, context):
        self.update_clock(request.lamport_timestamp)
        applied = self._apply_if_newer(
            request.key,
            request.content,
            request.lamport_timestamp,
            request.origin_replica,
        )
        return replica_pb2.SaveAck(
            accepted=applied,
            replica=self.name,
            lamport_timestamp=self.clock,
        )

    def GetValue(self, request, context):
        with self.lock:
            entry = self.store.get(request.key)
            if entry is None:
                return replica_pb2.ValueState(
                    key=request.key,
                    content="",
                    lamport_timestamp=0,
                    origin_replica="",
                )
            content, ts, origin = entry
            return replica_pb2.ValueState(
                key=request.key,
                content=content,
                lamport_timestamp=ts,
                origin_replica=origin,
            )


def serve(name: str, port: int) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    replica_pb2_grpc.add_ReplicaServiceServicer_to_server(ReplicaNode(name), server)
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    print(f"Replica-{name} listening on localhost:{port}", flush=True)
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python replica_node.py <A|B|C> <PORT>")
    serve(sys.argv[1], int(sys.argv[2]))
