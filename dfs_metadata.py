from __future__ import annotations

import json
import os
import threading
import time
from concurrent import futures

import grpc

import dfs_pb2
import dfs_pb2_grpc

REPLICATION_FACTOR = 2
HEARTBEAT_TIMEOUT = 6.0
STATE_FILE = os.path.join("dfs_data", "metadata.json")


class MetadataServicer(dfs_pb2_grpc.DfsMetadataServiceServicer):
    def __init__(self, state_file: str = STATE_FILE) -> None:
        self.lock = threading.RLock()
        self.state_file = state_file
        self.clock = 0
        self.addresses: dict[str, str] = {}
        self.last_seen: dict[str, float] = {}
        self.files: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.state_file):
            return
        with open(self.state_file, encoding="utf-8") as stream:
            data = json.load(stream)
        self.clock = int(data.get("clock", 0))
        self.addresses = data.get("addresses", {})
        self.files = data.get("files", {})

    def _save(self) -> None:
        directory = os.path.dirname(self.state_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        temporary = self.state_file + ".tmp"
        with open(temporary, "w", encoding="utf-8") as stream:
            json.dump({"clock": self.clock, "addresses": self.addresses,
                       "files": self.files}, stream)
        os.replace(temporary, self.state_file)

    def _alive(self, node_id: str) -> bool:
        seen = self.last_seen.get(node_id)
        return seen is not None and time.time() - seen <= HEARTBEAT_TIMEOUT

    def _chunk_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.files.values():
            for chunk in record["chunks"]:
                for node_id in chunk["nodes"]:
                    counts[node_id] = counts.get(node_id, 0) + 1
        return counts

    def _file_info(self, filename: str, record: dict) -> dfs_pb2.DfsFileInfo:
        chunks = []
        for chunk in record["chunks"]:
            nodes = sorted(chunk["nodes"],
                           key=lambda node: (not self._alive(node), node))
            chunks.append(dfs_pb2.DfsChunkPlan(
                index=chunk["index"], chunk_id=chunk["chunk_id"],
                sha256=chunk["sha256"], node_ids=nodes,
                addresses=[self.addresses.get(node, "") for node in nodes],
                alive=[self._alive(node) for node in nodes]))
        return dfs_pb2.DfsFileInfo(
            found=True, filename=filename, version=record["version"],
            size=record["size"], sha256=record["sha256"], chunks=chunks)

    def Heartbeat(self, request, context):
        with self.lock:
            self.last_seen[request.node_id] = time.time()
            if self.addresses.get(request.node_id) != request.address:
                self.addresses[request.node_id] = request.address
                self._save()
        return dfs_pb2.DfsAck(ok=True, message="ok")

    def AllocateFile(self, request, context):
        if not request.filename or request.num_chunks <= 0:
            return dfs_pb2.DfsAllocateReply(ok=False, message="invalid file request")
        with self.lock:
            self.clock = max(self.clock, request.lamport_timestamp) + 1
            live = [node for node in self.addresses if self._alive(node)]
            if not live:
                return dfs_pb2.DfsAllocateReply(
                    ok=False, message="no live storage nodes",
                    lamport_timestamp=self.clock)
            self.clock += 1
            counts = self._chunk_counts()
            load = {node: counts.get(node, 0) for node in live}
            replicas = min(REPLICATION_FACTOR, len(live))
            plans = []
            for index in range(request.num_chunks):
                chosen = sorted(live, key=lambda node: (load[node], node))[:replicas]
                for node in chosen:
                    load[node] += 1
                plans.append(dfs_pb2.DfsChunkPlan(
                    index=index,
                    chunk_id=f"{request.filename}.v{self.clock}.c{index}",
                    node_ids=chosen,
                    addresses=[self.addresses[node] for node in chosen]))
            return dfs_pb2.DfsAllocateReply(
                ok=True, message="allocated", version=self.clock, plan=plans,
                lamport_timestamp=self.clock)

    def CommitFile(self, request, context):
        if not request.filename or not request.chunks or any(
                not chunk.node_ids for chunk in request.chunks):
            return dfs_pb2.DfsCommitReply(ok=False, message="invalid commit")
        with self.lock:
            self.files[request.filename] = {
                "version": request.version, "size": request.size,
                "sha256": request.sha256,
                "chunks": [{"index": chunk.index, "chunk_id": chunk.chunk_id,
                            "sha256": chunk.sha256,
                            "nodes": list(chunk.node_ids)}
                           for chunk in sorted(request.chunks, key=lambda c: c.index)],
            }
            self.clock += 1
            self._save()
            return dfs_pb2.DfsCommitReply(
                ok=True, message="committed", lamport_timestamp=self.clock)

    def GetFileInfo(self, request, context):
        with self.lock:
            record = self.files.get(request.filename)
            return (self._file_info(request.filename, record)
                    if record else dfs_pb2.DfsFileInfo(
                        found=False, filename=request.filename))

    def ClusterStatus(self, request, context):
        with self.lock:
            counts = self._chunk_counts()
            nodes = [dfs_pb2.DfsNodeStatus(
                node_id=node, address=address, alive=self._alive(node),
                chunk_count=counts.get(node, 0))
                     for node, address in sorted(self.addresses.items())]
            files = [self._file_info(name, record)
                     for name, record in sorted(self.files.items())]
            return dfs_pb2.DfsStatusReply(nodes=nodes, files=files)

    def AddReplica(self, request, context):
        with self.lock:
            record = self.files.get(request.filename)
            if not record:
                return dfs_pb2.DfsAck(ok=False, message="unknown file")
            for chunk in record["chunks"]:
                if chunk["index"] == request.chunk_index:
                    if request.node_id not in chunk["nodes"]:
                        chunk["nodes"].append(request.node_id)
                        self._save()
                    return dfs_pb2.DfsAck(ok=True, message="replica recorded")
        return dfs_pb2.DfsAck(ok=False, message="unknown chunk")

    def monitor(self) -> None:
        previous: dict[str, bool] = {}
        while True:
            time.sleep(1)
            with self.lock:
                for node in self.addresses:
                    alive = self._alive(node)
                    if node in previous and previous[node] != alive:
                        print(f"Node {node} is {'UP' if alive else 'DOWN'}", flush=True)
                    previous[node] = alive


def serve() -> None:
    servicer = MetadataServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DfsMetadataServiceServicer_to_server(servicer, server)
    server.add_insecure_port("0.0.0.0:60501")
    server.start()
    threading.Thread(target=servicer.monitor, daemon=True).start()
    print("DFS metadata server listening on port 60501", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
