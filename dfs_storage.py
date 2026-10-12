from __future__ import annotations

import hashlib
import os
import sys
import threading
import time
from concurrent import futures

import grpc

import dfs_pb2
import dfs_pb2_grpc
import net_config


class StorageServicer(dfs_pb2_grpc.DfsStorageServiceServicer):
    def __init__(self, node_id: str, directory: str) -> None:
        self.node_id, self.directory = node_id, directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, chunk_id: str) -> str:
        return os.path.join(self.directory, chunk_id.replace("/", "_"))

    def PutChunk(self, request, context):
        if hashlib.sha256(request.data).hexdigest() != request.sha256:
            return dfs_pb2.DfsAck(ok=False, message="checksum mismatch")
        path = self._path(request.chunk_id)
        temporary = path + ".tmp"
        with open(temporary, "wb") as stream:
            stream.write(request.data)
        os.replace(temporary, path)
        return dfs_pb2.DfsAck(ok=True, message="stored")

    def GetChunk(self, request, context):
        path = self._path(request.chunk_id)
        if not os.path.exists(path):
            context.abort(grpc.StatusCode.NOT_FOUND, "chunk not found")
        with open(path, "rb") as stream:
            data = stream.read()
        return dfs_pb2.DfsChunkData(
            chunk_id=request.chunk_id, data=data,
            sha256=hashlib.sha256(data).hexdigest())


def heartbeat_loop(node_id: str, address: str) -> None:
    while True:
        try:
            with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
                dfs_pb2_grpc.DfsMetadataServiceStub(channel).Heartbeat(
                    dfs_pb2.DfsNodeInfo(node_id=node_id, address=address), timeout=2)
        except grpc.RpcError:
            pass
        time.sleep(2)


def serve(node_number: str) -> None:
    node_id = f"store-{node_number}"
    address = net_config.resolve(f"dfs-store-{node_number}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DfsStorageServiceServicer_to_server(
        StorageServicer(node_id, os.path.join("dfs_data", node_id)), server)
    server.add_insecure_port(f"0.0.0.0:{address.rsplit(':', 1)[1]}")
    server.start()
    threading.Thread(target=heartbeat_loop, args=(node_id, address), daemon=True).start()
    print(f"{node_id} listening on {address}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python dfs_storage.py <1|2|3>")
    serve(sys.argv[1])
