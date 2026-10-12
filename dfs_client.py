from __future__ import annotations

import hashlib
import os
import sys

import grpc

import dfs_pb2
import dfs_pb2_grpc
import net_config

CHUNK_SIZE = int(os.environ.get("DFS_CHUNK_SIZE", 64 * 1024))
CLOCK_FILE = ".dfs_client_clock"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _clock() -> int:
    try:
        with open(CLOCK_FILE, encoding="utf-8") as stream:
            return int(stream.read())
    except (OSError, ValueError):
        return 0


def _save_clock(value: int) -> None:
    with open(CLOCK_FILE, "w", encoding="utf-8") as stream:
        stream.write(str(value))


def _metadata() -> dfs_pb2_grpc.DfsMetadataServiceStub:
    return dfs_pb2_grpc.DfsMetadataServiceStub(
        grpc.insecure_channel(net_config.resolve("dfs-meta")))


def put(path: str) -> bool:
    filename = os.path.basename(path)
    with open(path, "rb") as stream:
        content = stream.read()
    pieces = [content[i:i + CHUNK_SIZE] for i in range(0, len(content), CHUNK_SIZE)] or [b""]
    clock = _clock() + 1
    try:
        with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
            metadata = dfs_pb2_grpc.DfsMetadataServiceStub(channel)
            allocation = metadata.AllocateFile(dfs_pb2.DfsAllocateRequest(
                filename=filename, num_chunks=len(pieces),
                lamport_timestamp=clock), timeout=5)
            if not allocation.ok:
                print(f"Upload failed: {allocation.message}")
                return False
            stored_chunks = []
            for plan in allocation.plan:
                digest = sha256(pieces[plan.index])
                stored = []
                for node_id, address in zip(plan.node_ids, plan.addresses):
                    try:
                        with grpc.insecure_channel(address) as storage_channel:
                            ack = dfs_pb2_grpc.DfsStorageServiceStub(storage_channel).PutChunk(
                                dfs_pb2.DfsChunkData(
                                    chunk_id=plan.chunk_id, data=pieces[plan.index],
                                    sha256=digest), timeout=5)
                        if ack.ok:
                            stored.append(node_id)
                    except grpc.RpcError:
                        continue
                if not stored:
                    print(f"Upload failed: no replica stored chunk {plan.index}")
                    return False
                stored_chunks.append(dfs_pb2.DfsChunkPlan(
                    index=plan.index, chunk_id=plan.chunk_id, sha256=digest,
                    node_ids=stored))
            commit = metadata.CommitFile(dfs_pb2.DfsCommitRequest(
                filename=filename, version=allocation.version, size=len(content),
                sha256=sha256(content), chunks=stored_chunks), timeout=5)
            _save_clock(max(clock, allocation.lamport_timestamp,
                            commit.lamport_timestamp) + 1)
            print(commit.message)
            return commit.ok
    except grpc.RpcError as exc:
        print(f"Metadata server unavailable: {exc.code().name}")
        return False


def get(filename: str, output_path: str) -> bool:
    try:
        with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
            info = dfs_pb2_grpc.DfsMetadataServiceStub(channel).GetFileInfo(
                dfs_pb2.DfsFileQuery(filename=filename), timeout=5)
    except grpc.RpcError as exc:
        print(f"Metadata server unavailable: {exc.code().name}")
        return False
    if not info.found:
        print(f"No file called '{filename}'")
        return False
    pieces = []
    for chunk in sorted(info.chunks, key=lambda item: item.index):
        for node_id, address in zip(chunk.node_ids, chunk.addresses):
            try:
                with grpc.insecure_channel(address) as channel:
                    reply = dfs_pb2_grpc.DfsStorageServiceStub(channel).GetChunk(
                        dfs_pb2.DfsChunkId(chunk_id=chunk.chunk_id), timeout=3)
                if sha256(reply.data) == chunk.sha256:
                    pieces.append(reply.data)
                    break
            except grpc.RpcError:
                continue
        else:
            print(f"Read failed: no healthy replica for chunk {chunk.index}")
            return False
    content = b"".join(pieces)
    if sha256(content) != info.sha256:
        print("Read failed: whole-file checksum mismatch")
        return False
    with open(output_path, "wb") as stream:
        stream.write(content)
    print(f"Wrote {len(content)} bytes to {output_path}")
    return True


def status() -> None:
    with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
        reply = dfs_pb2_grpc.DfsMetadataServiceStub(channel).ClusterStatus(
            dfs_pb2.DfsStatusRequest(), timeout=5)
    for node in reply.nodes:
        print(f"{node.node_id}: {'UP' if node.alive else 'DOWN'} ({node.chunk_count} chunks)")
    for file_info in reply.files:
        print(f"{file_info.filename}: {len(file_info.chunks)} chunks")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "put":
        raise SystemExit(0 if put(args[1]) else 1)
    if len(args) == 3 and args[0] == "get":
        raise SystemExit(0 if get(args[1], args[2]) else 1)
    if args == ["status"]:
        status()
    else:
        raise SystemExit("Usage: dfs_client.py put <path> | get <name> <output> | status")
