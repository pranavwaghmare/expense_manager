from __future__ import annotations

import hashlib

import grpc

import dfs_pb2
import dfs_pb2_grpc
import net_config

REPLICATION_FACTOR = 2


def repair() -> int:
    copied = 0
    with grpc.insecure_channel(net_config.resolve("dfs-meta")) as channel:
        metadata = dfs_pb2_grpc.DfsMetadataServiceStub(channel)
        status = metadata.ClusterStatus(dfs_pb2.DfsStatusRequest(), timeout=5)
        live = {node.node_id: node.address for node in status.nodes if node.alive}
        for file_info in status.files:
            for chunk in file_info.chunks:
                live_holders = [node for node in chunk.node_ids if node in live]
                targets = [node for node in live if node not in chunk.node_ids]
                if len(live_holders) >= REPLICATION_FACTOR:
                    continue
                data = None
                for node in live_holders:
                    try:
                        with grpc.insecure_channel(live[node]) as storage_channel:
                            reply = dfs_pb2_grpc.DfsStorageServiceStub(storage_channel).GetChunk(
                                dfs_pb2.DfsChunkId(chunk_id=chunk.chunk_id), timeout=5)
                        if hashlib.sha256(reply.data).hexdigest() == chunk.sha256:
                            data = reply.data
                            break
                    except grpc.RpcError:
                        continue
                if data is None:
                    print(f"Cannot repair {file_info.filename} chunk {chunk.index}")
                    continue
                for target in targets[:REPLICATION_FACTOR - len(live_holders)]:
                    try:
                        with grpc.insecure_channel(live[target]) as storage_channel:
                            ack = dfs_pb2_grpc.DfsStorageServiceStub(storage_channel).PutChunk(
                                dfs_pb2.DfsChunkData(
                                    chunk_id=chunk.chunk_id, data=data, sha256=chunk.sha256),
                                timeout=5)
                        if ack.ok and metadata.AddReplica(dfs_pb2.DfsReplicaUpdate(
                                filename=file_info.filename, chunk_index=chunk.index,
                                node_id=target), timeout=5).ok:
                            copied += 1
                    except grpc.RpcError:
                        continue
    print(f"Repair copied {copied} chunk(s)")
    return copied


if __name__ == "__main__":
    repair()
