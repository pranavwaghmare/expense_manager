import random
import sys
import threading
import time
from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc

ALL_NODES = {
    "A": "localhost:60401",
    "B": "localhost:60402",
    "C": "localhost:60403",
}

HEARTBEAT_INTERVAL = 0.5
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id):
        self.id = node_id
        self.peers = {n: addr for n, addr in ALL_NODES.items() if n != node_id}
        self.lock = threading.Lock()
        self.state = "FOLLOWER"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()

    def log(self, msg):
        print(f"[{self.id} term={self.current_term} {self.state}] {msg}", flush=True)

    def _reset_election_timer(self):
        self.last_heartbeat = time.time()

    def _random_timeout(self):
        return random.uniform(*ELECTION_TIMEOUT_RANGE)

    def RequestVote(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.VoteReply(term=self.current_term, vote_granted=False)

            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = "FOLLOWER"

            grant = self.voted_for in (None, request.candidate_id)
            if grant:
                self.voted_for = request.candidate_id
                self._reset_election_timer()
                self.log(f"Voted for {request.candidate_id} (term {request.term})")
                return raft_pb2.VoteReply(term=self.current_term, vote_granted=True)

            self.log(f"Rejected vote request from {request.candidate_id} (term {request.term})")
            return raft_pb2.VoteReply(term=self.current_term, vote_granted=False)

    def AppendEntries(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return raft_pb2.HeartbeatReply(term=self.current_term, success=False)

            self.current_term = request.term
            self.state = "FOLLOWER"
            self.leader_id = request.leader_id
            self._reset_election_timer()
            self.log(f"Received heartbeat from leader {request.leader_id}")
            return raft_pb2.HeartbeatReply(term=self.current_term, success=True)

    def _election_watchdog(self):
        while True:
            timeout = self._random_timeout()
            while True:
                time.sleep(0.1)
                if self.state == "LEADER":
                    break
                elapsed = time.time() - self.last_heartbeat
                if elapsed >= timeout:
                    self._start_election()
                    break

    def _start_election(self):
        with self.lock:
            if self.state == "LEADER":
                return
            self.state = "CANDIDATE"
            self.current_term += 1
            self.voted_for = self.id
            term = self.current_term
            self._reset_election_timer()
            self.log("Starting election")
            votes = 1

        for peer_id, addr in self.peers.items():
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    reply = stub.RequestVote(
                        raft_pb2.VoteRequest(term=term, candidate_id=self.id),
                        timeout=1,
                    )
                with self.lock:
                    if reply.term > term:
                        self.current_term = reply.term
                        self.state = "FOLLOWER"
                        self.voted_for = None
                        self.leader_id = None
                        self.log(f"Detected higher term {reply.term}; stepping down")
                        return
                    if reply.vote_granted:
                        votes += 1
            except grpc.RpcError:
                self.log(f"No response from {peer_id} (may be down)")

        with self.lock:
            if self.state == "CANDIDATE" and votes > len(ALL_NODES) // 2:
                self.state = "LEADER"
                self.leader_id = self.id
                self.log(f"*** ELECTED LEADER *** ({votes}/{len(ALL_NODES)} votes)")
            else:
                self.state = "FOLLOWER"
                self.leader_id = None
                self.log(f"Election failed ({votes}/{len(ALL_NODES)} votes) - reverting to follower")

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            if self.state != "LEADER":
                continue
            for peer_id, addr in self.peers.items():
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = raft_pb2_grpc.RaftServiceStub(channel)
                        stub.AppendEntries(
                            raft_pb2.HeartbeatRequest(term=self.current_term, leader_id=self.id),
                            timeout=1,
                        )
                except grpc.RpcError:
                    pass


def serve(node_id):
    node = RaftNode(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(node, server)
    port = ALL_NODES[node_id].split(":")[1]
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    print(f"RaftNode-{node_id} listening on {ALL_NODES[node_id]}", flush=True)

    threading.Thread(target=node._election_watchdog, daemon=True).start()
    threading.Thread(target=node._heartbeat_loop, daemon=True).start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python raft_node.py NODE_ID")
    serve(sys.argv[1])
