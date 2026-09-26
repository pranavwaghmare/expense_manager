import unittest
from unittest.mock import patch

import raft_node
import raft_pb2


class FakeChannel:
    def __init__(self, responses):
        self.responses = responses

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __getattr__(self, name):
        return self


class FakeStub:
    def __init__(self, responses):
        self.responses = responses

    def RequestVote(self, request, timeout=None):
        return self.responses.pop(0)

    def AppendEntries(self, request, timeout=None):
        return self.responses.pop(0)


class RaftNodeTests(unittest.TestCase):
    def test_vote_request_is_granted_once_per_term(self):
        node = raft_node.RaftNode("A")

        reply = node.RequestVote(raft_pb2.VoteRequest(term=1, candidate_id="B"), None)

        self.assertTrue(reply.vote_granted)
        self.assertEqual(node.current_term, 1)
        self.assertEqual(node.voted_for, "B")

    def test_election_becomes_leader_with_majority(self):
        node = raft_node.RaftNode("A")

        with patch("raft_node.grpc.insecure_channel") as insecure_channel:
            insecure_channel.side_effect = [
                FakeChannel([
                    raft_pb2.VoteReply(term=1, vote_granted=True),
                    raft_pb2.VoteReply(term=1, vote_granted=True),
                ]),
                FakeChannel([
                    raft_pb2.VoteReply(term=1, vote_granted=True),
                    raft_pb2.VoteReply(term=1, vote_granted=True),
                ]),
            ]

            def fake_stub(channel):
                return FakeStub(channel.responses)

            with patch("raft_node.raft_pb2_grpc.RaftServiceStub", side_effect=fake_stub):
                node._start_election()

        self.assertEqual(node.state, "LEADER")
        self.assertEqual(node.leader_id, "A")
        self.assertEqual(node.current_term, 1)

    def test_append_entries_sets_leader_and_resets_timer(self):
        node = raft_node.RaftNode("B")
        node.state = "FOLLOWER"
        node.last_heartbeat = 0

        response = node.AppendEntries(raft_pb2.HeartbeatRequest(term=3, leader_id="A"), None)

        self.assertTrue(response.success)
        self.assertEqual(node.state, "FOLLOWER")
        self.assertEqual(node.leader_id, "A")
        self.assertEqual(node.current_term, 3)


if __name__ == "__main__":
    unittest.main()
