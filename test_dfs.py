import hashlib
import tempfile
import unittest

from dfs_metadata import MetadataServicer


class MetadataTests(unittest.TestCase):
    def test_allocation_balances_live_nodes_and_persists_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            service = MetadataServicer(f"{directory}/metadata.json")
            for index in range(3):
                service.Heartbeat(
                    type("Request", (), {"node_id": f"store-{index}", "address": f"n{index}"})(),
                    None)
            reply = service.AllocateFile(
                type("Request", (), {
                    "filename": "a", "num_chunks": 4, "lamport_timestamp": 2})(),
                None)
            self.assertTrue(reply.ok)
            self.assertEqual({len(plan.node_ids) for plan in reply.plan}, {2})
            commit = service.CommitFile(type("Request", (), {
                "filename": "a", "version": reply.version, "size": 3,
                "sha256": hashlib.sha256(b"abc").hexdigest(),
                "chunks": [type("Chunk", (), {
                    "index": plan.index, "chunk_id": plan.chunk_id,
                    "sha256": plan.sha256, "node_ids": list(plan.node_ids)})()
                            for plan in reply.plan]})(), None)
            self.assertTrue(commit.ok)
            loaded = MetadataServicer(f"{directory}/metadata.json")
            self.assertTrue(loaded.GetFileInfo(
                type("Request", (), {"filename": "a"})(), None).found)


if __name__ == "__main__":
    unittest.main()
