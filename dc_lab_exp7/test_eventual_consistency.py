import unittest

from replica_node import ReplicaNode


class EventualConsistencyTests(unittest.TestCase):
    def test_lww_prefers_later_lamport_value(self):
        node = ReplicaNode("A")
        node._apply_if_newer("shared", "old", 2, "B")
        node._apply_if_newer("shared", "new", 3, "C")
        self.assertEqual(node.store["shared"][0], "new")

    def test_lww_ignores_stale_write(self):
        node = ReplicaNode("A")
        node._apply_if_newer("shared", "newest", 7, "Z")
        accepted = node._apply_if_newer("shared", "older", 6, "Y")
        self.assertFalse(accepted)
        self.assertEqual(node.store["shared"][0], "newest")

    def test_tie_break_uses_origin_name(self):
        node = ReplicaNode("A")
        node._apply_if_newer("shared", "beta", 5, "B")
        accepted = node._apply_if_newer("shared", "alpha", 5, "Z")
        self.assertTrue(accepted)
        self.assertEqual(node.store["shared"][0], "alpha")


if __name__ == "__main__":
    unittest.main()
