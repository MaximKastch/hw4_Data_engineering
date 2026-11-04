"""Integration tests against a real ensemble, in isolated temporary znodes.

Run: docker compose run --rm app python -m unittest discover -s tests -v
"""

import json
import os
import unittest
import uuid

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError

from src.coordinator import initialize
from src.receiver import checked_commit, encode, publish


class FencingIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.hosts = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
        self.zk = KazooClient(hosts=self.hosts, timeout=10.0)
        self.zk.start(timeout=20)
        self.root = "/lab4/test-" + uuid.uuid4().hex
        initialize(self.zk, self.root)

    def tearDown(self):
        try:
            self.zk.delete(self.root, recursive=True)
        finally:
            self.zk.stop()
            self.zk.close()

    def assert_publication_state(self, expected_data, expected_version, count):
        # Publication uses another session; synchronize this client's reads
        # with the leader before checking the committed result on a follower.
        self.zk.sync(self.root + "/result")
        data, stat = self.zk.get(self.root + "/result")
        self.assertEqual(json.loads(data), expected_data)
        self.assertEqual(stat.version, expected_version)
        self.assertEqual(len(self.zk.get_children(self.root + "/publications")), count)

    def test_fences_old_epoch_before_and_after_new_publication(self):
        current = self.zk.set(self.root + "/epoch", b"1", version=0).version
        old, new = {"coordinator": "old", "epoch": 0}, {"coordinator": "new", "epoch": 1}

        # No successor publication exists yet: guarding only the result's
        # version would miss precisely this stale-write window.
        with self.assertRaises(BadVersionError):
            publish(self.hosts, self.root, old, epoch_version=0, fencing=True)
        self.assert_publication_state({}, expected_version=0, count=0)

        version = publish(self.hosts, self.root, new, epoch_version=current, fencing=True)
        self.assertEqual(version, 1)
        self.assert_publication_state(new, expected_version=1, count=1)

        with self.assertRaises(BadVersionError):
            publish(self.hosts, self.root, old, epoch_version=0, fencing=True)
        self.assert_publication_state(new, expected_version=1, count=1)

    def test_stale_checkpoint_cannot_roll_back_progress(self):
        current = self.zk.set(self.root + "/epoch", b"1", version=0).version
        checkpoint = {"next_index": 1000, "coordinator": "new"}
        transaction = self.zk.transaction()
        transaction.check(self.root + "/epoch", version=current)
        transaction.set_data(self.root + "/checkpoint", encode(checkpoint))
        checked_commit(transaction)

        stale = self.zk.transaction()
        stale.check(self.root + "/epoch", version=0)
        stale.set_data(self.root + "/checkpoint", encode({"next_index": 200}))
        with self.assertRaises(BadVersionError):
            checked_commit(stale)
        data, stat = self.zk.get(self.root + "/checkpoint")
        self.assertEqual(json.loads(data), checkpoint)
        self.assertEqual(stat.version, 1)

    def test_without_fencing_old_epoch_overwrites_result(self):
        current = self.zk.set(self.root + "/epoch", b"1", version=0).version
        new, old = {"coordinator": "new", "epoch": 1}, {"coordinator": "old", "epoch": 0}
        self.assertEqual(publish(self.hosts, self.root, new, current, fencing=False), 1)
        self.assertEqual(publish(self.hosts, self.root, old, 0, fencing=False), 2)
        self.assert_publication_state(old, expected_version=2, count=2)


if __name__ == "__main__":
    unittest.main()
