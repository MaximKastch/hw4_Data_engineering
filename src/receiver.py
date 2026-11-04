"""The publication recipient, including its atomic fencing boundary.

ZooKeeper stores the small publication pointer in this lab. For an external DB,
the same token check and write must instead be atomic inside that DB.
"""

import json

from kazoo.client import KazooClient
from kazoo.exceptions import RolledBackError


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def checked_commit(transaction):
    """Kazoo multi() returns exceptions in a list; expose the actual failure."""
    results = transaction.commit()
    for item in results:
        if isinstance(item, Exception) and not isinstance(item, RolledBackError):
            raise item
    for item in results:
        if isinstance(item, Exception):
            raise item
    return results


def publish(hosts, root, pointer, epoch_version, fencing):
    # A recipient has its own connection. An expired worker session alone must
    # not be mistaken for protection against a stale request to another service.
    receiver = KazooClient(hosts=hosts, timeout=10.0)
    try:
        receiver.start(timeout=20)
        transaction = receiver.transaction()
        if fencing:
            transaction.check(root + "/epoch", version=epoch_version)
        transaction.set_data(root + "/result", encode(pointer), version=-1)
        transaction.create(root + "/publications/result-", encode(pointer), sequence=True)
        results = checked_commit(transaction)
        return results[1 if fencing else 0].version
    finally:
        receiver.stop()
        receiver.close()
