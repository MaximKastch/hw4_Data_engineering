"""Print the durable experiment state directly from ZooKeeper."""

import argparse
import json
import os
import re

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


def read_node(zk, path):
    try:
        data, stat = zk.get(path)
    except NoNodeError:
        return None
    return {"data": json.loads(data), "version": stat.version,
            "ephemeral_owner": stat.ephemeralOwner}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run):
        parser.error("--run must contain only letters, digits, _ or -")
    root = "/lab4/" + args.run
    hosts = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
    zk = KazooClient(hosts=hosts, timeout=10.0)
    try:
        zk.start(timeout=20)
        zk.sync(root)
        result = {"run": args.run, "root": root}
        for name in ("epoch", "leader", "checkpoint", "result"):
            result[name] = read_node(zk, root + "/" + name)
        result["publications"] = [
            {"path": root + "/publications/" + name,
             **read_node(zk, root + "/publications/" + name)}
            for name in sorted(zk.get_children(root + "/publications"))
        ]
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
