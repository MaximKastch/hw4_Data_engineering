"""Exercise the real three-node ensemble and retain unedited CLI output."""

import json
import re
import shlex
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "evidence/ensemble"
DC = ["docker", "compose"]
NODES = ["zk1", "zk2", "zk3"]


def run(name, args, stdin=None, timeout=45, check=True):
    command = DC + args
    started = time.monotonic()
    result = subprocess.run(command, cwd=ROOT, input=stdin, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                            errors="replace", timeout=timeout)
    elapsed = time.monotonic() - started
    preamble = "$ " + shlex.join(command) + "\n"
    if stdin:
        preamble += "--- stdin ---\n" + stdin + "--- output ---\n"
    (OUT / (name + ".txt")).write_text(
        preamble + result.stdout + f"\n[exit={result.returncode}; elapsed_s={elapsed:.3f}]\n",
        encoding="utf-8")
    print(f"{name}: exit={result.returncode}, elapsed={elapsed:.2f}s", flush=True)
    if check and result.returncode:
        raise RuntimeError(f"Command failed: {name}; see evidence/ensemble/{name}.txt")
    return result.stdout, result.returncode


def cli_args(node, wait=True):
    result = ["exec", "-T", node, "timeout", "20s", "zkCli.sh"]
    if wait:
        result.append("-waitforconnection")
    return result + ["-timeout", "10000", "-server", node + ":2181"]


def cli(name, node, commands, check=True, wait=True):
    return run(name, cli_args(node, wait), "\n".join(commands + ["quit", ""]), check=check)


def roles(prefix, nodes):
    found = {}
    for node in nodes:
        output, _ = run(prefix + "-" + node, ["exec", "-T", node, "zkServer.sh", "status"])
        match = re.search(r"Mode: (leader|follower)", output)
        if not match:
            raise AssertionError(f"No serving role for {node}: {output}")
        found[node] = match.group(1)
    return found


def ephemeral(path):
    command = DC + ["exec", "-T", "zk1", "timeout", "50s", "zkCli.sh",
                    "-waitforconnection", "-timeout", "10000", "-server", "zk1:2181"]
    owner_log = OUT / "ephemeral-owner.txt"
    with owner_log.open("w", encoding="utf-8") as log:
        log.write("$ " + shlex.join(command) + "\n")
        log.write(f"--- stdin: create -e {path} alive; later quit ---\n")
        log.flush()
        owner = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=log,
                                 stderr=subprocess.STDOUT, text=True, encoding="utf-8")
        try:
            owner.stdin.write(f"create -e {path} alive\n")
            owner.stdin.flush()
            deadline = time.monotonic() + 20
            while "Created " + path not in owner_log.read_text(encoding="utf-8"):
                if owner.poll() is not None or time.monotonic() > deadline:
                    raise AssertionError("Ephemeral owner did not create its node")
                time.sleep(0.2)
            before, _ = cli("ephemeral-observer-before", "zk2", ["get -s " + path])
            match = re.search(r"ephemeralOwner = (0x[0-9a-fA-F]+)", before)
            if not match or int(match.group(1), 16) == 0 or "alive\n" not in before:
                raise AssertionError("Observer did not see an owned ephemeral node")
            owner.stdin.write("quit\n")
            owner.stdin.flush()
            owner.wait(timeout=15)
            if owner.returncode:
                raise AssertionError("Owner did not close its session successfully")
        finally:
            if owner.poll() is None:
                try:
                    owner.stdin.write("quit\n")
                    owner.stdin.flush()
                    owner.wait(timeout=15)
                except (OSError, subprocess.TimeoutExpired):
                    owner.kill()
                    owner.wait(timeout=5)
            owner.stdin.close()
        log.write(f"\n[exit={owner.returncode}]\n")
    after, _ = cli("ephemeral-observer-after", "zk2", ["get -s " + path], check=False)
    if "Node does not exist: " + path not in after:
        raise AssertionError("Ephemeral node survived session close")
    return {"path": path, "ephemeral_owner": match.group(1), "visible_while_owner_alive": True,
            "owner_closed_with": "zkCli quit (graceful session close)", "absent_after_close": True}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    stamp = uuid.uuid4().hex[:10]
    probe, rejected = "/lab4-quorum-" + stamp, "/lab4-no-quorum-" + stamp
    summary = {"started_utc": datetime.now(timezone.utc).isoformat(), "probe_path": probe}
    stopped = []
    try:
        run("initial-compose-ps", ["ps"])
        run("java-version", ["exec", "-T", "zk1", "java", "-version"])
        version, _ = cli("zookeeper-version", "zk1", ["version"])
        if "3.9.6" not in version:
            raise AssertionError("Expected ZooKeeper 3.9.6")
        for node in NODES:
            run(node + "-config", ["exec", "-T", node, "cat", "/conf/zoo.cfg"])
            run(node + "-myid", ["exec", "-T", node, "cat", "/data/myid"])
        summary["initial_roles"] = roles("initial-status", NODES)
        if list(summary["initial_roles"].values()).count("leader") != 1:
            raise AssertionError("Expected exactly one leader")
        leader = next(node for node, role in summary["initial_roles"].items() if role == "leader")
        follower = next(node for node, role in summary["initial_roles"].items() if role == "follower")
        stopped.append(follower)
        run("stop-first-follower", ["stop", follower])
        survivors = [node for node in NODES if node != follower]
        summary["one_down_roles"] = roles("one-down-status", survivors)
        output, _ = cli("one-down-write-read", survivors[0], [f"create {probe} quorum-two",
                          f"set {probe} updated-with-two", "get -s " + probe])
        if "Created " + probe not in output or "updated-with-two\n" not in output:
            raise AssertionError("Two-node quorum failed actual write/read")
        summary["one_down"] = {"stopped": follower, "create_set_get_succeeded": True}
        stopped.append(leader)
        run("stop-second-leader", ["stop", leader])
        survivor = next(node for node in NODES if node not in stopped)
        time.sleep(3)
        run("two-down-compose-ps", ["ps", "--all"])
        status, status_code = run("two-down-status", ["exec", "-T", survivor, "zkServer.sh", "status"], check=False)
        if status_code == 0 or re.search(r"Mode: (leader|follower)", status):
            raise AssertionError("Single node unexpectedly serves requests")
        output, code = run("two-down-failed-write", cli_args(survivor, wait=False) +
                           ["create", rejected, "forbidden-without-quorum"], check=False)
        if "Created " + rejected in output or code == 0:
            raise AssertionError("Write without quorum unexpectedly succeeded")
        summary["two_down"] = {"stopped": stopped.copy(), "survivor": survivor,
                               "status_exit": status_code, "write_exit": code,
                               "write_succeeded": False, "rejected_path": rejected}
    finally:
        if stopped:
            run("restart-stopped", ["up", "-d", "--wait", "--wait-timeout", "90"] + stopped, timeout=110)
    summary["restored_roles"] = roles("restored-status", NODES)
    output, _ = cli("restored-read", "zk1", ["get -s " + probe, "get -s " + rejected], check=False)
    if "updated-with-two\n" not in output or "Node does not exist: " + rejected not in output:
        raise AssertionError("Recovery did not preserve the committed write / rejected absence")
    summary["recovery"] = {"committed_value_preserved": True, "rejected_write_absent": True}
    summary["ephemeral"] = ephemeral("/lab4-ephemeral-" + stamp)
    cli("cleanup-probe", "zk1", ["delete " + probe])
    run("final-compose-ps", ["ps"])
    summary["finished_utc"] = datetime.now(timezone.utc).isoformat()
    summary["all_checks_passed"] = True
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
