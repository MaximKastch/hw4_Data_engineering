"""Reproduce the complete lab; requires Docker Compose and Python 3.10+."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose"]


def command(args, transcript, *, timeout=120, check=True, input_text=None):
    started = datetime.now(timezone.utc).isoformat()
    result = subprocess.run(args, cwd=ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, encoding="utf-8",
                            errors="replace", timeout=timeout, input=input_text)
    with transcript.open("a", encoding="utf-8") as stream:
        stream.write(f"\n[{started}] $ {subprocess.list2cmdline(args)}\n")
        if input_text:
            stream.write("stdin:\n" + input_text + "\n")
        stream.write(result.stdout + f"\nexit_code={result.returncode}\n")
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}); see {transcript}")
    return result.stdout


def records(run, identity):
    path = ROOT / "logs" / run / f"{identity}.jsonl"
    if not path.exists():
        return []
    # The writer may be between write() and newline when we inspect its log.
    lines = path.read_text(encoding="utf-8").splitlines()
    output = []
    for line in lines:
        try:
            output.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return output


def wait_event(run, identity, event, timeout=60, predicate=lambda row: True):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = records(run, identity)
        failure = next((row for row in rows if row["event"] == "failed"), None)
        if failure:
            raise RuntimeError(f"Coordinator failed: {failure}")
        match = next((row for row in rows if row["event"] == event and predicate(row)), None)
        if match:
            return match
        time.sleep(0.1)
    raise TimeoutError(f"No {event} in {run}/{identity} after {timeout}s")


def experiment(run, fencing, pause_seconds=40):
    directory = ROOT / "evidence" / run
    if directory.exists() or (ROOT / "logs" / run).exists():
        raise FileExistsError(f"Run {run} already exists; choose a fresh --prefix")
    directory.mkdir(parents=True)
    transcript = directory / "commands.txt"
    names = {identity: f"de-lab4-{run}-{identity.lower()}" for identity in ("A", "B")}
    paused = False
    created = []
    try:
        for identity in ("A", "B"):
            command(COMPOSE + ["run", "-d", "--name", names[identity],
                    "-e", f"FENCING={fencing}", "app", "python", "-m",
                    "src.coordinator", "--run", run, "--id", identity], transcript)
            created.append(names[identity])
            if identity == "A":
                wait_event(run, "A", "became_leader")
        checkpoint = wait_event(run, "A", "checkpoint", predicate=lambda row: row["next_index"] >= 600)
        # Host and Linux VM clocks may differ. Get the pause request timestamp
        # from the same VM's CLOCK_MONOTONIC as both coordinators, before freeze.
        clock_text = command(["docker", "exec", names["A"], "python", "-c",
                              "import time,json;print(json.dumps({'time_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns()}))"], transcript)
        pause_clock = json.loads(clock_text)
        pause_requested = time.monotonic_ns()
        command(["docker", "pause", names["A"]], transcript)
        paused = True
        pause_confirmed = time.monotonic_ns()
        a_events = records(run, "A")
        checkpoint = [row for row in a_events if row["event"] == "checkpoint"][-1]
        if checkpoint["next_index"] >= checkpoint["total_rows"] or any(row["event"] == "calculation_complete" for row in a_events):
            raise AssertionError("Pause arrived after computation; increase coordinator --delay")
        print(f"{run}: A paused; waiting for B election and checkpoint recovery", flush=True)
        leader = wait_event(run, "B", "became_leader", timeout=35)
        restored = wait_event(run, "B", "restored")
        restored_path = ROOT / restored["checkpoint"]["path"]
        (directory / "restored-checkpoint.json").write_bytes(restored_path.read_bytes())
        wait_event(run, "B", "published", timeout=40)
        # Keep A frozen for approximately 40 seconds, including B's work.
        remaining = pause_seconds - (time.monotonic_ns() - pause_confirmed) / 1e9
        if remaining > 0:
            time.sleep(remaining)
        resume_requested = time.monotonic_ns()
        command(["docker", "unpause", names["A"]], transcript)
        paused = False
        wait_event(run, "A", "publish_rejected" if fencing else "published", timeout=45)
        for identity, name in names.items():
            exit_code = command(["docker", "wait", name], transcript, timeout=30).strip()
            if exit_code != "0":
                raise AssertionError(f"{name} exited with {exit_code}")
            stdout = command(["docker", "logs", name], transcript)
            (directory / f"{identity}-console.txt").write_text(stdout, encoding="utf-8")
        state_text = command(COMPOSE + ["run", "--rm", "-T", "app", "python",
                              "scripts/zk_inspect.py", "--run", run], transcript)
        # Compose may print a container lifecycle line before JSON.
        state = json.JSONDecoder().raw_decode(state_text[state_text.index("{"):])[0]
        (directory / "zookeeper-state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        cli = command(COMPOSE + ["exec", "-T", "zk1", "zkCli.sh", "-server", "zk1:2181",
                      "-waitforconnection", "-timeout", "10000"], transcript,
                      input_text=f"get -s /lab4/{run}/result\nquit\n")
        (directory / "result-zkcli.txt").write_text(cli, encoding="utf-8")
        events = records(run, "A") + records(run, "B")
        published = [row for row in events if row["event"] == "published"]
        rejected = [row for row in events if row["event"] == "publish_rejected"]
        completed = [row for row in events if row["event"] == "calculation_complete"]
        # Inspect schema is intentionally simple; preserve the raw snapshot.
        result = state["result"]
        checks = {
            "both_computations_equal_lab3": len(completed) == 2 and all(row["matches_lab3"] for row in completed),
            "backup_restored_nonzero_checkpoint": 0 < restored["next_index"] < restored["total_rows"],
            "publications_expected": len(published) == (1 if fencing else 2),
            "stale_rejected_only_with_fencing": len(rejected) == fencing and all(row["error"] == "BadVersionError" for row in rejected),
            "pointer_version_expected": result["version"] == (1 if fencing else 2),
            "pointer_owner_expected": result["data"]["coordinator"] == ("B" if fencing else "A"),
            "audit_count_expected": len(state["publications"]) == (1 if fencing else 2),
            "session_timeout_requested_10s": all(row["requested_session_timeout_ms"] == 10000 for row in events if row["event"] == "started"),
            "backup_epoch_newer": leader["epoch"] > next(row["epoch"] for row in records(run, "A") if row["event"] == "became_leader"),
        }
        docker_events = command(["docker", "events", "--since", str(pause_clock["time_ns"] // 1_000_000_000 - 1),
                                 "--until", str(pause_clock["time_ns"] // 1_000_000_000 + 10),
                                 "--filter", f"container={names['A']}", "--filter", "event=pause",
                                 "--format", "{{json .}}"], transcript)
        pause_events = [json.loads(line) for line in docker_events.splitlines() if line.startswith("{")]
        if len(pause_events) != 1:
            raise AssertionError(f"Expected one Docker pause event, got {pause_events}")
        paused_at_ns = pause_events[0]["timeNano"]
        summary = dict(run=run, fencing=bool(fencing), pause_requested_clock=pause_clock,
                       docker_pause_time_ns=paused_at_ns,
                       backup_became_leader_time_ns=leader["time_ns"],
                       failover_seconds=(leader["time_ns"] - paused_at_ns) / 1e9,
                       failover_from_precommand_monotonic_seconds=(leader["monotonic_ns"] - pause_clock["monotonic_ns"]) / 1e9,
                       pause_command_seconds=(pause_confirmed - pause_requested) / 1e9,
                       pause_duration_seconds=(resume_requested - pause_confirmed) / 1e9,
                       measurement="Docker daemon pause event timeNano to B became_leader time_ns; both clocks in Linux VM",
                       checkpoint_observed_before_pause=checkpoint["next_index"],
                       restored_index=restored["next_index"], published_count=len(published),
                       rejected_count=len(rejected), result_version=result["version"],
                       result_owner=result["data"]["coordinator"], checks=checks)
        (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if not all(checks.values()):
            raise AssertionError(f"Checks failed: {checks}")
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return summary
    finally:
        if paused:
            command(["docker", "unpause", names["A"]], transcript, check=False)
        for name in created:
            command(["docker", "rm", "-f", name], transcript, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default=datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ"))
    parser.add_argument("--skip-ensemble", action="store_true")
    args = parser.parse_args()
    import re
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.prefix):
        parser.error("--prefix must contain letters, digits, - or _")
    os.chdir(ROOT)
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    setup = evidence / f"{args.prefix}-setup.txt"
    command(COMPOSE + ["up", "-d", "--wait", "zk1", "zk2", "zk3"], setup)
    command(COMPOSE + ["build", "app"], setup, timeout=600)
    if not args.skip_ensemble:
        command([sys.executable, "scripts/check_ensemble.py"], setup, timeout=240)
    command(COMPOSE + ["run", "--rm", "-T", "app", "python", "-m", "unittest", "discover", "-s", "tests", "-v"], setup)
    summaries = [experiment(args.prefix + "-unfenced", 0), experiment(args.prefix + "-fenced", 1)]
    (evidence / f"{args.prefix}-summary.json").write_text(json.dumps(summaries, indent=2) + "\n", encoding="utf-8")
    for node in ("zk1", "zk2", "zk3"):
        output = command(COMPOSE + ["logs", "--no-color", node], setup)
        (evidence / f"{args.prefix}-{node}.log").write_text(output, encoding="utf-8")
    print("All checks passed. Ensemble remains running; stop it with docker compose down.", flush=True)


if __name__ == "__main__":
    main()
