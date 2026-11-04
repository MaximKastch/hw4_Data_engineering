"""One coordinator process; run A and B against the same --run namespace.

An already-started computation intentionally continues after session expiration
to reproduce stale work (for example, a worker returning after a long pause).
Its epoch is NEVER refreshed. Checkpoints are always fenced; FENCING controls
only final publication, making the two demonstrations directly comparable.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import (
    BadVersionError, ConnectionLoss, NoNodeError, NodeExistsError, SessionExpiredError,
)

from src.calculation import count_chunk, counts_digest, load_rows, write_counts
from src.receiver import checked_commit, encode, publish


class Journal:
    def __init__(self, run, identity):
        self.run, self.identity = run, identity
        path = Path("logs") / run / f"{identity}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = path.open("a", encoding="utf-8", buffering=1)
        self.lock = threading.Lock()

    def __call__(self, event, **fields):
        record = dict(timestamp=datetime.now(timezone.utc).isoformat(),
                      time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                      run=self.run, coordinator=self.identity, event=event, **fields)
        line = json.dumps(record, ensure_ascii=False)
        with self.lock:
            self.stream.write(line + "\n")
            self.stream.flush()
            print(line, flush=True)


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def initialize(zk, root):
    zk.ensure_path(root + "/publications")
    for name, value in (("epoch", b"0"), ("checkpoint", b"{}"), ("result", b"{}")):
        try:
            zk.create(root + "/" + name, value)
        except NodeExistsError:
            pass


def acquire(zk, root, identity, log):
    log("waiting_for_leadership")
    while True:
        try:
            # Recover an election whose transaction committed but whose reply
            # was lost; never borrow another coordinator's token.
            try:
                data, stat = zk.get(root + "/leader")
                leader = json.loads(data)
                if (leader["coordinator"] == identity
                        and stat.ephemeralOwner == zk.client_id[0]):
                    return leader["epoch"], leader["epoch_version"]
                time.sleep(0.2)
                continue
            except NoNodeError:
                pass
            data, stat = zk.get(root + "/epoch")
            epoch, version = int(data) + 1, stat.version + 1
            leader = dict(coordinator=identity, epoch=epoch, epoch_version=version)
            transaction = zk.transaction()
            transaction.create(root + "/leader", encode(leader), ephemeral=True)
            transaction.set_data(root + "/epoch", str(epoch).encode(), version=stat.version)
            checked_commit(transaction)
            return epoch, version
        except (NodeExistsError, BadVersionError, ConnectionLoss, SessionExpiredError):
            time.sleep(0.2)


def calculate(args, zk, root, epoch, epoch_version, log):
    source = Path("data/selected_vacancies.csv.gz")
    input_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    rows = load_rows(source)
    data, _ = zk.get(root + "/checkpoint")
    pointer = json.loads(data)
    counts, next_index, with_skills = Counter(), 0, 0
    if pointer:
        raw = Path(pointer["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != pointer["sha256"]:
            raise ValueError("Checkpoint checksum mismatch")
        saved = json.loads(raw)
        if saved["input_sha256"] != input_sha256:
            raise ValueError("Checkpoint belongs to another input")
        counts = Counter({tuple(pair): frequency for pair, frequency in saved["counts"]})
        next_index, with_skills = saved["next_index"], saved["with_skills"]
    restored_index = next_index
    log("restored", next_index=next_index, total_rows=len(rows), checkpoint=pointer)
    stale_checkpoint = False
    while next_index < len(rows):
        end = min(next_index + args.chunk_size, len(rows))
        partial, skilled = count_chunk(rows[next_index:end])
        counts.update(partial)
        with_skills += skilled
        next_index = end
        if not stale_checkpoint:
            path = Path("runtime") / args.run / "checkpoints" / f"{args.id}-{end}.json"
            saved = dict(input_sha256=input_sha256, next_index=end,
                         with_skills=with_skills, counts=list(counts.items()))
            atomic_json(path, saved)
            checkpoint = dict(path=path.as_posix(), next_index=end, coordinator=args.id,
                              epoch=epoch, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            transaction = zk.transaction()
            transaction.check(root + "/epoch", version=epoch_version)
            transaction.set_data(root + "/checkpoint", encode(checkpoint))
            try:
                checked_commit(transaction)
                log("checkpoint", next_index=end, total_rows=len(rows), path=path.as_posix())
            except (BadVersionError, SessionExpiredError, ConnectionLoss) as error:
                # A stale task may still produce a result, but cannot roll back
                # the shared checkpoint. Final publication tests the recipient.
                stale_checkpoint = True
                log("checkpoint_rejected", next_index=end, error=type(error).__name__)
        time.sleep(args.delay)  # Reproducible time window for docker pause.

    output = Path("results") / args.run / args.id
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / "counts_2.csv"
    write_counts(result_path, counts)
    digest = counts_digest(result_path)
    baseline_digest = counts_digest(Path("baseline/counts_2.csv"))
    expected = json.loads(Path("baseline/metrics.json").read_text(encoding="utf-8"))
    controls_match = (
        len(rows) == expected["selected_rows"]
        and with_skills == expected["vacancies_with_skills"]
        and len(counts) == expected["unique"]["2"]
        and sum(counts.values()) == expected["emitted"]["2"]
    )
    metrics = dict(selected_rows=len(rows), vacancies_with_skills=with_skills,
                   unique_pairs=len(counts), pair_frequency_sum=sum(counts.values()),
                   source_rows_before_projection=expected["source_rows"],
                   counts_sha256=digest, baseline_sha256=baseline_digest,
                   matches_lab3=digest == baseline_digest and controls_match,
                   restored_index=restored_index,
                   processed_rows=len(rows) - restored_index)
    atomic_json(output / "metrics.json", metrics)
    log("calculation_complete", **metrics)
    if not metrics["matches_lab3"]:
        raise ValueError("Calculated pairs differ from homework 3")
    return dict(coordinator=args.id, epoch=epoch, epoch_version=epoch_version,
                path=result_path.as_posix(), metrics=metrics)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--id", required=True)
    parser.add_argument("--chunk-size", type=int, default=200)
    parser.add_argument("--delay", type=float, default=0.4)
    args = parser.parse_args()
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", value) for value in (args.run, args.id)):
        parser.error("--run and --id must contain only letters, digits, _ or -")
    if args.chunk_size < 1 or args.delay < 0:
        parser.error("--chunk-size must be positive and --delay nonnegative")
    hosts = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
    fencing_value = os.environ.get("FENCING", "0")
    if fencing_value not in ("0", "1"):
        parser.error("FENCING must be 0 or 1")
    fencing = fencing_value == "1"
    root = "/lab4/" + args.run
    logging.basicConfig(level=logging.WARNING)
    log = Journal(args.run, args.id)
    zk = KazooClient(hosts=hosts, timeout=10.0)
    zk.add_listener(lambda state: log("session_state", state=state))
    try:
        zk.start(timeout=30)
        # Kazoo 2.10 stores the requested timeout here; the negotiated value is
        # local to its handshake code and is also visible in server session logs.
        log("started", fencing=fencing, requested_session_timeout_ms=10000,
            session_id=hex(zk.client_id[0]), hosts=hosts)
        initialize(zk, root)
        epoch, epoch_version = acquire(zk, root, args.id, log)
        log("became_leader", epoch=epoch, epoch_version=epoch_version)
        pointer = calculate(args, zk, root, epoch, epoch_version, log)
        try:
            version = publish(hosts, root, pointer, epoch_version, fencing)
            log("published", message="записал результат", epoch=epoch,
                result_version=version, path=pointer["path"])
        except BadVersionError as error:
            log("publish_rejected", error=type(error).__name__, epoch=epoch,
                epoch_version=epoch_version, message="запись устаревшего лидера отклонена")
            if not fencing:
                raise
    except Exception as error:
        log("failed", error=type(error).__name__, message=str(error))
        raise
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
