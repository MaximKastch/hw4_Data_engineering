"""The skill-pair calculation from homework 3, without its S3 transport."""

import csv
import gzip
import hashlib
import json
import re
from collections import Counter
from itertools import combinations
from pathlib import Path


def selected(row, config):
    """Keep the exact selection rule used in homework 3."""
    role_filter = config.get("role_filter")
    if role_filter is not None and row.get("role") != role_filter:
        return False
    return re.search(config["pattern"], (row.get("name") or "").lower()) is not None


def skill_set(value):
    """Normalize, deduplicate and order skills exactly as in homework 3."""
    return tuple(sorted({
        re.sub(r"[ \t\r\n]+", " ", skill).strip(" \t\r\n").lower()
        for skill in (value or "").split(";")
        if skill.strip(" \t\r\n")
    }))


def load_rows(path):
    """Read the already selected input projection, preserving source order."""
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    csv.field_size_limit(2147483647)
    with opener(path, "rt", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def count_chunk(rows):
    counts = Counter()
    with_skills = 0
    for row in rows:
        skills = skill_set(row.get("key_skills", ""))
        with_skills += bool(skills)
        counts.update(combinations(skills, 2))
    return counts, with_skills


def write_counts(path, counts):
    """Use homework 3's CSV serialization, so file hashes are comparable."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["skills", "frequency"])
        for skills, frequency in sorted(counts.items()):
            writer.writerow([json.dumps(list(skills), ensure_ascii=False), frequency])


def counts_digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
