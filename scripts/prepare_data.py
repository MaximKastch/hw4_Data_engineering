"""Build the small, lossless input projection from the original homework 3."""

import argparse
import csv
import gzip
import io
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.calculation import count_chunk, counts_digest, load_rows, selected, write_counts


def prepare(lab3):
    lab3 = Path(lab3).resolve()
    data_dir = ROOT / "data"
    baseline_dir = ROOT / "baseline"
    data_dir.mkdir(exist_ok=True)
    baseline_dir.mkdir(exist_ok=True)
    config = json.loads((lab3 / "config.json").read_text(encoding="utf-8-sig"))
    original_manifest = json.loads(
        (lab3 / "evidence/dataset_manifest.json").read_text(encoding="utf-8-sig")
    )
    original_metrics = json.loads(
        (lab3 / "results/runs/mr_combined_1/metrics.json").read_text(encoding="utf-8-sig")
    )
    source = lab3 / "it_vacancies_2024.csv"
    source_hash = counts_digest(source)
    if source_hash != original_manifest["sha256_local"]:
        raise AssertionError("The source differs from the recorded homework 3 dataset")

    fields = ["name", "role", "key_skills"]
    destination = data_dir / "selected_vacancies.csv.gz"
    source_rows = selected_rows = 0
    csv.field_size_limit(2147483647)
    # Empty filename and mtime=0 remove timestamp/path metadata from the gzip header.
    with source.open(encoding="utf-8-sig", newline="") as source_stream:
        with destination.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as output:
                    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
                    writer.writeheader()
                    for row in csv.DictReader(source_stream):
                        source_rows += 1
                        if selected(row, config):
                            writer.writerow({field: row.get(field, "") for field in fields})
                            selected_rows += 1

    for source_path, name in [
        (lab3 / "config.json", "config.json"),
        (lab3 / "evidence/dataset_manifest.json", "dataset_manifest.json"),
        (lab3 / "results/runs/mr_combined_1/metrics.json", "metrics.json"),
        (lab3 / "results/mr_combined/counts_2.csv", "counts_2.csv"),
    ]:
        shutil.copyfile(source_path, baseline_dir / name)

    counts, with_skills = count_chunk(load_rows(destination))
    verification_path = data_dir / "preparation_check.csv"
    write_counts(verification_path, counts)
    calculated_hash = counts_digest(verification_path)
    baseline_hash = counts_digest(baseline_dir / "counts_2.csv")
    checks = {
        "source_rows_equal": source_rows == original_metrics["source_rows"],
        "selected_rows_equal": selected_rows == original_metrics["selected_rows"],
        "with_skills_equal": with_skills == original_metrics["vacancies_with_skills"],
        "unique_pairs_equal": len(counts) == original_metrics["unique"]["2"],
        "pair_frequency_sum_equal": sum(counts.values()) == original_metrics["emitted"]["2"],
        "counts_csv_byte_equal": verification_path.read_bytes() == (baseline_dir / "counts_2.csv").read_bytes(),
    }
    if not all(checks.values()):
        raise AssertionError(f"Projection verification failed: {checks}")
    verification_path.unlink()

    manifest = {
        "source_repository": "https://github.com/MaximKastch/hw3_Data_engineering",
        "source_commit": "3a0101b690b04ac8bf5962b674afb178de91a401",
        "source_file": source.name,
        "source_bytes": source.stat().st_size,
        "source_sha256": source_hash,
        "source_rows": source_rows,
        "selection": {"role_filter": config["role_filter"], "name_regex_lowercase": config["pattern"]},
        "projection_fields": fields,
        "projection": "data/selected_vacancies.csv.gz",
        "projection_bytes": destination.stat().st_size,
        "projection_sha256": counts_digest(destination),
        "projection_preserves_source_order": True,
        "selected_rows": selected_rows,
        "vacancies_with_skills": with_skills,
        "unique_pairs": len(counts),
        "pair_frequency_sum": sum(counts.values()),
        "counts_csv_sha256": calculated_hash,
        "baseline_counts_csv_sha256": baseline_hash,
        "checks": checks,
        "notes": "Input projection contains every selected vacancy, including empty skills; it is not a sample. Only irrelevant columns and rows rejected by the original filter are omitted. Pair counts are recomputed from input skills, never loaded from the baseline.",
    }
    (data_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab3", type=Path, default=ROOT.parent / "lab3")
    prepare(parser.parse_args().lab3)
