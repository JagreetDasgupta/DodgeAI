"""
profiler.py — Loads and profiles JSONL files per entity.

Computes column types, null counts, unique counts, full-row duplicates,
and candidate primary-key columns.
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from file_scanner import EntityManifest


def load_jsonl(filepath: str) -> list[dict]:
    """Load all JSON records from a single JSONL file."""
    records: list[dict] = []
    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_entity_records(manifest: EntityManifest) -> list[dict]:
    """Load and concatenate records from all part-files of an entity."""
    all_records: list[dict] = []
    for fi in manifest.files:
        all_records.extend(load_jsonl(fi.full_path))
    return all_records


# ------------------------------------------------------------------
# Column profiling
# ------------------------------------------------------------------

def _infer_dominant_type(values: list[Any], sample_size: int = 500) -> str:
    """Return the most common Python type name among non-null values."""
    type_counts: dict[str, int] = defaultdict(int)
    for v in values[:sample_size]:
        type_counts[type(v).__name__] += 1
    return max(type_counts, key=type_counts.get) if type_counts else "unknown"


def profile_column(key: str, records: list[dict]) -> dict:
    """Profile a single column across all records."""
    values = [rec.get(key) for rec in records]
    non_null = [v for v in values if v is not None and v != "" and v != []]
    null_count = len(values) - len(non_null)
    unique_count = len({str(v) for v in non_null})

    # Sample values (up to 3 unique)
    samples: list[str] = []
    seen: set[str] = set()
    for v in non_null:
        vs = str(v)[:120]
        if vs not in seen:
            samples.append(vs)
            seen.add(vs)
        if len(samples) >= 3:
            break

    return {
        "column": key,
        "inferred_type": _infer_dominant_type(non_null),
        "null_count": null_count,
        "null_pct": round(null_count / len(values) * 100, 1) if values else 0,
        "unique_count": unique_count,
        "unique_pct": round(unique_count / len(non_null) * 100, 1) if non_null else 0,
        "sample_values": samples,
    }


# ------------------------------------------------------------------
# Entity-level profiling
# ------------------------------------------------------------------

def profile_entity(entity_name: str, records: list[dict]) -> dict:
    """
    Build a complete profile for an entity: columns, duplicates,
    candidate PKs, etc.
    """
    if not records:
        return {
            "entity": entity_name,
            "total_records": 0,
            "columns": [],
            "note": "NO DATA",
        }

    # Collect all keys
    all_keys: set[str] = set()
    for rec in records:
        all_keys.update(rec.keys())
    all_keys_sorted = sorted(all_keys)

    col_profiles = [profile_column(k, records) for k in all_keys_sorted]

    # Duplicate detection (full-row JSON equality)
    row_strs = [json.dumps(r, sort_keys=True) for r in records]
    dup_count = len(row_strs) - len(set(row_strs))

    # Candidate PK columns: >95 % unique, zero nulls
    possible_keys = [
        cp["column"] for cp in col_profiles
        if cp["unique_pct"] > 95 and cp["null_count"] == 0
    ]

    return {
        "entity": entity_name,
        "total_records": len(records),
        "duplicate_rows": dup_count,
        "column_count": len(all_keys_sorted),
        "columns": col_profiles,
        "possible_key_columns": possible_keys,
    }


def save_profiles(profiles: list[dict], output_path: Path) -> None:
    """Persist entity profiles as JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(profiles, fh, indent=2, default=str)
