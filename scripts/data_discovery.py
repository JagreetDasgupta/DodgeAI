"""
Phase 1 - Step 1: Data Discovery
Scans the SAP O2C dataset, profiles each file, and outputs a comprehensive summary.
"""

import os
import json
import sys
from pathlib import Path
from collections import defaultdict

DATA_ROOT = Path(r"c:\Users\jagre\OneDrive\Desktop\Dodge AI\sap-order-to-cash-dataset\sap-o2c-data")


def scan_files(root: Path) -> list[dict]:
    """Recursively scan for all data files and gather metadata."""
    results = []
    for dirpath, dirnames, filenames in os.walk(root):
        for fname in sorted(filenames):
            fpath = Path(dirpath) / fname
            results.append({
                "directory": Path(dirpath).name,
                "filename": fname,
                "full_path": str(fpath),
                "extension": fpath.suffix,
                "size_bytes": fpath.stat().st_size,
            })
    return results


def load_jsonl(filepath: str) -> list[dict]:
    """Load all records from a JSONL file."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def profile_entity(entity_name: str, files: list[dict]) -> dict:
    """Profile all files belonging to a single entity directory."""
    all_records = []
    file_details = []
    for finfo in files:
        records = load_jsonl(finfo["full_path"])
        file_details.append({
            "filename": finfo["filename"],
            "size_bytes": finfo["size_bytes"],
            "record_count": len(records),
        })
        all_records.extend(records)

    if not all_records:
        return {
            "entity": entity_name,
            "file_count": len(files),
            "files": file_details,
            "total_records": 0,
            "columns": [],
            "note": "NO DATA FOUND",
        }

    # Gather all column names across all records
    all_keys = set()
    for rec in all_records:
        all_keys.update(rec.keys())
    all_keys = sorted(all_keys)

    # Infer types, count nulls, detect sample values
    column_profiles = []
    for key in all_keys:
        values = [rec.get(key) for rec in all_records]
        non_null_values = [v for v in values if v is not None and v != "" and v != []]
        null_count = len(values) - len(non_null_values)

        # Infer type from non-null values
        type_counts = defaultdict(int)
        for v in non_null_values[:500]:  # sample first 500
            type_counts[type(v).__name__] += 1

        dominant_type = max(type_counts, key=type_counts.get) if type_counts else "unknown"

        # Sample values (first 3 unique non-null)
        sample_vals = []
        seen = set()
        for v in non_null_values:
            vstr = str(v)[:100]
            if vstr not in seen:
                sample_vals.append(vstr)
                seen.add(vstr)
            if len(sample_vals) >= 3:
                break

        # Unique count (for key detection)
        unique_count = len(set(str(v) for v in non_null_values))

        column_profiles.append({
            "column": key,
            "inferred_type": dominant_type,
            "null_count": null_count,
            "null_pct": round(null_count / len(values) * 100, 1) if values else 0,
            "unique_count": unique_count,
            "unique_pct": round(unique_count / len(non_null_values) * 100, 1) if non_null_values else 0,
            "sample_values": sample_vals,
        })

    # Detect duplicates (full row)
    row_strings = [json.dumps(rec, sort_keys=True) for rec in all_records]
    duplicate_count = len(row_strings) - len(set(row_strings))

    # Identify possible key columns (high uniqueness)
    possible_keys = [
        cp["column"] for cp in column_profiles
        if cp["unique_pct"] > 95 and cp["null_count"] == 0
    ]

    return {
        "entity": entity_name,
        "file_count": len(files),
        "files": file_details,
        "total_records": len(all_records),
        "duplicate_rows": duplicate_count,
        "column_count": len(all_keys),
        "columns": column_profiles,
        "possible_key_columns": possible_keys,
    }


def main():
    print("=" * 80)
    print("PHASE 1 — STEP 1: DATA DISCOVERY")
    print("=" * 80)
    print(f"\nScanning: {DATA_ROOT}\n")

    # Step 1: Scan all files
    all_files = scan_files(DATA_ROOT)
    print(f"Found {len(all_files)} files total.\n")

    # Group by directory (entity)
    entities = defaultdict(list)
    for f in all_files:
        entities[f["directory"]].append(f)

    print(f"Detected {len(entities)} entity directories:\n")
    for ename in sorted(entities.keys()):
        files = entities[ename]
        total_size = sum(f["size_bytes"] for f in files)
        print(f"  {ename}: {len(files)} file(s), {total_size:,} bytes")

    # Step 2: Profile each entity
    print("\n" + "=" * 80)
    print("ENTITY PROFILES")
    print("=" * 80)

    all_profiles = []
    for ename in sorted(entities.keys()):
        print(f"\n{'─' * 70}")
        print(f"Entity: {ename}")
        print(f"{'─' * 70}")

        profile = profile_entity(ename, entities[ename])
        all_profiles.append(profile)

        print(f"  Files: {profile['file_count']}")
        print(f"  Total records: {profile['total_records']}")
        if 'duplicate_rows' in profile:
            print(f"  Duplicate rows: {profile['duplicate_rows']}")
        print(f"  Columns: {profile.get('column_count', 0)}")

        if profile.get('possible_key_columns'):
            print(f"  Possible key columns: {', '.join(profile['possible_key_columns'])}")

        print(f"\n  Column Details:")
        for cp in profile['columns']:
            print(f"    {cp['column']}")
            print(f"      type: {cp['inferred_type']}, nulls: {cp['null_count']} ({cp['null_pct']}%), "
                  f"unique: {cp['unique_count']} ({cp['unique_pct']}%)")
            print(f"      samples: {cp['sample_values'][:3]}")

    # Save profiles as JSON for next step
    output_path = Path(r"c:\Users\jagre\OneDrive\Desktop\Dodge AI\output\discovery_profiles.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_profiles, f, indent=2, default=str)
    print(f"\n\nProfiles saved to: {output_path}")


if __name__ == "__main__":
    main()
