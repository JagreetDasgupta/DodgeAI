"""
main.py — Orchestrator for the Phase 1 data pipeline.

Runs all stages in sequence:
  1. Scan files
  2. Profile entities
  3. Transform / normalise
  4. Store (SQLite + CSV + Parquet)
  5. Validate
  6. Generate schema summary
"""

import json
import sys
import os
from pathlib import Path

# Ensure src/ is on sys.path when run from project root
sys.path.insert(0, os.path.dirname(__file__))

from src.file_scanner import scan_data_directory, print_manifest_summary
from src.profiler import load_jsonl, load_entity_records, profile_entity, save_profiles
from src.schema_inference import (
    ENTITY_SCHEMAS,
    get_schema_for_entity,
    save_schema_summary,
)
from src.transformer import transform_entity
from src.storage_writer import write_sqlite, write_csv, write_parquet
from src.validator import validate_database, print_validation_report

# ──────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "sap-order-to-cash-dataset" / "sap-o2c-data"
OUTPUT_DIR = PROJECT_ROOT / "output"
CLEAN_DIR = OUTPUT_DIR / "clean"
DB_PATH = OUTPUT_DIR / "sap_o2c.db"
PROFILES_PATH = OUTPUT_DIR / "discovery_profiles.json"
SCHEMA_PATH = OUTPUT_DIR / "schema_summary.json"


def main() -> None:
    print("=" * 72)
    print("PHASE 1 — GRAPH-BASED DATA MODELING  ·  DATA FOUNDATION PIPELINE")
    print("=" * 72)

    # ── Stage 1: Scan ─────────────────────────────────────────────
    print("\n▶ Stage 1: Scanning data directory …")
    manifests = scan_data_directory(DATA_ROOT)
    print_manifest_summary(manifests)

    # ── Stage 2: Profile ──────────────────────────────────────────
    print("\n▶ Stage 2: Profiling entities …")
    profiles = []
    entity_records_cache: dict[str, list[dict]] = {}
    for ename in sorted(manifests):
        records = load_entity_records(manifests[ename])
        entity_records_cache[ename] = records
        profile = profile_entity(ename, records)
        profiles.append(profile)
        print(f"  {ename}: {profile['total_records']} records, "
              f"{profile.get('column_count', 0)} columns, "
              f"{profile.get('duplicate_rows', 0)} duplicates")
    save_profiles(profiles, PROFILES_PATH)

    # ── Stage 3 + 4: Transform & Store ────────────────────────────
    print("\n▶ Stage 3-4: Transforming and storing …")
    entity_row_counts: dict[str, int] = {}

    for schema in ENTITY_SCHEMAS:
        src_dir = schema["source_directory"]
        table_name = schema["table_name"]

        if src_dir not in manifests:
            print(f"  ✗ {table_name}: source directory '{src_dir}' not found – skipping")
            continue

        # Build raw records with file-map for lineage
        manifest = manifests[src_dir]
        raw_records: list[dict] = []
        file_map: list[tuple[str, int, int]] = []
        for fi in manifest.files:
            chunk = load_jsonl(fi.full_path)
            start = len(raw_records)
            raw_records.extend(chunk)
            end = len(raw_records)
            file_map.append((fi.filename, start, end))

        # Transform
        cleaned = transform_entity(table_name, raw_records, file_map)

        # Store to SQLite
        inserted = write_sqlite(DB_PATH, table_name, cleaned, schema["primary_key"])
        entity_row_counts[table_name] = inserted

        # Export CSV
        csv_path = write_csv(CLEAN_DIR, table_name, cleaned)

        # Export Parquet
        pq_path = write_parquet(CLEAN_DIR, table_name, cleaned)
        pq_note = f", Parquet: {pq_path.name}" if pq_path else ""

        print(f"  ✓ {table_name}: {inserted} rows → SQLite, CSV: {csv_path.name}{pq_note}")

    # ── Stage 5: Validate ─────────────────────────────────────────
    print("\n▶ Stage 5: Validating …")
    results = validate_database(DB_PATH)
    print_validation_report(results)

    # ── Stage 6: Schema summary ───────────────────────────────────
    print("▶ Stage 6: Generating schema summary …")
    save_schema_summary(SCHEMA_PATH)

    # ── Summary ───────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("PIPELINE COMPLETE")
    print("=" * 72)
    print(f"  Database  : {DB_PATH}")
    print(f"  Clean CSVs: {CLEAN_DIR}")
    print(f"  Schema    : {SCHEMA_PATH}")
    print(f"  Profiles  : {PROFILES_PATH}")
    total_rows = sum(entity_row_counts.values())
    print(f"  Total rows: {total_rows:,} across {len(entity_row_counts)} tables")
    print()


if __name__ == "__main__":
    main()
