"""
storage_writer.py — Persists cleaned data to SQLite and exports Parquet / CSV.

Creates:
  output/sap_o2c.db          — one table per entity
  output/clean/<entity>.csv  — CSV export
  output/clean/<entity>.parquet  — Parquet export  (requires pyarrow)
"""

import csv
import sqlite3
from pathlib import Path
from typing import Any


# ──────────────────────────────────────────────────────────────────
# SQLite helpers
# ──────────────────────────────────────────────────────────────────

def _sqlite_type(value: Any) -> str:
    """Map a Python value to a SQLite column affinity."""
    if isinstance(value, bool) or isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


def _infer_column_types(records: list[dict]) -> dict[str, str]:
    """Scan records to determine best SQLite type per column."""
    if not records:
        return {}
    columns = list(records[0].keys())
    col_types: dict[str, str] = {c: "TEXT" for c in columns}

    for rec in records[:200]:        # sample first 200
        for col in columns:
            val = rec.get(col)
            if val is None:
                continue
            inferred = _sqlite_type(val)
            # Promote: TEXT > REAL > INTEGER (broadest wins)
            if inferred == "TEXT":
                col_types[col] = "TEXT"
            elif inferred == "REAL" and col_types[col] != "TEXT":
                col_types[col] = "REAL"
            elif inferred == "INTEGER" and col_types[col] == "INTEGER":
                pass
    return col_types


def write_sqlite(
    db_path: Path,
    table_name: str,
    records: list[dict],
    primary_key: list[str] | None = None,
) -> int:
    """
    Write records into a SQLite table, creating it if needed.

    Returns the number of rows inserted.
    """
    if not records:
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    col_types = _infer_column_types(records)
    columns = list(records[0].keys())

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Build CREATE TABLE DDL
    col_defs = ", ".join(f'"{c}" {col_types.get(c, "TEXT")}' for c in columns)
    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        pk_clause = f", PRIMARY KEY ({pk_cols})"
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs}{pk_clause});'
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cur.execute(ddl)

    # Bulk insert
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f'INSERT INTO "{table_name}" ({", ".join(f"{c!r}" for c in columns)}) VALUES ({placeholders});'
    # Column quoting fix — use double-quote style
    col_list = ", ".join(f'"{c}"' for c in columns)
    insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders});'

    rows = [tuple(rec.get(c) for c in columns) for rec in records]
    cur.executemany(insert_sql, rows)
    conn.commit()
    count = cur.rowcount
    conn.close()
    return len(rows)


# ──────────────────────────────────────────────────────────────────
# CSV export
# ──────────────────────────────────────────────────────────────────

def write_csv(output_dir: Path, table_name: str, records: list[dict]) -> Path:
    """Write records as a CSV file.  Returns the path written."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{table_name}.csv"
    if not records:
        csv_path.write_text("")
        return csv_path

    columns = list(records[0].keys())
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
    return csv_path


# ──────────────────────────────────────────────────────────────────
# Parquet export (optional — graceful fallback if pyarrow missing)
# ──────────────────────────────────────────────────────────────────

def write_parquet(output_dir: Path, table_name: str, records: list[dict]) -> Path | None:
    """
    Write records as a Parquet file (requires pyarrow).

    Returns the path if successful, or None if pyarrow is not installed.
    """
    try:
        import pyarrow as pa            # type: ignore
        import pyarrow.parquet as pq     # type: ignore
    except ImportError:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{table_name}.parquet"
    if not records:
        return None

    columns = list(records[0].keys())
    col_data = {c: [r.get(c) for r in records] for c in columns}
    table = pa.table(col_data)
    pq.write_table(table, str(parquet_path))
    return parquet_path
