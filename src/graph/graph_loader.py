"""
graph_loader.py — Loads Phase 1 schema + SQLite data for graph construction.

Provides:
  load_schema()       → parsed schema_summary.json
  load_table_rows()   → all rows from a given SQLite table as list[dict]
  get_table_columns() → column names for a table
"""

import json
import sqlite3
from pathlib import Path
from typing import Any


def load_schema(schema_path: Path) -> dict:
    """Load and return the schema summary JSON."""
    with open(schema_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_table_rows(db_path: Path, table_name: str) -> list[dict[str, Any]]:
    """
    Load all rows from *table_name* as a list of dicts.
    Column names become dict keys.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{table_name}"')
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_table_columns(db_path: Path, table_name: str) -> list[str]:
    """Return ordered column names for a table."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table_name}")')
    cols = [row[1] for row in cur.fetchall()]
    conn.close()
    return cols
