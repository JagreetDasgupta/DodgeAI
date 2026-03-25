"""
transformer.py — Core normalization pipeline.

Transforms raw JSONL records into clean, flat dicts ready for SQL storage:
  • camelCase  →  snake_case column names
  • ISO 8601 dates  →  YYYY-MM-DD strings
  • Nested {hours, minutes, seconds} dicts  →  HH:MM:SS strings
  • Numeric strings  →  float / int where appropriate
  • 100 %-null columns  →  dropped
  • Lineage columns (_source_file, _source_row) appended
"""

import re
from typing import Any

from src.schema_inference import camel_to_snake


# ──────────────────────────────────────────────────────────────────
# Column-name normalisation
# ──────────────────────────────────────────────────────────────────

def normalise_column_names(record: dict) -> dict:
    """Convert all keys in *record* from camelCase to snake_case."""
    return {camel_to_snake(k): v for k, v in record.items()}


# ──────────────────────────────────────────────────────────────────
# Value-level transformations
# ──────────────────────────────────────────────────────────────────

_ISO_DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?$"
)

# Columns whose string values should be cast to float
_NUMERIC_COLUMNS = {
    "total_net_amount", "net_amount", "gross_weight", "net_weight",
    "billing_quantity", "actual_delivery_quantity", "requested_quantity",
    "confd_order_qty_by_matl_avail_check",
    "amount_in_company_code_currency", "amount_in_transaction_currency",
}


def _parse_iso_date(value: str) -> str:
    """Extract YYYY-MM-DD from an ISO 8601 datetime string."""
    return value[:10]


def _flatten_time_dict(d: dict) -> str:
    """Convert {'hours': H, 'minutes': M, 'seconds': S} → 'HH:MM:SS'."""
    h = d.get("hours", 0)
    m = d.get("minutes", 0)
    s = d.get("seconds", 0)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _try_cast_numeric(value: Any, col_name: str) -> Any:
    """Cast string to float if column is known-numeric; else return as-is."""
    if col_name not in _NUMERIC_COLUMNS:
        return value
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def transform_value(key: str, value: Any) -> Any:
    """Apply all value-level transforms to a single field."""
    if value is None:
        return None

    # Flatten nested time dicts
    if isinstance(value, dict) and "hours" in value:
        return _flatten_time_dict(value)

    # Parse ISO date strings
    if isinstance(value, str) and _ISO_DATE_RE.match(value):
        return _parse_iso_date(value)

    # Cast known-numeric columns
    value = _try_cast_numeric(value, key)

    # Convert booleans stored as Python bool → 0/1 for SQL compat
    if isinstance(value, bool):
        return int(value)

    return value


# ──────────────────────────────────────────────────────────────────
# Record and entity transforms
# ──────────────────────────────────────────────────────────────────

def transform_record(record: dict, source_file: str, source_row: int) -> dict:
    """
    Apply full normalisation to a single raw record.

    1. Rename columns to snake_case.
    2. Transform each value.
    3. Attach lineage metadata.
    """
    renamed = normalise_column_names(record)
    transformed = {k: transform_value(k, v) for k, v in renamed.items()}
    transformed["_source_file"] = source_file
    transformed["_source_row"] = source_row
    return transformed


def drop_all_null_columns(records: list[dict]) -> list[dict]:
    """Remove columns that are NULL (or empty-string) in *every* record."""
    if not records:
        return records

    all_keys = set()
    for r in records:
        all_keys.update(r.keys())

    cols_to_drop: set[str] = set()
    for col in all_keys:
        if col.startswith("_"):          # keep lineage cols
            continue
        if all(r.get(col) is None or r.get(col) == "" for r in records):
            cols_to_drop.add(col)

    if not cols_to_drop:
        return records

    return [{k: v for k, v in r.items() if k not in cols_to_drop} for r in records]


def deduplicate(records: list[dict]) -> list[dict]:
    """Remove exact-duplicate rows (keeping first occurrence)."""
    import json as _json
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in records:
        sig = _json.dumps(r, sort_keys=True, default=str)
        if sig not in seen:
            seen.add(sig)
            deduped.append(r)
    return deduped


def transform_entity(
    entity_name: str,
    raw_records: list[dict],
    file_map: list[tuple[str, int, int]],
) -> list[dict]:
    """
    Full transformation pipeline for one entity.

    Parameters
    ----------
    entity_name : str
        Entity / table name.
    raw_records : list[dict]
        All raw JSON records concatenated from part-files.
    file_map : list of (filename, start_row, end_row)
        Mapping from global record index to source-file origin.
        Used to attach _source_file / _source_row lineage.

    Returns
    -------
    list[dict]
        Normalised, cleaned records.
    """
    # Build a lookup: global index → (filename, local_row)
    idx_to_source: dict[int, tuple[str, int]] = {}
    for fname, start, end in file_map:
        for i in range(start, end):
            idx_to_source[i] = (fname, i - start + 1)

    transformed = []
    for idx, rec in enumerate(raw_records):
        src_file, src_row = idx_to_source.get(idx, ("unknown", idx + 1))
        transformed.append(transform_record(rec, src_file, src_row))

    transformed = drop_all_null_columns(transformed)
    transformed = deduplicate(transformed)
    return transformed
