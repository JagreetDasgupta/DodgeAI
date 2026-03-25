"""
sql_queries.py — SQL-based aggregation and lookup helpers.

Queries run against the Phase 1 SQLite database (output/sap_o2c.db).
All functions return lists of dicts ready for QueryResponse.records.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────────────────────────
# Generic aggregation
# ──────────────────────────────────────────────────────────────────

def top_by_count(
    db_path: Path,
    *,
    count_table: str,
    count_column: str,
    group_table: str | None = None,
    group_column: str | None = None,
    order: str = "desc",
    limit: int = 10,
) -> list[dict]:
    """
    Generic 'top N by count' query.

    If group_table==count_table, does a simple GROUP BY count_column.
    Otherwise joins count_table to group_table via matching column names.
    """
    direction = "DESC" if order.lower() == "desc" else "ASC"
    conn = _connect(db_path)
    cur = conn.cursor()

    sql = (
        f'SELECT "{count_column}", COUNT(*) AS cnt '
        f'FROM "{count_table}" '
        f'GROUP BY "{count_column}" '
        f'ORDER BY cnt {direction} '
        f'LIMIT {int(limit)}'
    )
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ──────────────────────────────────────────────────────────────────
# Pre-built aggregation metrics
# ──────────────────────────────────────────────────────────────────

_METRICS: dict[str, dict[str, str]] = {
    # product metrics
    "product_billing_document_count": {
        "count_table": "billing_document_items",
        "count_column": "material",
        "label": "Products by number of billing document items",
    },
    "product_sales_order_count": {
        "count_table": "sales_order_items",
        "count_column": "material",
        "label": "Products by number of sales order items",
    },

    # customer metrics
    "customer_sales_order_count": {
        "count_table": "sales_order_headers",
        "count_column": "sold_to_party",
        "label": "Customers by number of sales orders",
    },
    "customer_billing_count": {
        "count_table": "billing_document_headers",
        "count_column": "sold_to_party",
        "label": "Customers by number of billing documents",
    },
    "customer_payment_count": {
        "count_table": "payments_accounts_receivable",
        "count_column": "customer",
        "label": "Customers by number of payments",
    },

    # billing metrics
    "billing_document_item_count": {
        "count_table": "billing_document_items",
        "count_column": "billing_document",
        "label": "Billing documents by number of items",
    },

    # sales order metrics
    "sales_order_item_count": {
        "count_table": "sales_order_items",
        "count_column": "sales_order",
        "label": "Sales orders by number of items",
    },

    # delivery metrics
    "delivery_item_count": {
        "count_table": "outbound_delivery_items",
        "count_column": "delivery_document",
        "label": "Delivery documents by number of items",
    },

    # plant metrics
    "plant_product_count": {
        "count_table": "product_plants",
        "count_column": "plant",
        "label": "Plants by number of products",
    },

    # journal entry count per customer
    "customer_journal_entry_count": {
        "count_table": "journal_entry_items_ar",
        "count_column": "customer",
        "label": "Customers by number of journal entries",
    },
}


def run_aggregation(
    db_path: Path,
    metric: str,
    order: str = "desc",
    limit: int = 10,
) -> tuple[list[dict], str]:
    """
    Execute a pre-defined aggregation metric.

    Returns (records, label).  Raises KeyError if metric unknown.
    """
    if metric not in _METRICS:
        known = ", ".join(sorted(_METRICS.keys()))
        raise KeyError(f"Unknown metric '{metric}'. Known metrics: {known}")

    spec = _METRICS[metric]
    rows = top_by_count(
        db_path,
        count_table=spec["count_table"],
        count_column=spec["count_column"],
        order=order,
        limit=limit,
    )
    return rows, spec["label"]


def list_available_metrics() -> list[dict]:
    """Return all registered metrics with labels."""
    return [{"metric": k, "label": v["label"]} for k, v in sorted(_METRICS.items())]


# ──────────────────────────────────────────────────────────────────
# Record lookup
# ──────────────────────────────────────────────────────────────────

def lookup_record(
    db_path: Path,
    table: str,
    pk_column: str,
    pk_value: str,
) -> dict | None:
    """Fetch a single record by PK.  Returns None if not found."""
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{table}" WHERE "{pk_column}" = ?', (pk_value,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def count_table(db_path: Path, table: str) -> int:
    """Return row count for a table."""
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    cnt = cur.fetchone()[0]
    conn.close()
    return cnt
