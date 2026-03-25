"""
anomaly_checks.py — Integrity and broken-flow detection.

Combines SQL and graph queries to find:
  - delivered but not billed orders
  - billing docs without deliveries
  - unlinked payments
  - incomplete O2C flows
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from graph_queries import _load_graph


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────────────────────────
# SQL-based anomaly checks
# ──────────────────────────────────────────────────────────────────

def delivered_not_billed(db_path: Path) -> list[dict]:
    """
    Find delivery documents that have no matching billing document item.
    Uses: outbound_delivery_headers vs billing_document_items.reference_sd_document.
    """
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT d.delivery_document, d.overall_goods_movement_status, d.creation_date
        FROM outbound_delivery_headers d
        LEFT JOIN billing_document_items bi
            ON d.delivery_document = bi.reference_sd_document
        WHERE bi.reference_sd_document IS NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def billed_without_delivery(db_path: Path) -> list[dict]:
    """
    Find billing documents whose reference_sd_document does not exist
    in outbound_delivery_headers.
    """
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT bi.billing_document, bi.reference_sd_document
        FROM billing_document_items bi
        LEFT JOIN outbound_delivery_headers d
            ON bi.reference_sd_document = d.delivery_document
        WHERE d.delivery_document IS NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def payments_without_journal_link(db_path: Path) -> list[dict]:
    """
    Find payment records whose clearing_accounting_document does not
    match any journal_entry_items_ar.accounting_document.
    """
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.accounting_document, p.clearing_accounting_document, p.customer,
               p.amount_in_transaction_currency
        FROM payments_accounts_receivable p
        LEFT JOIN journal_entry_items_ar j
            ON p.clearing_accounting_document = j.accounting_document
        WHERE j.accounting_document IS NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def orders_without_delivery(db_path: Path) -> list[dict]:
    """
    Find sales orders that have no matching delivery (via outbound_delivery_items.reference_sd_document).
    """
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT s.sales_order, s.sold_to_party, s.total_net_amount, s.creation_date
        FROM sales_order_headers s
        LEFT JOIN outbound_delivery_items di
            ON s.sales_order = di.reference_sd_document
        WHERE di.reference_sd_document IS NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def billing_without_journal(db_path: Path) -> list[dict]:
    """
    Find billing documents without a corresponding journal entry.
    """
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT bh.billing_document, bh.total_net_amount, bh.sold_to_party
        FROM billing_document_headers bh
        LEFT JOIN journal_entry_items_ar j
            ON bh.billing_document = j.reference_document
        WHERE j.reference_document IS NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ──────────────────────────────────────────────────────────────────
# Graph-based anomaly checks
# ──────────────────────────────────────────────────────────────────

def disconnected_nodes(graphml_path: Path) -> list[dict]:
    """Find nodes with zero edges."""
    G = _load_graph(graphml_path)
    results = []
    for nid in G.nodes():
        if G.degree(nid) == 0:
            data = G.nodes[nid]
            results.append({
                "node_id": nid,
                "node_type": data.get("node_type", ""),
                "label": data.get("label", ""),
            })
    return results


def incomplete_o2c_flows(graphml_path: Path) -> list[dict]:
    """
    Check each sales_order_headers node for a complete O2C chain.
    Returns orders with missing steps.
    """
    G = _load_graph(graphml_path)

    _CHAIN = [
        "sales_order_headers",
        "sales_order_items",
        "outbound_delivery_items",
        "outbound_delivery_headers",
        "billing_document_items",
        "billing_document_headers",
        "journal_entry_items_ar",
        "payments_accounts_receivable",
    ]

    results = []
    for nid, data in G.nodes(data=True):
        if data.get("node_type") != "sales_order_headers":
            continue

        # BFS from this order
        reached_types: set[str] = set()
        visited = {nid}
        frontier = {nid}
        for _ in range(8):
            nxt: set[str] = set()
            for n in frontier:
                for nbr in set(G.successors(n)) | set(G.predecessors(n)):
                    if nbr not in visited:
                        visited.add(nbr)
                        nxt.add(nbr)
                        reached_types.add(G.nodes[nbr].get("node_type", ""))
            if not nxt:
                break
            frontier = nxt

        reached_types.add("sales_order_headers")
        missing = [s for s in _CHAIN if s not in reached_types]
        if missing:
            results.append({
                "order_node": nid,
                "label": data.get("label", ""),
                "missing_steps": missing,
                "reached_steps": [s for s in _CHAIN if s in reached_types],
            })

    return results


# ──────────────────────────────────────────────────────────────────
# Registry of check types
# ──────────────────────────────────────────────────────────────────

INTEGRITY_CHECKS: dict[str, dict] = {
    "delivered_not_billed": {
        "fn_type": "sql",
        "label": "Deliveries without a matching billing document",
    },
    "billed_without_delivery": {
        "fn_type": "sql",
        "label": "Billing documents referencing non-existent deliveries",
    },
    "payments_without_journal_link": {
        "fn_type": "sql",
        "label": "Payments not linked to any journal entry",
    },
    "orders_without_delivery": {
        "fn_type": "sql",
        "label": "Sales orders with no delivery",
    },
    "billing_without_journal": {
        "fn_type": "sql",
        "label": "Billing documents with no journal entry",
    },
    "disconnected_nodes": {
        "fn_type": "graph",
        "label": "Nodes with zero edges in the graph",
    },
    "incomplete_o2c_flows": {
        "fn_type": "graph",
        "label": "Sales orders with incomplete O2C flow chains",
    },
}


def run_integrity_check(
    check_type: str,
    db_path: Path,
    graphml_path: Path,
) -> tuple[list[dict], str]:
    """
    Run a named integrity check.

    Returns (records, label).
    Raises KeyError if check_type is unknown.
    """
    if check_type not in INTEGRITY_CHECKS:
        known = ", ".join(sorted(INTEGRITY_CHECKS.keys()))
        raise KeyError(f"Unknown check_type '{check_type}'. Known: {known}")

    spec = INTEGRITY_CHECKS[check_type]

    fn_map = {
        "delivered_not_billed": lambda: delivered_not_billed(db_path),
        "billed_without_delivery": lambda: billed_without_delivery(db_path),
        "payments_without_journal_link": lambda: payments_without_journal_link(db_path),
        "orders_without_delivery": lambda: orders_without_delivery(db_path),
        "billing_without_journal": lambda: billing_without_journal(db_path),
        "disconnected_nodes": lambda: disconnected_nodes(graphml_path),
        "incomplete_o2c_flows": lambda: incomplete_o2c_flows(graphml_path),
    }

    rows = fn_map[check_type]()
    return rows, spec["label"]


def list_available_checks() -> list[dict]:
    """Return all registered integrity checks."""
    return [{"check_type": k, "label": v["label"]} for k, v in sorted(INTEGRITY_CHECKS.items())]
