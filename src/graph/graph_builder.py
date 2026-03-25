"""
graph_builder.py — Constructs a NetworkX DiGraph from the Phase 1 data.

Nodes:  one per record across all 19 tables.
Edges:  one per FK match found in actual data, with confidence metadata.
"""

from __future__ import annotations

import networkx as nx
from pathlib import Path
from typing import Any

from graph_loader import load_schema, load_table_rows


# ──────────────────────────────────────────────────────────────────
# Node-ID generation
# ──────────────────────────────────────────────────────────────────

def make_node_id(table_name: str, pk_fields: dict[str, Any]) -> str:
    """
    Deterministic, stable node ID.
    Format: ``table_name::val1[::val2[::...]]``
    """
    vals = "::".join(str(pk_fields[k]) for k in sorted(pk_fields))
    return f"{table_name}::{vals}"


# ──────────────────────────────────────────────────────────────────
# Human-readable label
# ──────────────────────────────────────────────────────────────────

# For each table, pick the most informative columns for a short label.
_LABEL_FIELDS: dict[str, list[str]] = {
    "sales_order_headers":              ["sales_order", "sold_to_party", "total_net_amount"],
    "sales_order_items":                ["sales_order", "sales_order_item", "material", "net_amount"],
    "sales_order_schedule_lines":       ["sales_order", "sales_order_item", "schedule_line"],
    "outbound_delivery_headers":        ["delivery_document", "overall_goods_movement_status"],
    "outbound_delivery_items":          ["delivery_document", "delivery_document_item", "actual_delivery_quantity"],
    "billing_document_headers":         ["billing_document", "total_net_amount", "sold_to_party"],
    "billing_document_items":           ["billing_document", "billing_document_item", "net_amount"],
    "billing_document_cancellations":   ["billing_document", "total_net_amount"],
    "journal_entry_items_ar":           ["accounting_document", "amount_in_transaction_currency"],
    "payments_accounts_receivable":     ["accounting_document", "amount_in_transaction_currency", "customer"],
    "business_partners":                ["business_partner", "business_partner_name"],
    "business_partner_addresses":       ["business_partner", "address_id", "city_name", "region"],
    "customer_company_assignments":     ["customer", "company_code"],
    "customer_sales_area_assignments":  ["customer", "sales_organization", "distribution_channel"],
    "products":                         ["product", "product_old_id", "product_type"],
    "product_descriptions":             ["product", "product_description"],
    "product_plants":                   ["product", "plant"],
    "product_storage_locations":        ["product", "plant", "storage_location"],
    "plants":                           ["plant", "plant_name"],
}


def _build_label(table_name: str, row: dict) -> str:
    """Build a concise human-readable label from key fields."""
    fields = _LABEL_FIELDS.get(table_name, [])
    parts = []
    for f in fields:
        v = row.get(f)
        if v is not None and v != "":
            parts.append(f"{f}={v}")
    return f"[{table_name}] " + ", ".join(parts) if parts else f"[{table_name}]"


# ──────────────────────────────────────────────────────────────────
# Metadata selection (exclude lineage and very large fields)
# ──────────────────────────────────────────────────────────────────

_SKIP_META = {"_source_file", "_source_row"}


def _select_metadata(row: dict, pk_keys: set[str]) -> dict:
    """Return all non-PK, non-lineage fields as metadata."""
    return {k: v for k, v in row.items()
            if k not in pk_keys and k not in _SKIP_META}


# ──────────────────────────────────────────────────────────────────
# Full graph construction
# ──────────────────────────────────────────────────────────────────

def build_graph(db_path: Path, schema_path: Path) -> nx.DiGraph:
    """
    Build the complete O2C graph.

    Steps
    -----
    1. Load schema and iterate over every entity.
    2. For each row → create a node.
    3. For each FK definition → create edges where data matches.
    """
    schema = load_schema(schema_path)
    G = nx.DiGraph()

    # --- Pass 1: Create nodes ------------------------------------------
    # Also build a lookup: (table_name, tuple_of_pk_values) → node_id
    pk_index: dict[str, dict[tuple, str]] = {}   # table → {pk_tuple → node_id}

    for entity in schema["entities"]:
        table = entity["table_name"]
        pk_cols = entity["primary_key"]
        rows = load_table_rows(db_path, table)
        pk_index[table] = {}

        for row in rows:
            pk_fields = {c: row[c] for c in pk_cols}
            node_id = make_node_id(table, pk_fields)
            pk_tuple = tuple(str(row[c]) for c in pk_cols)
            pk_index[table][pk_tuple] = node_id

            G.add_node(
                node_id,
                node_type=table,
                label=_build_label(table, row),
                source_table=table,
                pk_fields=str(pk_fields),       # serialised for GraphML compat
                metadata=str(_select_metadata(row, set(pk_cols))),
                _source_file=row.get("_source_file", ""),
                _source_row=row.get("_source_row", ""),
            )

    # --- Pass 2: Create edges ------------------------------------------
    for entity in schema["entities"]:
        child_table = entity["table_name"]
        pk_cols = entity["primary_key"]
        rows = load_table_rows(db_path, child_table)

        for fk in entity.get("foreign_keys", []):
            fk_cols = fk["columns"]
            parent_table = fk["references_table"]
            ref_cols = fk["references_columns"]
            confidence = fk.get("confidence", "high")
            note = fk.get("note", "")

            parent_index = pk_index.get(parent_table, {})
            edge_type = f"{child_table}->{parent_table}"

            for row in rows:
                # Build child node ID
                child_pk = {c: row[c] for c in pk_cols}
                child_id = make_node_id(child_table, child_pk)

                # Build lookup key into parent index
                fk_values = tuple(str(row.get(c, "")) for c in fk_cols)

                # Skip if FK value is null/empty
                if any(v == "" or v == "None" for v in fk_values):
                    continue

                # For single-column FKs the parent PK tuple is the FK value.
                # For multi-column FKs, the FK columns must match the referenced
                # columns in order (they may be a prefix of a composite PK).
                # We need to find any parent whose PK *starts with* fk_values.
                parent_id = parent_index.get(fk_values)

                if parent_id is None and len(fk_values) < len(ref_cols):
                    # Won't happen with our schema, but safety net.
                    continue

                if parent_id is None:
                    # No matching parent — record an orphan edge for traceability
                    # but do NOT add the edge (there is no target node).
                    continue

                G.add_edge(
                    child_id,
                    parent_id,
                    edge_type=edge_type,
                    confidence=confidence,
                    fk_columns=str(fk_cols),
                    ref_columns=str(ref_cols),
                    note=note,
                )

    return G


def get_graph_summary(G: nx.DiGraph) -> dict:
    """Return a short summary of graph statistics."""
    node_types: dict[str, int] = {}
    for _, data in G.nodes(data=True):
        t = data.get("node_type", "unknown")
        node_types[t] = node_types.get(t, 0) + 1

    edge_types: dict[str, int] = {}
    uncertain_count = 0
    for _, _, data in G.edges(data=True):
        t = data.get("edge_type", "unknown")
        edge_types[t] = edge_types.get(t, 0) + 1
        if data.get("confidence") == "uncertain":
            uncertain_count += 1

    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "node_types": node_types,
        "edge_types": edge_types,
        "uncertain_edges": uncertain_count,
        "connected_components": nx.number_weakly_connected_components(G),
    }
