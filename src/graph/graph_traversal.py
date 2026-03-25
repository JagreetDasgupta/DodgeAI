"""
graph_traversal.py — Traversal and query helpers for the O2C graph.

Provides:
  get_node / get_neighbors / trace_flow / extract_subgraph
  find_disconnected_nodes / find_incomplete_flows
  Business-flow tracers for order→delivery→billing→journal→payment
"""

from __future__ import annotations

import networkx as nx
from typing import Any


# ──────────────────────────────────────────────────────────────────
# Basic helpers
# ──────────────────────────────────────────────────────────────────

def get_node(G: nx.DiGraph, node_id: str) -> dict | None:
    """Return node attributes or None if not found."""
    if node_id in G:
        return dict(G.nodes[node_id])
    return None


def get_neighbors(
    G: nx.DiGraph,
    node_id: str,
    direction: str = "both",
) -> list[dict]:
    """
    Return neighbour nodes with edge metadata.

    Parameters
    ----------
    direction : 'out' | 'in' | 'both'
    """
    results: list[dict] = []
    if direction in ("out", "both"):
        for _, tgt, edata in G.out_edges(node_id, data=True):
            results.append({
                "node_id": tgt,
                "direction": "out",
                **dict(G.nodes[tgt]),
                "edge": dict(edata),
            })
    if direction in ("in", "both"):
        for src, _, edata in G.in_edges(node_id, data=True):
            results.append({
                "node_id": src,
                "direction": "in",
                **dict(G.nodes[src]),
                "edge": dict(edata),
            })
    return results


# ──────────────────────────────────────────────────────────────────
# Flow tracing (BFS up to max_depth)
# ──────────────────────────────────────────────────────────────────

def trace_flow(
    G: nx.DiGraph,
    start_node_id: str,
    max_depth: int = 5,
    direction: str = "both",
) -> list[dict]:
    """
    BFS from *start_node_id* up to *max_depth* hops.

    Returns a list of visited nodes with their depth and path.
    """
    if start_node_id not in G:
        return []

    visited: dict[str, int] = {start_node_id: 0}
    queue: list[tuple[str, int]] = [(start_node_id, 0)]
    results: list[dict] = []

    while queue:
        current, depth = queue.pop(0)
        node_data = dict(G.nodes[current])
        results.append({"node_id": current, "depth": depth, **node_data})

        if depth >= max_depth:
            continue

        neighbours = set()
        if direction in ("out", "both"):
            neighbours.update(G.successors(current))
        if direction in ("in", "both"):
            neighbours.update(G.predecessors(current))

        for nbr in neighbours:
            if nbr not in visited:
                visited[nbr] = depth + 1
                queue.append((nbr, depth + 1))

    return results


def extract_subgraph(
    G: nx.DiGraph,
    node_id: str,
    depth: int = 2,
) -> nx.DiGraph:
    """
    Extract the ego-subgraph around *node_id* within *depth* hops
    (both directions).
    """
    nodes = {n["node_id"] for n in trace_flow(G, node_id, max_depth=depth)}
    return G.subgraph(nodes).copy()


# ──────────────────────────────────────────────────────────────────
# Disconnected / orphan detection
# ──────────────────────────────────────────────────────────────────

def find_disconnected_nodes(G: nx.DiGraph) -> list[str]:
    """Return node IDs with zero edges (completely isolated)."""
    return [n for n in G.nodes() if G.degree(n) == 0]


def find_disconnected_by_type(G: nx.DiGraph) -> dict[str, list[str]]:
    """Return disconnected nodes grouped by node_type."""
    groups: dict[str, list[str]] = {}
    for n in G.nodes():
        if G.degree(n) == 0:
            ntype = G.nodes[n].get("node_type", "unknown")
            groups.setdefault(ntype, []).append(n)
    return groups


# ──────────────────────────────────────────────────────────────────
# Business-flow tracers
# ──────────────────────────────────────────────────────────────────

# The O2C flow chain:
#   SalesOrder → OrderItem → Delivery → BillingDoc → JournalEntry → Payment
# Edges go child→parent (FK direction), so:
#   order_item →(out)→ order_header  (child has FK to parent)
# We want the *business flow* direction, which may be opposite.

_FLOW_CHAIN = [
    "sales_order_headers",
    "sales_order_items",
    "outbound_delivery_items",
    "outbound_delivery_headers",
    "billing_document_items",
    "billing_document_headers",
    "journal_entry_items_ar",
    "payments_accounts_receivable",
]


def _typed_neighbors(G: nx.DiGraph, node_id: str, target_type: str) -> list[str]:
    """Return all neighbor IDs whose node_type matches *target_type*."""
    nbrs = set(G.successors(node_id)) | set(G.predecessors(node_id))
    return [n for n in nbrs if G.nodes[n].get("node_type") == target_type]


def trace_order_flow(G: nx.DiGraph, order_node_id: str) -> dict[str, list[str]]:
    """
    Trace a sales order through the O2C chain.

    Returns a dict keyed by entity type with lists of reached node IDs.
    """
    flow: dict[str, list[str]] = {t: [] for t in _FLOW_CHAIN}
    flow["sales_order_headers"].append(order_node_id)

    # From order header → find order items
    items = _typed_neighbors(G, order_node_id, "sales_order_items")
    flow["sales_order_items"] = items

    # From order items → find delivery items
    for item_id in items:
        flow["outbound_delivery_items"].extend(
            _typed_neighbors(G, item_id, "outbound_delivery_items")
        )

    # From delivery items → delivery headers
    for di_id in flow["outbound_delivery_items"]:
        flow["outbound_delivery_headers"].extend(
            _typed_neighbors(G, di_id, "outbound_delivery_headers")
        )

    # From delivery headers → billing items
    for dh_id in flow["outbound_delivery_headers"]:
        flow["billing_document_items"].extend(
            _typed_neighbors(G, dh_id, "billing_document_items")
        )

    # From billing items → billing headers
    for bi_id in flow["billing_document_items"]:
        flow["billing_document_headers"].extend(
            _typed_neighbors(G, bi_id, "billing_document_headers")
        )

    # From billing headers → journal entries
    for bh_id in flow["billing_document_headers"]:
        flow["journal_entry_items_ar"].extend(
            _typed_neighbors(G, bh_id, "journal_entry_items_ar")
        )

    # From journal entries → payments
    for je_id in flow["journal_entry_items_ar"]:
        flow["payments_accounts_receivable"].extend(
            _typed_neighbors(G, je_id, "payments_accounts_receivable")
        )

    # Deduplicate
    for k in flow:
        flow[k] = list(dict.fromkeys(flow[k]))

    return flow


def find_incomplete_flows(G: nx.DiGraph) -> list[dict]:
    """
    Check each sales_order_headers node for a complete O2C chain.
    Returns list of orders with gaps.
    """
    incomplete = []
    for nid, data in G.nodes(data=True):
        if data.get("node_type") != "sales_order_headers":
            continue
        chain = trace_order_flow(G, nid)
        gaps = [step for step in _FLOW_CHAIN if not chain[step]]
        if gaps:
            incomplete.append({
                "order_node": nid,
                "missing_steps": gaps,
                "reached_steps": [s for s in _FLOW_CHAIN if chain[s]],
            })
    return incomplete
