"""
graph_queries.py — Graph-based traversal, flow-tracing, and neighborhood queries.

Loads the Phase 2 graph (GraphML) and delegates to graph_traversal helpers.
"""

from __future__ import annotations

import networkx as nx
from pathlib import Path
from typing import Any, Optional
import json


# ──────────────────────────────────────────────────────────────────
# Graph loading (cached at module level for reuse)
# ──────────────────────────────────────────────────────────────────

_GRAPH_CACHE: dict[str, nx.DiGraph] = {}


def _load_graph(graphml_path: Path) -> nx.DiGraph:
    key = str(graphml_path)
    if key not in _GRAPH_CACHE:
        _GRAPH_CACHE[key] = nx.read_graphml(str(graphml_path))
    return _GRAPH_CACHE[key]


def to_visjs_format(nodes: list[dict], edges: list[dict], max_nodes: int = 400) -> dict:
    """
    Format subgraphs for vis-network. Truncates if nodes > max_nodes.
    Nodes must have: node_id, label, node_type (for grouping), and payload data.
    Edges must have: source, target, edge_type, confidence.
    """
    if len(nodes) > max_nodes:
        nodes = nodes[:max_nodes]
        kept_ids = {n["node_id"] for n in nodes}
        edges = [e for e in edges if e["source"] in kept_ids and e["target"] in kept_ids]

    vis_nodes = []
    for n in nodes:
        # Create a clean payload for the metadata popup
        data_payload = {k: v for k, v in n.items() if k not in ("node_id", "label", "node_type")}
        vis_nodes.append({
            "id": n["node_id"],
            "label": n.get("label", n["node_id"])[:30],  # Truncate overly long labels visually
            "group": n.get("node_type", "unknown"),
            "data": data_payload,
        })

    vis_edges = []
    for e in edges:
        vis_edges.append({
            "from": e["source"],
            "to": e["target"],
            "label": e.get("edge_type", ""),
            "dashes": e.get("confidence") == "uncertain",  # Dashed line for uncertain
        })

    return {"nodes": vis_nodes, "edges": vis_edges}


# ──────────────────────────────────────────────────────────────────
# Node helpers
# ──────────────────────────────────────────────────────────────────

def get_node(G: nx.DiGraph, node_id: str) -> dict | None:
    if node_id in G:
        return {"node_id": node_id, **dict(G.nodes[node_id])}
    return None


def resolve_node_id(G: nx.DiGraph, entity_id: str, entity_type: Optional[str] = None) -> str | None:
    """
    Resolve a user-facing ID string (e.g. '740506') to an internal graph node ID ('sales_order_headers::740506').
    If entity_type is given, checks that exact combination first.
    If no entity_type, scans for '::{entity_id}' and applies a priority heuristic if multiple exist.
    """
    if entity_type:
        candidate = f"{entity_type}::{entity_id}"
        if candidate in G:
            return candidate

    # Fallback: scan nodes for matching ID
    matches = []
    suffix = f"::{entity_id}"
    for nid in G.nodes():
        if nid.endswith(suffix):
            matches.append(nid)

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    # Heuristic priority if multiple matches found
    priority = ["sales_order_headers", "business_partners", "outbound_delivery_headers", "billing_document_headers", "plants", "products"]
    for p in priority:
        for m in matches:
            if m.startswith(p + "::"):
                return m

    return matches[0]


# ──────────────────────────────────────────────────────────────────
# Neighborhood / subgraph
# ──────────────────────────────────────────────────────────────────

def neighborhood(
    graphml_path: Path,
    node_id: str,
    depth: int = 2,
) -> dict:
    """
    Extract the ego-subgraph around *node_id* within *depth* hops.
    Returns nodes list, edges list, and counts.
    """
    G = _load_graph(graphml_path)
    if node_id not in G:
        return {"status": "not_found", "node_id": node_id, "nodes": [], "edges": []}

    # BFS to collect reachable nodes
    visited = {node_id: 0}
    queue = [(node_id, 0)]
    while queue:
        cur, d = queue.pop(0)
        if d >= depth:
            continue
        for nbr in set(G.successors(cur)) | set(G.predecessors(cur)):
            if nbr not in visited:
                visited[nbr] = d + 1
                queue.append((nbr, d + 1))

    sub_nodes = []
    for nid, d in visited.items():
        data = dict(G.nodes[nid])
        sub_nodes.append({"node_id": nid, "depth": d, "node_type": data.get("node_type", ""), "label": data.get("label", "")})

    sub_edges = []
    node_set = set(visited.keys())
    for u, v, data in G.edges(data=True):
        if u in node_set and v in node_set:
            sub_edges.append({
                "source": u, "target": v,
                "edge_type": data.get("edge_type", ""),
                "confidence": data.get("confidence", ""),
            })

    return {
        "status": "ok",
        "center_node": node_id,
        "depth": depth,
        "node_count": len(sub_nodes),
        "edge_count": len(sub_edges),
        "nodes": sub_nodes,
        "edges": sub_edges,
    }


# ──────────────────────────────────────────────────────────────────
# Flow tracing
# ──────────────────────────────────────────────────────────────────

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
    nbrs = set(G.successors(node_id)) | set(G.predecessors(node_id))
    return [n for n in nbrs if G.nodes[n].get("node_type") == target_type]


def trace_flow(
    graphml_path: Path,
    node_id: str,
    depth: int = 8,
) -> dict:
    """
    Trace the O2C business flow from any starting node.
    Walks outward through the flow chain in both directions.
    """
    G = _load_graph(graphml_path)
    if node_id not in G:
        return {"status": "not_found", "node_id": node_id}

    start_type = G.nodes[node_id].get("node_type", "")
    flow: dict[str, list[dict]] = {t: [] for t in _FLOW_CHAIN}

    # Seed the starting node
    if start_type in flow:
        flow[start_type].append({"node_id": node_id, "label": G.nodes[node_id].get("label", "")})

    # Walk the chain from the start type position
    chain_idx = _FLOW_CHAIN.index(start_type) if start_type in _FLOW_CHAIN else -1

    # Expand from known nodes to find connected nodes of each type
    all_reached = {node_id}
    frontier = {node_id}

    for _ in range(depth):
        next_frontier: set[str] = set()
        for n in frontier:
            for nbr in set(G.successors(n)) | set(G.predecessors(n)):
                if nbr not in all_reached:
                    all_reached.add(nbr)
                    next_frontier.add(nbr)
                    nbr_type = G.nodes[nbr].get("node_type", "")
                    if nbr_type in flow:
                        flow[nbr_type].append({
                            "node_id": nbr,
                            "label": G.nodes[nbr].get("label", ""),
                        })
        if not next_frontier:
            break
        frontier = next_frontier

    # Deduplicate
    for k in flow:
        seen = set()
        deduped = []
        for item in flow[k]:
            if item["node_id"] not in seen:
                seen.add(item["node_id"])
                deduped.append(item)
        flow[k] = deduped

    # Build path summary
    path_steps = []
    for step in _FLOW_CHAIN:
        path_steps.append({
            "entity_type": step,
            "count": len(flow[step]),
            "node_ids": [n["node_id"] for n in flow[step]],
        })

    gaps = [s for s in _FLOW_CHAIN if not flow[s]]

    return {
        "status": "ok",
        "start_node": node_id,
        "start_type": start_type,
        "flow_steps": path_steps,
        "gaps": gaps,
        "total_nodes_reached": len(all_reached),
    }


# ──────────────────────────────────────────────────────────────────
# Relationship analysis
# ──────────────────────────────────────────────────────────────────

def relationship_summary(graphml_path: Path) -> dict:
    """Summarise edge types, counts, and uncertainty."""
    G = _load_graph(graphml_path)
    edge_counts: dict[str, dict] = {}
    for _, _, data in G.edges(data=True):
        etype = data.get("edge_type", "unknown")
        if etype not in edge_counts:
            edge_counts[etype] = {"count": 0, "uncertain": 0}
        edge_counts[etype]["count"] += 1
        if data.get("confidence") == "uncertain":
            edge_counts[etype]["uncertain"] += 1

    records = [
        {"edge_type": k, "count": v["count"], "uncertain_count": v["uncertain"]}
        for k, v in sorted(edge_counts.items(), key=lambda x: -x[1]["count"])
    ]
    return {
        "total_edge_types": len(edge_counts),
        "total_edges": sum(v["count"] for v in edge_counts.values()),
        "total_uncertain": sum(v["uncertain"] for v in edge_counts.values()),
        "records": records,
    }


def top_degree_nodes(
    graphml_path: Path,
    limit: int = 10,
    node_type: str | None = None,
) -> list[dict]:
    """Return nodes with highest degree (in+out)."""
    G = _load_graph(graphml_path)
    nodes = []
    for nid, deg in G.degree():
        data = G.nodes[nid]
        if node_type and data.get("node_type") != node_type:
            continue
        nodes.append({
            "node_id": nid,
            "degree": deg,
            "node_type": data.get("node_type", ""),
            "label": data.get("label", ""),
        })
    nodes.sort(key=lambda x: -x["degree"])
    return nodes[:limit]


def uncertain_edges(graphml_path: Path) -> list[dict]:
    """Return all uncertain edges with metadata."""
    G = _load_graph(graphml_path)
    results = []
    for src, tgt, data in G.edges(data=True):
        if data.get("confidence") == "uncertain":
            results.append({
                "source": src, "target": tgt,
                "edge_type": data.get("edge_type", ""),
                "note": data.get("note", ""),
            })
    return results
