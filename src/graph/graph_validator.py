"""
graph_validator.py — Validates graph integrity and completeness.

Checks:
  • Node / edge counts per type
  • Connected components
  • Uncertain-edge count
  • FK relationship coverage
  • Disconnected / orphan nodes
  • Broken or missing links
"""

from __future__ import annotations

import json
import networkx as nx
from pathlib import Path
from typing import Any

from src.graph.graph_loader import load_schema
from graph_traversal import (
    find_disconnected_nodes,
    find_disconnected_by_type,
    find_incomplete_flows,
)


def validate_graph(
    G: nx.DiGraph,
    schema_path: Path,
) -> dict[str, Any]:
    """
    Run all validation checks and return a structured report.
    """
    schema = load_schema(schema_path)
    report: dict[str, Any] = {}

    # ── 1. Counts ──────────────────────────────────────────────
    node_counts: dict[str, int] = {}
    for _, data in G.nodes(data=True):
        t = data.get("node_type", "unknown")
        node_counts[t] = node_counts.get(t, 0) + 1

    edge_counts: dict[str, int] = {}
    uncertain_edges = 0
    for _, _, data in G.edges(data=True):
        t = data.get("edge_type", "unknown")
        edge_counts[t] = edge_counts.get(t, 0) + 1
        if data.get("confidence") == "uncertain":
            uncertain_edges += 1

    report["node_count"] = G.number_of_nodes()
    report["edge_count"] = G.number_of_edges()
    report["node_counts_by_type"] = node_counts
    report["edge_counts_by_type"] = edge_counts
    report["uncertain_edges"] = uncertain_edges

    # ── 2. Connected components ────────────────────────────────
    comps = list(nx.weakly_connected_components(G))
    report["connected_components"] = len(comps)
    report["largest_component_size"] = max(len(c) for c in comps) if comps else 0
    report["smallest_component_size"] = min(len(c) for c in comps) if comps else 0

    # ── 3. FK coverage ─────────────────────────────────────────
    expected_fk_types = set()
    for entity in schema["entities"]:
        for fk in entity.get("foreign_keys", []):
            expected_fk_types.add(
                f"{entity['table_name']}->{fk['references_table']}"
            )
    covered = set(edge_counts.keys()) & expected_fk_types
    missing = expected_fk_types - covered

    report["expected_fk_relationships"] = len(expected_fk_types)
    report["covered_fk_relationships"] = len(covered)
    report["missing_fk_relationships"] = sorted(missing)

    # ── 4. Disconnected nodes ──────────────────────────────────
    disconnected = find_disconnected_nodes(G)
    disconnected_by_type = find_disconnected_by_type(G)
    report["disconnected_node_count"] = len(disconnected)
    report["disconnected_by_type"] = {
        k: len(v) for k, v in disconnected_by_type.items()
    }

    # ── 5. Incomplete O2C flows ────────────────────────────────
    incomplete = find_incomplete_flows(G)
    report["incomplete_flow_count"] = len(incomplete)
    report["incomplete_flow_sample"] = incomplete[:5]

    return report


def print_validation_report(report: dict) -> None:
    """Pretty-print the validation report."""
    print("\n" + "=" * 72)
    print("GRAPH VALIDATION REPORT")
    print("=" * 72)

    print(f"\n  Nodes: {report['node_count']:,}")
    print(f"  Edges: {report['edge_count']:,}")
    print(f"  Uncertain edges: {report['uncertain_edges']}")
    print(f"  Connected components: {report['connected_components']}")
    print(f"  Largest component: {report['largest_component_size']:,} nodes")

    print(f"\n  FK coverage: {report['covered_fk_relationships']}"
          f" / {report['expected_fk_relationships']}")
    if report["missing_fk_relationships"]:
        for m in report["missing_fk_relationships"]:
            print(f"    MISSING: {m}")

    print(f"\n  Disconnected nodes: {report['disconnected_node_count']}")
    for ntype, cnt in report.get("disconnected_by_type", {}).items():
        print(f"    {ntype}: {cnt}")

    print(f"\n  Incomplete O2C flows: {report['incomplete_flow_count']} / "
          f"{report['node_counts_by_type'].get('sales_order_headers', 0)} orders")
    for ic in report.get("incomplete_flow_sample", [])[:3]:
        print(f"    {ic['order_node']}")
        print(f"      missing: {', '.join(ic['missing_steps'])}")

    print("\n  Node counts by type:")
    for t in sorted(report["node_counts_by_type"]):
        print(f"    {t}: {report['node_counts_by_type'][t]}")

    print("\n  Edge counts by type:")
    for t in sorted(report["edge_counts_by_type"]):
        cnt = report["edge_counts_by_type"][t]
        print(f"    {t}: {cnt}")

    print("\n" + "=" * 72)
