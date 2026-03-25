"""
main.py — Phase 2 orchestrator: build, validate, and export the O2C graph.

Usage:
    cd "c:\\Users\\jagre\\OneDrive\\Desktop\\Dodge AI"
    python src/graph/main.py
"""

import json
import sys
import os
from pathlib import Path

# Ensure src/graph/ is on sys.path
sys.path.insert(0, os.path.dirname(__file__))

from graph_builder import build_graph, get_graph_summary
from graph_validator import validate_graph, print_validation_report
from graph_exporter import export_graphml, export_json, export_csv
from graph_traversal import trace_order_flow

# ──────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "output" / "sap_o2c.db"
SCHEMA_PATH = PROJECT_ROOT / "output" / "schema_summary.json"
GRAPH_DIR = PROJECT_ROOT / "output" / "graph"


def main() -> None:
    print("=" * 72)
    print("PHASE 2  -  GRAPH CONSTRUCTION PIPELINE")
    print("=" * 72)

    # ── Stage 1: Build ────────────────────────────────────────
    print("\n>> Stage 1: Building graph from SQLite + schema ...")
    G = build_graph(DB_PATH, SCHEMA_PATH)
    summary = get_graph_summary(G)
    print(f"   Nodes: {summary['total_nodes']:,}")
    print(f"   Edges: {summary['total_edges']:,}")
    print(f"   Uncertain edges: {summary['uncertain_edges']}")
    print(f"   Weakly connected components: {summary['connected_components']}")

    # ── Stage 2: Validate ─────────────────────────────────────
    print("\n>> Stage 2: Validating graph ...")
    report = validate_graph(G, SCHEMA_PATH)
    print_validation_report(report)

    # Save validation report as JSON
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    report_path = GRAPH_DIR / "validation_report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"   Validation report saved to {report_path}")

    # ── Stage 3: Export ───────────────────────────────────────
    print("\n>> Stage 3: Exporting graph ...")
    gml_path = export_graphml(G, GRAPH_DIR)
    print(f"   GraphML:  {gml_path}")

    json_path = export_json(G, GRAPH_DIR)
    print(f"   JSON:     {json_path}")

    nodes_csv, edges_csv = export_csv(G, GRAPH_DIR)
    print(f"   Nodes CSV: {nodes_csv}")
    print(f"   Edges CSV: {edges_csv}")

    # ── Stage 4: Sample flow trace ────────────────────────────
    print("\n>> Stage 4: Sample order flow trace ...")
    # Find the first sales_order_headers node
    order_nodes = [
        nid for nid, data in G.nodes(data=True)
        if data.get("node_type") == "sales_order_headers"
    ]
    if order_nodes:
        sample_order = sorted(order_nodes)[0]
        print(f"   Tracing flow for: {sample_order}")
        flow = trace_order_flow(G, sample_order)
        for step, nodes_list in flow.items():
            status = f"{len(nodes_list)} node(s)" if nodes_list else "-- none --"
            print(f"     {step}: {status}")
    else:
        print("   No sales_order_headers nodes found.")

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("PHASE 2 PIPELINE COMPLETE")
    print("=" * 72)
    print(f"  Graph artifacts: {GRAPH_DIR}")
    print(f"  Total nodes:     {summary['total_nodes']:,}")
    print(f"  Total edges:     {summary['total_edges']:,}")
    print(f"  Uncertain edges: {summary['uncertain_edges']}")
    print()


if __name__ == "__main__":
    main()
