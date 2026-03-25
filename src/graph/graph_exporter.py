"""
graph_exporter.py — Exports the O2C graph to multiple formats.

Exports to output/graph/:
  - sap_o2c_graph.graphml   (GraphML)
  - sap_o2c_graph.json      (JSON — nodes + edges)
  - nodes.csv / edges.csv   (flat CSV)
"""

import csv
import json
from pathlib import Path

import networkx as nx


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


# ──────────────────────────────────────────────────────────────────
# GraphML
# ──────────────────────────────────────────────────────────────────

def export_graphml(G: nx.DiGraph, output_dir: Path) -> Path:
    """Export graph as GraphML.  Returns file path."""
    out = _ensure_dir(output_dir) / "sap_o2c_graph.graphml"
    nx.write_graphml(G, str(out))
    return out


# ──────────────────────────────────────────────────────────────────
# JSON
# ──────────────────────────────────────────────────────────────────

def export_json(G: nx.DiGraph, output_dir: Path) -> Path:
    """Export graph as a JSON file with nodes and edges arrays."""
    out = _ensure_dir(output_dir) / "sap_o2c_graph.json"

    nodes = []
    for nid, data in G.nodes(data=True):
        nodes.append({"id": nid, **{k: v for k, v in data.items()}})

    edges = []
    for src, tgt, data in G.edges(data=True):
        edges.append({"source": src, "target": tgt, **{k: v for k, v in data.items()}})

    payload = {
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "nodes": nodes,
        "edges": edges,
    }

    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    return out


# ──────────────────────────────────────────────────────────────────
# CSV (node list + edge list)
# ──────────────────────────────────────────────────────────────────

def export_csv(G: nx.DiGraph, output_dir: Path) -> tuple[Path, Path]:
    """
    Export nodes.csv and edges.csv.
    Returns (nodes_path, edges_path).
    """
    d = _ensure_dir(output_dir)

    # Nodes
    nodes_path = d / "nodes.csv"
    node_fields = ["id", "node_type", "label", "source_table",
                   "pk_fields", "_source_file", "_source_row"]
    with open(nodes_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=node_fields, extrasaction="ignore")
        writer.writeheader()
        for nid, data in G.nodes(data=True):
            writer.writerow({"id": nid, **data})

    # Edges
    edges_path = d / "edges.csv"
    edge_fields = ["source", "target", "edge_type", "confidence",
                   "fk_columns", "ref_columns", "note"]
    with open(edges_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=edge_fields, extrasaction="ignore")
        writer.writeheader()
        for src, tgt, data in G.edges(data=True):
            writer.writerow({"source": src, "target": tgt, **data})

    return nodes_path, edges_path
