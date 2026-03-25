"""
main.py — Phase 3 orchestrator: demo / validation of the query engine.

Runs a set of representative queries and prints structured results.

Usage:
    cd "c:\\Users\\jagre\\OneDrive\\Desktop\\Dodge AI"
    python src/query/main.py
"""

import json
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from src.query.query_models import QueryRequest
from src.query.query_router import QueryEngine

# ──────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────
from src.config import DB_PATH, GRAPH_PATH

DB_PATH_OBJ = Path(DB_PATH)
GRAPHML_PATH_OBJ = Path(GRAPH_PATH)
OUTPUT_DIR = PROJECT_ROOT / "output" / "query"


def _print_response(label: str, resp):
    print(f"\n{'─' * 72}")
    print(f"  {label}")
    print(f"{'─' * 72}")
    print(f"  status: {resp.status}")
    print(f"  message: {resp.message}")
    print(f"  total_count: {resp.total_count}")
    if resp.records:
        for i, r in enumerate(resp.records[:5]):
            print(f"  [{i+1}] {r}")
        if len(resp.records) > 5:
            print(f"  ... and {len(resp.records) - 5} more")
    if resp.metadata:
        for k, v in resp.metadata.items():
            if k == "edges":
                print(f"  {k}: ({len(v)} edges, capped)")
            elif isinstance(v, list) and len(v) > 5:
                print(f"  {k}: {v[:5]} ... ({len(v)} total)")
            else:
                print(f"  {k}: {v}")


def main():
    print("=" * 72)
    print("PHASE 3  —  DETERMINISTIC QUERY ENGINE  —  DEMO")
    print("=" * 72)

    engine = QueryEngine(DB_PATH, GRAPHML_PATH)

    results: list[dict] = []

    # ── 1. Aggregation queries ────────────────────────────────
    print("\n\n>> AGGREGATION QUERIES")

    agg_queries = [
        {"query_type": "aggregation", "metric": "customer_sales_order_count", "limit": 5},
        {"query_type": "aggregation", "metric": "product_billing_document_count", "limit": 5},
        {"query_type": "aggregation", "metric": "billing_document_item_count", "limit": 5},
        {"query_type": "aggregation", "metric": "plant_product_count", "limit": 5},
    ]
    for q in agg_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Aggregation: {q['metric']}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── 2. Flow tracing ───────────────────────────────────────
    print("\n\n>> FLOW TRACING QUERIES")

    flow_queries = [
        {"query_type": "flow_trace", "entity_type": "sales_order_headers",
         "entity_id": "740506", "depth": 8},
        {"query_type": "flow_trace", "entity_type": "billing_document_headers",
         "entity_id": "90000025", "depth": 8},
    ]
    for q in flow_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Flow: {q['entity_type']}::{q['entity_id']}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── 3. Neighborhood queries ───────────────────────────────
    print("\n\n>> NEIGHBORHOOD QUERIES")

    nbr_queries = [
        {"query_type": "neighborhood", "entity_type": "billing_document_headers",
         "entity_id": "90000025", "depth": 2},
        {"query_type": "neighborhood", "entity_type": "business_partners",
         "entity_id": "100", "depth": 2},
    ]
    for q in nbr_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Neighborhood: {q['entity_type']}::{q['entity_id']}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── 4. Integrity checks ───────────────────────────────────
    print("\n\n>> INTEGRITY / ANOMALY CHECKS")

    check_queries = [
        {"query_type": "integrity_check", "check_type": "delivered_not_billed"},
        {"query_type": "integrity_check", "check_type": "payments_without_journal_link"},
        {"query_type": "integrity_check", "check_type": "orders_without_delivery"},
        {"query_type": "integrity_check", "check_type": "incomplete_o2c_flows"},
    ]
    for q in check_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Integrity: {q['check_type']}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── 5. Relationship queries ───────────────────────────────
    print("\n\n>> RELATIONSHIP QUERIES")

    rel_queries = [
        {"query_type": "relationship", "metric": "summary"},
        {"query_type": "relationship", "metric": "top_degree", "limit": 5},
        {"query_type": "relationship", "metric": "uncertain", "limit": 5},
    ]
    for q in rel_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Relationship: {q.get('metric')}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── 6. Error handling ─────────────────────────────────────
    print("\n\n>> ERROR HANDLING")

    error_queries = [
        {"query_type": "invalid_type"},
        {"query_type": "aggregation"},  # missing metric
        {"query_type": "flow_trace", "entity_id": "NONEXISTENT_NODE"},
        {"query_type": "integrity_check", "check_type": "fake_check"},
    ]
    for q in error_queries:
        req = QueryRequest.from_dict(q)
        resp = engine.execute(req)
        _print_response(f"Error test: {q}", resp)
        results.append({"query": q, "response": resp.to_dict()})

    # ── Save results ──────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "demo_results.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)

    # ── Available metrics / checks ────────────────────────────
    print("\n\n>> AVAILABLE METRICS")
    for m in engine.available_metrics():
        print(f"  {m['metric']}: {m['label']}")

    print("\n>> AVAILABLE INTEGRITY CHECKS")
    for c in engine.available_checks():
        print(f"  {c['check_type']}: {c['label']}")

    # ── Summary ───────────────────────────────────────────────
    ok_count = sum(1 for r in results if r["response"]["status"] == "ok")
    err_count = sum(1 for r in results if r["response"]["status"] == "error")
    no_res = sum(1 for r in results if r["response"]["status"] == "no_results")
    total = len(results)

    print(f"\n{'=' * 72}")
    print(f"PHASE 3 DEMO COMPLETE")
    print(f"{'=' * 72}")
    print(f"  Queries run:    {total}")
    print(f"  OK:             {ok_count}")
    print(f"  No results:     {no_res}")
    print(f"  Errors:         {err_count}")
    print(f"  Results saved:  {report_path}")
    print()


if __name__ == "__main__":
    main()
