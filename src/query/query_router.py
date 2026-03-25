"""
query_router.py — Routes a QueryRequest to the appropriate handler.

Decision logic:
  aggregation     → sql_queries.run_aggregation
  flow_trace      → graph_queries.trace_flow
  neighborhood    → graph_queries.neighborhood
  integrity_check → anomaly_checks.run_integrity_check
  relationship    → graph_queries.relationship_summary / top_degree / uncertain
"""

from __future__ import annotations

from pathlib import Path

from query_models import QueryRequest, QueryResponse
from sql_queries import run_aggregation, list_available_metrics
from graph_queries import (
    resolve_node_id,
    trace_flow,
    neighborhood,
    relationship_summary,
    top_degree_nodes,
    uncertain_edges,
)
from anomaly_checks import run_integrity_check, list_available_checks
from result_formatter import (
    format_aggregation,
    format_flow_trace,
    format_neighborhood,
    format_integrity_check,
    format_relationship,
    format_error,
)


class QueryEngine:
    """
    Deterministic query engine.

    Accepts a QueryRequest, validates it, routes it to the right handler,
    and returns a QueryResponse.
    """

    def __init__(self, db_path: Path, graphml_path: Path):
        self.db_path = db_path
        self.graphml_path = graphml_path

    def execute(self, req: QueryRequest) -> QueryResponse:
        """Route and execute a query."""
        errors = req.validate()
        if errors:
            return format_error(req.query_type, "; ".join(errors))

        try:
            handler = {
                "aggregation": self._handle_aggregation,
                "flow_trace": self._handle_flow_trace,
                "neighborhood": self._handle_neighborhood,
                "integrity_check": self._handle_integrity_check,
                "relationship": self._handle_relationship,
            }.get(req.query_type)

            if handler is None:
                return format_error(req.query_type, f"No handler for '{req.query_type}'")

            return handler(req)

        except KeyError as exc:
            return format_error(req.query_type, str(exc))
        except Exception as exc:
            return format_error(req.query_type, f"Unexpected error: {exc}")

    # ──────────────────────────────────────────────────────────
    # Handlers
    # ──────────────────────────────────────────────────────────

    def _handle_aggregation(self, req: QueryRequest) -> QueryResponse:
        records, label = run_aggregation(
            self.db_path,
            metric=req.metric,
            order=req.order_by,
            limit=req.limit,
        )
        return format_aggregation(req, records, label)

    def _handle_flow_trace(self, req: QueryRequest) -> QueryResponse:
        # Resolve node ID
        node_id = req.entity_id
        if req.entity_type and "::" not in node_id:
            resolved = resolve_node_id(
                __import__("graph_queries")._load_graph(self.graphml_path),
                req.entity_id,
                req.entity_type,
            )
            if resolved:
                node_id = resolved

        result = trace_flow(self.graphml_path, node_id, depth=req.depth)
        return format_flow_trace(req, result)

    def _handle_neighborhood(self, req: QueryRequest) -> QueryResponse:
        node_id = req.entity_id
        if req.entity_type and "::" not in node_id:
            resolved = resolve_node_id(
                __import__("graph_queries")._load_graph(self.graphml_path),
                req.entity_id,
                req.entity_type,
            )
            if resolved:
                node_id = resolved

        result = neighborhood(self.graphml_path, node_id, depth=req.depth)
        return format_neighborhood(req, result)

    def _handle_integrity_check(self, req: QueryRequest) -> QueryResponse:
        records, label = run_integrity_check(
            req.check_type, self.db_path, self.graphml_path,
        )
        return format_integrity_check(req, records, label)

    def _handle_relationship(self, req: QueryRequest) -> QueryResponse:
        metric = req.metric or "summary"

        if metric == "summary":
            result = relationship_summary(self.graphml_path)
            return format_relationship(
                req, result["records"],
                f"Relationship summary: {result['total_edge_types']} types, "
                f"{result['total_edges']} edges, {result['total_uncertain']} uncertain",
                extra_meta={
                    "total_edge_types": result["total_edge_types"],
                    "total_edges": result["total_edges"],
                    "total_uncertain": result["total_uncertain"],
                },
            )

        elif metric == "top_degree":
            nodes = top_degree_nodes(
                self.graphml_path,
                limit=req.limit,
                node_type=req.entity_type,
            )
            return format_relationship(
                req, nodes,
                f"Top {req.limit} nodes by degree"
                + (f" (type={req.entity_type})" if req.entity_type else ""),
            )

        elif metric == "uncertain":
            edges = uncertain_edges(self.graphml_path)
            # Aggregate by edge_type for readable output
            type_counts: dict[str, int] = {}
            for e in edges:
                et = e.get("edge_type", "unknown")
                type_counts[et] = type_counts.get(et, 0) + 1
            aggregated = [
                {"edge_type": et, "count": cnt}
                for et, cnt in sorted(type_counts.items(), key=lambda x: -x[1])
            ]
            return format_relationship(
                req, aggregated,
                f"Uncertain relationships ({len(edges)} total)",
                extra_meta={"total_uncertain": len(edges)},
            )

        else:
            return format_error("relationship", f"Unknown relationship metric '{metric}'")

    # ──────────────────────────────────────────────────────────
    # Discovery helpers (for Phase 4 LLM)
    # ──────────────────────────────────────────────────────────

    def available_metrics(self) -> list[dict]:
        return list_available_metrics()

    def available_checks(self) -> list[dict]:
        return list_available_checks()
