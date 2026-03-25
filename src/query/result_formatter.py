"""
result_formatter.py — Standardised response formatting.

Wraps raw query results into QueryResponse objects with consistent
metadata, making them easy for Phase 4's LLM to consume.
"""

from __future__ import annotations

from query_models import QueryRequest, QueryResponse


def format_aggregation(
    req: QueryRequest,
    records: list[dict],
    label: str,
) -> QueryResponse:
    return QueryResponse(
        query_type="aggregation",
        status="ok" if records else "no_results",
        message=label,
        total_count=len(records),
        records=records,
        metadata={
            "metric": req.metric,
            "order_by": req.order_by,
            "limit": req.limit,
        },
    )


def format_flow_trace(
    req: QueryRequest,
    flow_result: dict,
) -> QueryResponse:
    if flow_result.get("status") == "not_found":
        return QueryResponse(
            query_type="flow_trace",
            status="error",
            message=f"Node '{req.entity_id}' not found in graph.",
        )

    # Extract node IDs along the path
    paths: list[list[str]] = []
    for step in flow_result.get("flow_steps", []):
        if step["node_ids"]:
            paths.append(step["node_ids"])

    return QueryResponse(
        query_type="flow_trace",
        status="ok",
        message=f"Flow trace from {flow_result.get('start_type', '?')} node",
        total_count=flow_result.get("total_nodes_reached", 0),
        records=flow_result.get("flow_steps", []),
        paths=paths,
        metadata={
            "start_node": flow_result.get("start_node"),
            "start_type": flow_result.get("start_type"),
            "gaps": flow_result.get("gaps", []),
        },
    )


def format_neighborhood(
    req: QueryRequest,
    nbr_result: dict,
) -> QueryResponse:
    if nbr_result.get("status") == "not_found":
        return QueryResponse(
            query_type="neighborhood",
            status="error",
            message=f"Node '{req.entity_id}' not found in graph.",
        )

    return QueryResponse(
        query_type="neighborhood",
        status="ok",
        message=f"Neighborhood of {nbr_result.get('center_node')} (depth={nbr_result.get('depth')})",
        total_count=nbr_result.get("node_count", 0),
        records=nbr_result.get("nodes", []),
        metadata={
            "center_node": nbr_result.get("center_node"),
            "depth": nbr_result.get("depth"),
            "edge_count": nbr_result.get("edge_count", 0),
            "edges": nbr_result.get("edges", [])[:50],  # cap for payload size
        },
    )


def format_integrity_check(
    req: QueryRequest,
    records: list[dict],
    label: str,
) -> QueryResponse:
    return QueryResponse(
        query_type="integrity_check",
        status="ok" if records else "no_results",
        message=label,
        total_count=len(records),
        records=records[:100],  # cap for large result sets
        metadata={
            "check_type": req.check_type,
            "capped": len(records) > 100,
            "full_count": len(records),
        },
    )


def format_relationship(
    req: QueryRequest,
    records: list[dict],
    label: str,
    extra_meta: dict | None = None,
) -> QueryResponse:
    return QueryResponse(
        query_type="relationship",
        status="ok" if records else "no_results",
        message=label,
        total_count=len(records),
        records=records,
        metadata=extra_meta or {},
    )


def format_error(query_type: str, message: str) -> QueryResponse:
    return QueryResponse(
        query_type=query_type,
        status="error",
        message=message,
    )
