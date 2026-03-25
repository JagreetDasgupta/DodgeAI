"""
routes_meta.py — Health, schema, and supported-queries endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter
from models import HealthResponse, SupportedQueriesResponse
from src.api.dependency import get_schema_summary
from src.config import DB_PATH, GRAPH_PATH
import os

router = APIRouter(tags=["Meta"])


@router.get("/health", response_model=HealthResponse)
async def health():
    """Health check."""
    return HealthResponse(
        status="ok",
        phase="5",
        components={
            "database": os.path.exists(DB_PATH),
            "graph": os.path.exists(GRAPH_PATH),
        },
    )


@router.get("/schema")
async def schema():
    """Return the full schema summary from Phase 1."""
    return get_schema_summary()


@router.get("/supported-queries", response_model=SupportedQueriesResponse)
async def supported_queries():
    """Return all supported query types, metrics, checks, and entities."""
    from llm_prompting import (
        SUPPORTED_METRICS,
        SUPPORTED_INTEGRITY_CHECKS,
        SUPPORTED_RELATIONSHIP_METRICS,
        SUPPORTED_ENTITY_TYPES,
    )
    from src.query.sql_queries import list_available_metrics
    from src.query.anomaly_checks import list_available_checks

    return SupportedQueriesResponse(
        query_types=[
            {
                "type": "aggregation",
                "description": "SQL-style aggregations with joins across domains",
                "example": "Which customers have the most sales orders?"
            },
            {
                "type": "flow_trace",
                "description": "Graph BFS to trace related order-to-cash records",
                "example": "Trace the flow of sales order 740506"
            },
            {
                "type": "neighborhood",
                "description": "Extract the ego-subgraph around any node",
                "example": "Show me nodes connected to business partner 320000083"
            },
            {
                "type": "integrity_check",
                "description": "Cross-table anomaly detection combining SQL and graph",
                "example": "Find deliveries that were not billed"
            },
            {
                "type": "relationship",
                "description": "Query graph edge topology and uncertain connections",
                "example": "Which relationships are uncertain in the graph?"
            }
        ],
        metrics=list_available_metrics(),
        integrity_checks=list_available_checks(),
        relationship_metrics=SUPPORTED_RELATIONSHIP_METRICS,
        entity_types=SUPPORTED_ENTITY_TYPES,
    )
