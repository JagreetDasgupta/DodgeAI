"""
models.py — Pydantic request/response models for the API.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Any


# ──────────────────────────────────────────────────────────────────
# Query endpoint
# ──────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000, description="Natural language question")
    provider: str = Field("offline", description="LLM provider: 'offline', 'gemini', or 'groq'")


class QueryResponse(BaseModel):
    question: str
    action: str              # answer | reject | clarify | error
    answer: str | None
    structured_query: dict | None = None
    raw_response: dict | None = None


# ──────────────────────────────────────────────────────────────────
# Graph endpoints
# ──────────────────────────────────────────────────────────────────

class NodeResponse(BaseModel):
    status: str
    node_id: str
    data: dict | None = None


class NeighborhoodResponse(BaseModel):
    status: str
    center_node: str
    depth: int
    node_count: int = 0
    edge_count: int = 0
    nodes: list[dict] = []
    edges: list[dict] = []
    visjs_data: dict | None = None


class FlowTraceResponse(BaseModel):
    status: str
    start_node: str
    depth: int = 8
    total_nodes_reached: int = 0
    flow_steps: list[dict] = []
    gaps: list[str] = []
    visjs_data: dict | None = None


class GraphMetadataResponse(BaseModel):
    total_nodes: int
    total_edges: int
    entity_types: list[str]
    edge_types: list[str]
    uncertain_edge_count: int


# ──────────────────────────────────────────────────────────────────
# Meta endpoints
# ──────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str = "ok"
    phase: str = "5"
    components: dict = {}


class SupportedQueriesResponse(BaseModel):
    query_types: list[dict]
    metrics: list[dict]
    integrity_checks: list[dict]
    relationship_metrics: list[str]
    entity_types: list[str]
