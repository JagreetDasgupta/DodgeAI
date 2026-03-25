"""
query_models.py — Typed request / response dataclasses for the query engine.

Every query enters as a QueryRequest and exits as a QueryResponse.
These are plain dataclasses serialisable to JSON for Phase 4 LLM consumption.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Literal
import json


# ──────────────────────────────────────────────────────────────────
# Request
# ──────────────────────────────────────────────────────────────────

VALID_QUERY_TYPES = (
    "aggregation",
    "flow_trace",
    "neighborhood",
    "integrity_check",
    "relationship",
)

@dataclass
class QueryRequest:
    """Normalised query object accepted by the router."""
    query_type: str                               # one of VALID_QUERY_TYPES
    entity_type: str | None = None                # e.g. "sales_order_headers"
    entity_id: str | None = None                  # PK value or node ID
    metric: str | None = None                     # for aggregation queries
    check_type: str | None = None                 # for integrity checks
    order_by: str = "desc"                        # "asc" or "desc"
    limit: int = 10
    depth: int = 2                                # for neighborhood / flow
    filters: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> list[str]:
        """Return a list of validation errors (empty = valid)."""
        errors: list[str] = []
        if self.query_type not in VALID_QUERY_TYPES:
            errors.append(
                f"Invalid query_type '{self.query_type}'. "
                f"Must be one of {VALID_QUERY_TYPES}"
            )
        if self.query_type == "aggregation" and not self.metric:
            errors.append("Aggregation queries require a 'metric' field.")
        if self.query_type == "flow_trace" and not self.entity_id:
            errors.append("Flow-trace queries require an 'entity_id'.")
        if self.query_type == "neighborhood" and not self.entity_id:
            errors.append("Neighborhood queries require an 'entity_id'.")
        if self.query_type == "integrity_check" and not self.check_type:
            errors.append("Integrity-check queries require a 'check_type'.")
        return errors

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "QueryRequest":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ──────────────────────────────────────────────────────────────────
# Response
# ──────────────────────────────────────────────────────────────────

@dataclass
class QueryResponse:
    """Structured result returned by every query handler."""
    query_type: str
    status: Literal["ok", "error", "no_results"] = "ok"
    message: str = ""
    total_count: int = 0
    records: list[dict[str, Any]] = field(default_factory=list)
    paths: list[list[str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)
