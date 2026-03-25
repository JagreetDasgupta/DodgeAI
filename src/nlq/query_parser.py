"""
query_parser.py — Strict JSON parse + validation of LLM output.

Takes the raw LLM output string and attempts to:
  1. Extract valid JSON from it (handles markdown fences, trailing text, etc.)
  2. Classify the action as: query, reject, or clarify
  3. Validate the query against supported values
  4. Return a ParseResult with either a valid QueryRequest or an error/message
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from llm_prompting import (
    SUPPORTED_ENTITY_TYPES,
    SUPPORTED_METRICS,
    SUPPORTED_INTEGRITY_CHECKS,
    SUPPORTED_RELATIONSHIP_METRICS,
)


@dataclass
class ParseResult:
    """Result of parsing the LLM output."""
    action: str            # "query" | "reject" | "clarify" | "error"
    query_dict: dict | None = None
    message: str = ""
    reason: str = ""


# ──────────────────────────────────────────────────────────────────
# JSON extraction
# ──────────────────────────────────────────────────────────────────

def _extract_json(raw: str) -> dict | None:
    """
    Try to extract a JSON object from the LLM response.
    Handles:
      - raw JSON
      - JSON inside ```json ... ``` fences
      - JSON mixed with trailing prose
    """
    raw = raw.strip()

    # Remove markdown code fences
    fence_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?\s*```', raw, re.DOTALL)
    if fence_match:
        raw = fence_match.group(1).strip()

    # Try direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try to find the first { ... } block
    brace_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', raw, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group())
        except json.JSONDecodeError:
            pass

    return None


# ──────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────

VALID_QUERY_TYPES = {"aggregation", "flow_trace", "neighborhood", "integrity_check", "relationship"}


def _validate_query(d: dict) -> list[str]:
    """Validate a query dict against the supported schema.  Returns errors."""
    errors: list[str] = []

    qt = d.get("query_type")
    if qt not in VALID_QUERY_TYPES:
        errors.append(f"Invalid query_type '{qt}'")
        return errors

    if qt == "aggregation":
        metric = d.get("metric")
        if not metric:
            errors.append("Missing 'metric' for aggregation query")
        elif metric not in SUPPORTED_METRICS:
            errors.append(f"Unknown metric '{metric}'. Supported: {SUPPORTED_METRICS}")

    elif qt == "flow_trace":
        if not d.get("entity_id"):
            errors.append("Missing 'entity_id' for flow_trace query")

    elif qt == "neighborhood":
        if not d.get("entity_id"):
            errors.append("Missing 'entity_id' for neighborhood query")

    elif qt == "integrity_check":
        ct = d.get("check_type")
        if not ct:
            errors.append("Missing 'check_type' for integrity_check query")
        elif ct not in SUPPORTED_INTEGRITY_CHECKS:
            errors.append(f"Unknown check_type '{ct}'. Supported: {SUPPORTED_INTEGRITY_CHECKS}")

    elif qt == "relationship":
        metric = d.get("metric")
        if not metric:
            errors.append("Missing 'metric' for relationship query")
        elif metric not in SUPPORTED_RELATIONSHIP_METRICS:
            errors.append(f"Unknown relationship metric '{metric}'. Supported: {SUPPORTED_RELATIONSHIP_METRICS}")

    return errors


# ──────────────────────────────────────────────────────────────────
# Main parse function
# ──────────────────────────────────────────────────────────────────

def parse_llm_output(raw: str) -> ParseResult:
    """
    Parse and validate the raw LLM output.

    Returns a ParseResult with action:
      - "query"    → valid query_dict ready for QueryEngine
      - "reject"   → out-of-domain or unsupported
      - "clarify"  → ambiguous, message contains the clarification question
      - "error"    → parse or validation failure
    """
    d = _extract_json(raw)

    if d is None:
        return ParseResult(
            action="error",
            message=f"Failed to parse JSON from LLM output: {raw[:200]}",
        )

    # Check for reject / clarify actions
    if d.get("action") == "reject":
        return ParseResult(
            action="reject",
            reason=d.get("reason", "unknown"),
            message=d.get("message", "This question is outside the scope of the SAP O2C query system."),
        )

    if d.get("action") == "clarify":
        return ParseResult(
            action="clarify",
            message=d.get("message", "Could you please provide more details?"),
        )

    # Must be a query — validate it
    if "query_type" not in d:
        return ParseResult(
            action="error",
            message=f"LLM output has no 'query_type' or 'action': {d}",
        )

    errors = _validate_query(d)
    if errors:
        return ParseResult(
            action="error",
            message="; ".join(errors),
        )

    return ParseResult(
        action="query",
        query_dict=d,
    )
