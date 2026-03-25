"""
guardrails.py — Pre-LLM and post-LLM guardrails.

Pre-LLM:  Fast reject of obviously off-topic input before calling the LLM.
Post-LLM: Sanity-check the parsed result before executing the query.
"""

from __future__ import annotations

import re
from src.nlq.query_parser import ParseResult


# ──────────────────────────────────────────────────────────────────
# Pre-LLM guardrails
# ──────────────────────────────────────────────────────────────────

_OOD_PATTERNS = [
    r"\b(write|generate|create)\s+(a\s+)?(poem|story|essay|song|code|script|program)\b",
    r"\b(joke|riddle|fun fact|trivia)\b",
    r"\b(recipe|cook|ingredient)\b",
    r"\b(weather|forecast|temperature)\b",
    r"\b(translate|translation)\b",
    r"\b(personal|relationship|dating|love)\s*(advice|tips)?\b",
    r"\bmeaning\s+of\s+life\b",
    r"\b(who|what)\s+(is|are|was|were)\s+(the\s+)?(president|capital|tallest|fastest)\b",
    r"\b(play|game|chess|tic.tac)\b",
]

_OOD_RE = [re.compile(p, re.IGNORECASE) for p in _OOD_PATTERNS]


def pre_check(user_input: str) -> ParseResult | None:
    """
    Fast-reject obviously off-topic inputs BEFORE calling the LLM.

    Returns a ParseResult with action="reject" if blocked, else None.
    """
    text = user_input.strip()

    # Too short
    if len(text) < 3:
        return ParseResult(
            action="reject",
            reason="too_short",
            message=(
                "❌ Please provide a more specific question about the SAP O2C dataset.\n\n"
                "Try asking:\n"
                "• \"Which customers have the most sales orders?\"\n"
                "• \"Trace the flow of sales order 740506\"\n"
                "• \"Find deliveries that were not billed\""
            ),
        )

    # Too long (likely pasted code / essay)
    if len(text) > 2000:
        return ParseResult(
            action="reject",
            reason="too_long",
            message="Your message is too long. Please ask a concise question about the SAP O2C dataset.",
        )

    # Regex OOD patterns
    for pat in _OOD_RE:
        if pat.search(text):
            return ParseResult(
                action="reject",
                reason="out_of_domain",
                message=(
                    "❌ Out of scope.\n\n"
                    "This system only supports:\n"
                    "• Sales orders, deliveries, billing, payments\n"
                    "• Aggregations, flow tracing, integrity checks, graph relationships\n\n"
                    "Try asking:\n"
                    "• \"Which customers have the most orders?\"\n"
                    "• \"Trace sales order 740506\"\n"
                    "• \"Find deliveries that were not billed\""
                ),
            )

    return None  # Passed


# ──────────────────────────────────────────────────────────────────
# Post-LLM guardrails
# ──────────────────────────────────────────────────────────────────

def post_check(result: ParseResult) -> ParseResult:
    """
    Sanity-check the parsed LLM result before execution.

    Currently checks:
      - entity_id is not suspiciously long or obviously fabricated
      - depth is reasonable (1–10)
      - limit is reasonable (1–100)
    """
    if result.action != "query" or result.query_dict is None:
        return result

    d = result.query_dict

    # Check entity_id sanity
    eid = d.get("entity_id")
    if eid is not None:
        if len(str(eid)) > 50:
            return ParseResult(
                action="error",
                message=f"Entity ID '{eid[:20]}...' looks suspiciously long. Are you sure this is correct?",
            )

    # Clamp depth
    depth = d.get("depth")
    if depth is not None:
        d["depth"] = max(1, min(int(depth), 10))

    # Clamp limit
    limit = d.get("limit")
    if limit is not None:
        d["limit"] = max(1, min(int(limit), 100))

    return result
