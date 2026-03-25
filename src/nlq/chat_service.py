"""
chat_service.py — Full NL→Query→Response pipeline.

Orchestrates:
  user message → pre-guardrail → LLM classify → parse → post-guardrail
  → QueryEngine.execute → summarize → final answer

This is the single entry point that Phase 5 / a UI would call.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Any

# Add parent dirs to path so we can import query engine modules
_QUERY_DIR = str(Path(__file__).resolve().parent.parent / "query")
if _QUERY_DIR not in sys.path:
    sys.path.insert(0, _QUERY_DIR)

from src.nlq.intent_classifier import classify, LLMProvider, OfflineProvider, GeminiProvider, GroqProvider
from src.nlq.query_parser import parse_llm_output, ParseResult
from src.nlq.guardrails import pre_check, post_check
from src.nlq.response_summarizer import summarize

# Phase 3 imports
from src.query.query_models import QueryRequest
from src.query.query_router import QueryEngine


class ChatService:
    """
    End-to-end NL query service.

    Usage:
        svc = ChatService(db_path, graphml_path)
        result = svc.ask("Which customers have the most sales orders?")
        print(result["answer"])
    """

    def __init__(
        self,
        db_path: Path,
        graphml_path: Path,
        provider: LLMProvider | None = None,
    ):
        self.engine = QueryEngine(db_path, graphml_path)
        self.provider = provider

    def ask(self, user_question: str) -> dict[str, Any]:
        """
        Process a natural language question end-to-end.

        Returns a dict with:
          - question: original input
          - action: "answer" | "reject" | "clarify" | "error"
          - answer: human-readable response
          - structured_query: the JSON query dict (if action=answer)
          - raw_response: the QueryResponse dict (if action=answer)
          - llm_raw: raw LLM output string
        """
        result: dict[str, Any] = {
            "question": user_question,
            "action": None,
            "answer": None,
            "structured_query": None,
            "raw_response": None,
            "llm_raw": None,
        }

        # ── Stage 1: Pre-guardrail ─────────────────────────────
        pre = pre_check(user_question)
        if pre is not None:
            result["action"] = "reject"
            result["answer"] = pre.message
            return result

        # ── Stage 2: LLM classify ──────────────────────────────
        raw_llm = classify(user_question, self.provider)
        result["llm_raw"] = raw_llm

        # ── Stage 3: Parse + validate ──────────────────────────
        parsed = parse_llm_output(raw_llm)

        # ── Stage 4: Post-guardrail ────────────────────────────
        parsed = post_check(parsed)

        if parsed.action == "reject":
            result["action"] = "reject"
            result["answer"] = (
                parsed.message or
                "This question is outside the scope of the SAP O2C query system. "
                "I can help with aggregations, flow tracing, integrity checks, "
                "and relationship analysis."
            )
            return result

        if parsed.action == "clarify":
            result["action"] = "clarify"
            result["answer"] = parsed.message
            return result

        if parsed.action == "error":
            result["action"] = "error"
            result["answer"] = f"Could not process your question: {parsed.message}"
            return result

        # ── Stage 5: Execute query ─────────────────────────────
        assert parsed.query_dict is not None
        result["structured_query"] = parsed.query_dict

        # Build QueryRequest from the parsed dict
        req = QueryRequest.from_dict(parsed.query_dict)
        resp = self.engine.execute(req)
        resp_dict = resp.to_dict()
        result["raw_response"] = resp_dict

        # ── Stage 6: Summarize ─────────────────────────────────
        result["action"] = "answer"
        result["answer"] = summarize(resp_dict)

        return result


def create_service(
    db_path: Path | None = None,
    graphml_path: Path | None = None,
    provider_name: str = "offline",
) -> ChatService:
    """
    Helper to create a ChatService with default paths.

    provider_name: "gemini" | "groq" | "offline" (default)
    """
    from src.config import DB_PATH, GRAPH_PATH
    
    db = db_path or Path(DB_PATH)
    gml = graphml_path or Path(GRAPH_PATH)

    providers = {
        "gemini": GeminiProvider,
        "groq": GroqProvider,
    }
    provider = providers.get(provider_name, lambda: OfflineProvider())()
    return ChatService(db, gml, provider=provider)
