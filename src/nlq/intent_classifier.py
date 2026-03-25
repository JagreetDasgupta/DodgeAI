"""
intent_classifier.py — LLM-based intent classification.

Calls the configured LLM provider with the constrained prompt and
returns raw JSON text. The provider is abstracted so it can be swapped
(Gemini, OpenAI, Ollama, etc.).
"""

from __future__ import annotations
import json
import os
import re
from typing import Protocol
import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

print("KEY:", os.environ.get("GEMINI_API_KEY"))
from llm_prompting import build_messages


# ──────────────────────────────────────────────────────────────────
# Abstract provider
# ──────────────────────────────────────────────────────────────────

class LLMProvider(Protocol):
    """Any callable that takes messages and returns a string."""
    def complete(self, messages: list[dict[str, str]]) -> str: ...


# ──────────────────────────────────────────────────────────────────
# Google Gemini provider (free tier)
# ──────────────────────────────────────────────────────────────────

class GeminiProvider:
    """
    Uses google-generativeai SDK with a free-tier API key.

    Set env var GEMINI_API_KEY before running.
    """

    def __init__(self, model: str = "gemini-2.0-flash"):
        self.model_name = model
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from google import genai
                api_key = os.environ.get("GEMINI_API_KEY", "")
                if not api_key:
                    raise ValueError(
                        "GEMINI_API_KEY environment variable is not set. "
                        "Get a free key at https://aistudio.google.com/apikey"
                    )
                self._client = genai.Client(api_key=api_key)
            except ImportError:
                raise ImportError(
                    "google-genai package not installed. "
                    "Run: pip install google-genai"
                )
        return self._client

    def complete(self, messages: list[dict[str, str]]) -> str:
        client = self._get_client()

        # Build contents for Gemini
        # Gemini uses a different message format — combine system + few-shot into
        # a single conversation
        system_text = ""
        contents = []

        for msg in messages:
            if msg["role"] == "system":
                system_text = msg["content"]
            elif msg["role"] == "user":
                contents.append({"role": "user", "parts": [{"text": msg["content"]}]})
            elif msg["role"] == "assistant":
                contents.append({"role": "model", "parts": [{"text": msg["content"]}]})

        from google.genai import types

        response = client.models.generate_content(
            model=self.model_name,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=system_text,
                temperature=0.0,
                max_output_tokens=512,
            ),
        )

        return response.text.strip()


# ──────────────────────────────────────────────────────────────────
# Groq provider (llama-3.3-70b-versatile)
# ──────────────────────────────────────────────────────────────────

class GroqProvider:
    """
    Uses the Groq SDK for fast inference on open-source models.

    Set env var GROQ_API_KEY before running.
    """

    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        self.model_name = model
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from groq import Groq
                api_key = os.environ.get("GROQ_API_KEY", "")
                if not api_key:
                    raise ValueError(
                        "GROQ_API_KEY environment variable is not set. "
                        "Get a free key at https://console.groq.com"
                    )
                self._client = Groq(api_key=api_key)
            except ImportError:
                raise ImportError(
                    "groq package not installed. "
                    "Run: pip install groq"
                )
        return self._client

    def complete(self, messages: list[dict[str, str]]) -> str:
        import time
        client = self._get_client()

        for attempt in range(2):
            try:
                response = client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=0.0,
                    max_tokens=512,
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                # Retry once on rate-limit errors
                if attempt == 0 and "rate_limit" in type(e).__name__.lower():
                    time.sleep(2)
                    continue
                raise


# ──────────────────────────────────────────────────────────────────
# Fallback / offline provider (rule-based, for testing without API)
# ──────────────────────────────────────────────────────────────────

class OfflineProvider:
    """
    A simple keyword-based classifier for offline / no-API testing.
    Covers the main patterns. Will NOT handle edge cases — use a real
    LLM for production.
    """

    def complete(self, messages: list[dict[str, str]]) -> str:
        user_msg = messages[-1]["content"].lower().strip()

        # Out-of-domain
        ood_keywords = [
            "joke", "poem", "story", "recipe", "weather",
            "write code", "python code", "javascript",
            "personal", "advice", "meaning of life",
            "hello", "hi there", "how are you",
        ]
        for kw in ood_keywords:
            if kw in user_msg:
                return json.dumps({"action": "reject", "reason": "out_of_domain"})

        # Integrity checks
        check_map = {
            "delivered not billed": "delivered_not_billed",
            "deliveries that were not billed": "delivered_not_billed",
            "deliveries not billed": "delivered_not_billed",
            "billed without delivery": "billed_without_delivery",
            "billing without delivery": "billed_without_delivery",
            "orders without delivery": "orders_without_delivery",
            "sales orders with no delivery": "orders_without_delivery",
            "billing without journal": "billing_without_journal",
            "billing documents with no journal": "billing_without_journal",
            "payments without journal": "payments_without_journal_link",
            "payments not linked": "payments_without_journal_link",
            "unlinked payments": "payments_without_journal_link",
            "disconnected nodes": "disconnected_nodes",
            "orphan nodes": "disconnected_nodes",
            "incomplete flow": "incomplete_o2c_flows",
            "incomplete o2c": "incomplete_o2c_flows",
            "broken flow": "incomplete_o2c_flows",
        }
        for phrase, check in check_map.items():
            if phrase in user_msg:
                return json.dumps({"query_type": "integrity_check", "check_type": check})

        # Relationship queries
        if "uncertain" in user_msg and ("relationship" in user_msg or "edge" in user_msg):
            return json.dumps({"query_type": "relationship", "metric": "uncertain"})
        if "top degree" in user_msg or "highest degree" in user_msg or "most connected" in user_msg:
            return json.dumps({"query_type": "relationship", "metric": "top_degree", "limit": 10})
        if "relationship summary" in user_msg or "edge summary" in user_msg:
            return json.dumps({"query_type": "relationship", "metric": "summary"})

        # Aggregation
        agg_map = {
            ("customer", "sales order"): "customer_sales_order_count",
            ("customer", "billing"): "customer_billing_count",
            ("customer", "payment"): "customer_payment_count",
            ("customer", "journal"): "customer_journal_entry_count",
            ("product", "billing"): "product_billing_document_count",
            ("product", "sales order"): "product_sales_order_count",
            ("billing document", "item"): "billing_document_item_count",
            ("sales order", "item"): "sales_order_item_count",
            ("delivery", "item"): "delivery_item_count",
            ("plant", "product"): "plant_product_count",
        }
        for (k1, k2), metric in agg_map.items():
            if k1 in user_msg and k2 in user_msg:
                limit = 10
                # Try to extract limit
                limit_match = re.search(r'top\s+(\d+)', user_msg)
                if limit_match:
                    limit = int(limit_match.group(1))
                return json.dumps({
                    "query_type": "aggregation",
                    "metric": metric,
                    "order_by": "asc" if "least" in user_msg or "fewest" in user_msg else "desc",
                    "limit": limit,
                })

        # Flow trace
        if "trace" in user_msg or "flow" in user_msg:
            # Try to extract entity and ID
            entity_patterns = [
                (r"sales order\s+(\d+)", "sales_order_headers"),
                (r"order\s+(\d+)", "sales_order_headers"),
                (r"billing document\s+(\d+)", "billing_document_headers"),
                (r"billing\s+(\d+)", "billing_document_headers"),
                (r"delivery\s+(\d+)", "outbound_delivery_headers"),
            ]
            for pattern, entity in entity_patterns:
                m = re.search(pattern, user_msg)
                if m:
                    return json.dumps({
                        "query_type": "flow_trace",
                        "entity_type": entity,
                        "entity_id": m.group(1),
                        "depth": 8,
                    })
            # No ID found
            return json.dumps({
                "action": "clarify",
                "message": "Which entity would you like to trace? Please provide the entity type and ID (e.g., 'sales order 740506').",
            })

        # Neighborhood
        if "neighbor" in user_msg or "around" in user_msg or "connected to" in user_msg or "subgraph" in user_msg:
            entity_patterns = [
                (r"sales order\s+(\d+)", "sales_order_headers"),
                (r"order\s+(\d+)", "sales_order_headers"),
                (r"billing document\s+(\d+)", "billing_document_headers"),
                (r"billing\s+(\d+)", "billing_document_headers"),
                (r"business partner\s+(\d+)", "business_partners"),
                (r"customer\s+(\d+)", "business_partners"),
                (r"delivery\s+(\d+)", "outbound_delivery_headers"),
                (r"product\s+(S\w+)", "products"),
            ]
            depth = 2
            depth_match = re.search(r'(\d+)\s*hops?', user_msg)
            if depth_match:
                depth = int(depth_match.group(1))
            for pattern, entity in entity_patterns:
                m = re.search(pattern, user_msg)
                if m:
                    return json.dumps({
                        "query_type": "neighborhood",
                        "entity_type": entity,
                        "entity_id": m.group(1),
                        "depth": depth,
                    })
            return json.dumps({
                "action": "clarify",
                "message": "Which entity would you like to explore? Please provide the entity type and ID.",
            })

        # Fallback
        return json.dumps({"action": "reject", "reason": "unsupported_query"})


# ──────────────────────────────────────────────────────────────────
# classify function
# ──────────────────────────────────────────────────────────────────

def classify(user_question: str, provider: LLMProvider | None = None) -> str:
    """
    Send the user question through the LLM prompt and return
    the raw response string.
    """
    if provider is None:
        provider = OfflineProvider()

    messages = build_messages(user_question)
    return provider.complete(messages)
