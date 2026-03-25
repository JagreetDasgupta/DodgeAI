"""
main.py — Phase 4 demo: NL → structured query → answer.

Runs a set of test cases covering all query types, rejections,
and clarifications. Supports offline (rule-based), Gemini, and Groq modes.

Usage:
    cd "c:\\Users\\jagre\\OneDrive\\Desktop\\Dodge AI"

    # Offline mode (no API key needed):
    python src/nlq/main.py

    # Gemini mode (requires GEMINI_API_KEY):
    python src/nlq/main.py --gemini

    # Groq mode (requires GROQ_API_KEY):
    python src/nlq/main.py --groq
"""

import json
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from chat_service import create_service

# ──────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output" / "nlq"

# ──────────────────────────────────────────────────────────────────
# Test cases
# ──────────────────────────────────────────────────────────────────
TEST_CASES = [
    # ── Aggregation ───────────────────────────────────────────
    {
        "label": "Aggregation: customer sales orders",
        "question": "Which customers have the most sales orders?",
        "expected_action": "answer",
    },
    {
        "label": "Aggregation: product billing",
        "question": "Which products are associated with the highest number of billing documents?",
        "expected_action": "answer",
    },
    {
        "label": "Aggregation: plant products",
        "question": "Which plants have the most products?",
        "expected_action": "answer",
    },

    # ── Flow trace ────────────────────────────────────────────
    {
        "label": "Flow trace: sales order",
        "question": "Trace the full flow of sales order 740506",
        "expected_action": "answer",
    },
    {
        "label": "Flow trace: missing ID",
        "question": "Trace the flow of a sales order",
        "expected_action": "clarify",
    },

    # ── Neighborhood ──────────────────────────────────────────
    {
        "label": "Neighborhood: business partner",
        "question": "Show all nodes connected to business partner 320000083 within 2 hops",
        "expected_action": "answer",
    },

    # ── Integrity checks ─────────────────────────────────────
    {
        "label": "Integrity: delivered not billed",
        "question": "Find deliveries that were not billed",
        "expected_action": "answer",
    },
    {
        "label": "Integrity: incomplete flows",
        "question": "Show me incomplete O2C flows",
        "expected_action": "answer",
    },
    {
        "label": "Integrity: unlinked payments",
        "question": "Are there any payments not linked to journal entries?",
        "expected_action": "answer",
    },

    # ── Relationship ──────────────────────────────────────────
    {
        "label": "Relationship: uncertain edges",
        "question": "Which relationships are uncertain in the graph?",
        "expected_action": "answer",
    },
    {
        "label": "Relationship: top degree",
        "question": "Which nodes have the highest degree in the graph?",
        "expected_action": "answer",
    },

    # ── Rejections ────────────────────────────────────────────
    {
        "label": "Reject: joke",
        "question": "Tell me a joke about SAP",
        "expected_action": "reject",
    },
    {
        "label": "Reject: recipe",
        "question": "Give me a recipe for pasta",
        "expected_action": "reject",
    },
    {
        "label": "Reject: code generation",
        "question": "Write code to sort a list in Python",
        "expected_action": "reject",
    },
    {
        "label": "Reject: too short",
        "question": "hi",
        "expected_action": "reject",
    },
]


def _print_result(label: str, result: dict):
    print(f"\n{'─' * 72}")
    print(f"  {label}")
    print(f"  Q: {result['question']}")
    print(f"{'─' * 72}")
    print(f"  Action: {result['action']}")
    if result.get("structured_query"):
        print(f"  Query:  {json.dumps(result['structured_query'], indent=None)}")
    print(f"  Answer:")
    for line in (result.get("answer") or "").split("\n"):
        print(f"    {line}")


def main():
    if "--groq" in sys.argv:
        provider_name = "groq"
    elif "--gemini" in sys.argv:
        provider_name = "gemini"
    else:
        provider_name = "offline"

    mode_label = provider_name.upper()

    print("=" * 72)
    print(f"PHASE 4  —  NL QUERY LAYER  —  DEMO  ({mode_label})")
    print("=" * 72)

    svc = create_service(provider_name=provider_name)
    results: list[dict] = []
    pass_count = 0

    for tc in TEST_CASES:
        result = svc.ask(tc["question"])
        _print_result(tc["label"], result)

        match = result["action"] == tc["expected_action"]
        if match:
            pass_count += 1
            print(f"  ✅ PASS (expected: {tc['expected_action']})")
        else:
            print(f"  ❌ FAIL (expected: {tc['expected_action']}, got: {result['action']})")

        results.append({
            "label": tc["label"],
            "question": tc["question"],
            "expected_action": tc["expected_action"],
            "actual_action": result["action"],
            "pass": match,
            "structured_query": result.get("structured_query"),
            "answer": result.get("answer"),
            "llm_raw": result.get("llm_raw"),
        })

    # ── Save results ──────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "demo_results.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)

    # ── Summary ───────────────────────────────────────────────
    total = len(results)
    fail_count = total - pass_count

    print(f"\n{'=' * 72}")
    print(f"PHASE 4 DEMO COMPLETE")
    print(f"{'=' * 72}")
    print(f"  Test cases:  {total}")
    print(f"  PASS:        {pass_count}")
    print(f"  FAIL:        {fail_count}")
    print(f"  Mode:        {mode_label}")
    print(f"  Results:     {report_path}")
    print()


if __name__ == "__main__":
    main()
