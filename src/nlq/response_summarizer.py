"""
response_summarizer.py — Converts QueryResponse payloads into clean NL text.

The summaries are data-grounded: every statement references the actual data
from the QueryResponse. The LLM is NOT used here — this is deterministic.
"""

from __future__ import annotations

from typing import Any

# Confidence tag appended to all data-backed responses
_CONFIDENCE_TAG = "\n\n🔍 Source: QueryEngine (data-backed)"

# Friendly labels for entity types in flow traces
_ENTITY_LABELS = {
    "sales_order_headers": "📦 Sales Orders",
    "sales_order_items": "📄 Sales Order Items",
    "outbound_delivery_items": "📋 Delivery Items",
    "outbound_delivery_headers": "🚚 Deliveries",
    "billing_document_items": "📑 Billing Items",
    "billing_document_headers": "🧾 Billing Documents",
    "journal_entry_items_ar": "📒 Journal Entries",
    "payments_accounts_receivable": "💰 Payments",
}


def summarize(response: dict) -> str:
    """
    Convert a QueryResponse (as dict) into a human-readable summary.
    """
    status = response.get("status", "error")
    qtype = response.get("query_type", "unknown")
    message = response.get("message", "")
    total = response.get("total_count", 0)
    records = response.get("records", [])
    metadata = response.get("metadata", {})

    if status == "error":
        return f"⚠ Error: {message}"

    if status == "no_results":
        return f"No results found. ({message})"

    # Dispatch by query type
    handler = _HANDLERS.get(qtype, _generic_summary)
    result = handler(message, total, records, metadata, response)
    return result + _CONFIDENCE_TAG


# ──────────────────────────────────────────────────────────────────
# Per-type summarizers
# ──────────────────────────────────────────────────────────────────

def _summarize_aggregation(msg, total, records, meta, resp):
    lines = [f"📊 **{msg}** ({total} results)"]
    for i, r in enumerate(records[:10], 1):
        # Format the key-value pairs nicely
        parts = [f"{k}={v}" for k, v in r.items() if k != "cnt"]
        count = r.get("cnt", "?")
        lines.append(f"  {i}. {', '.join(parts)} — **{count}** records")
    if total > 10:
        lines.append(f"  ... and {total - 10} more")
    return "\n".join(lines)


def _summarize_flow_trace(msg, total, records, meta, resp):
    gaps = meta.get("gaps", [])
    start = meta.get("start_node", "?")
    start_type = meta.get("start_type", "?")
    start_label = _ENTITY_LABELS.get(start_type, start_type)

    lines = [f"🔗 **Flow trace from {start_label}** (node: {start})"]
    lines.append(f"   Total nodes reached: {total}")
    lines.append("")

    for step in records:
        etype = step.get("entity_type", "?")
        label = _ENTITY_LABELS.get(etype, etype)
        count = step.get("count", 0)
        status = f"✅ {count} node(s)" if count else "❌ none"
        lines.append(f"   {label} → {status}")

    if gaps:
        gap_labels = [_ENTITY_LABELS.get(g, g) for g in gaps]
        lines.append(f"\n   ⚠ Gaps in flow: {', '.join(gap_labels)}")
    else:
        lines.append(f"\n   ✅ Complete flow — no gaps")

    return "\n".join(lines)


def _summarize_neighborhood(msg, total, records, meta, resp):
    center = meta.get("center_node", "?")
    depth = meta.get("depth", "?")
    edge_count = meta.get("edge_count", 0)

    lines = [f"🏘 **Neighborhood of {center}** (depth={depth})"]
    lines.append(f"   {total} nodes, {edge_count} edges")

    # Group by node_type
    type_counts: dict[str, int] = {}
    for r in records:
        nt = r.get("node_type", "unknown")
        type_counts[nt] = type_counts.get(nt, 0) + 1

    for nt in sorted(type_counts):
        lines.append(f"   {nt}: {type_counts[nt]}")

    return "\n".join(lines)


def _summarize_integrity(msg, total, records, meta, resp):
    check = meta.get("check_type", "?")
    capped = meta.get("capped", False)
    full_count = meta.get("full_count", total)

    if total == 0:
        return f"✅ **{msg}**: No issues found!"

    lines = [f"⚠ **{msg}**: {full_count} issue(s) found"]

    for i, r in enumerate(records[:5], 1):
        parts = [f"{k}={v}" for k, v in r.items() if k not in ("missing_steps", "reached_steps")]
        lines.append(f"  {i}. {', '.join(parts)}")
        if "missing_steps" in r:
            lines.append(f"     Missing: {', '.join(r['missing_steps'])}")

    if full_count > 5:
        lines.append(f"  ... and {full_count - 5} more")

    return "\n".join(lines)


def _summarize_relationship(msg, total, records, meta, resp):
    lines = [f"🔀 **{msg}**"]

    if "total_uncertain" in meta:
        lines.append(f"   Total uncertain edges: {meta['total_uncertain']}")
    lines.append("")

    for i, r in enumerate(records[:10], 1):
        if "edge_type" in r and "count" in r and "degree" not in r:
            # Aggregated format (uncertain edges by type, or summary)
            et = r['edge_type'].replace('_', ' ').replace('->', ' → ')
            uncertain = r.get("uncertain_count", "")
            uc_str = f" (⚠ {uncertain} uncertain)" if uncertain else ""
            lines.append(f"  {i}. {et} — **{r['count']}** edges{uc_str}")
        elif "degree" in r:
            node_type = r.get('node_type', '')
            label = r.get('label', r.get('node_id', '?'))
            lines.append(f"  {i}. [{node_type}] {label} — degree **{r['degree']}**")
        elif "source" in r:
            lines.append(f"  {i}. {r['source']} → {r['target']} ({r.get('edge_type', '?')})")
        else:
            lines.append(f"  {i}. {r}")

    if total > 10:
        lines.append(f"  ... and {total - 10} more")

    return "\n".join(lines)


def _generic_summary(msg, total, records, meta, resp):
    lines = [f"📋 **{msg}** — {total} result(s)"]
    for i, r in enumerate(records[:5], 1):
        lines.append(f"  {i}. {r}")
    return "\n".join(lines)


_HANDLERS = {
    "aggregation": _summarize_aggregation,
    "flow_trace": _summarize_flow_trace,
    "neighborhood": _summarize_neighborhood,
    "integrity_check": _summarize_integrity,
    "relationship": _summarize_relationship,
}
