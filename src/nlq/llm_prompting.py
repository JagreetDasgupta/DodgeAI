"""
llm_prompting.py — Schema-aware system prompt and few-shot examples.

Builds the constrained LLM prompt that forces structured JSON output
within the SAP O2C schema. The LLM is told:
  - exactly which query types exist
  - exactly which metrics, checks, and entity types are supported
  - exactly which JSON shapes are valid
  - to refuse anything outside scope
"""

from __future__ import annotations


# ──────────────────────────────────────────────────────────────────
# Supported schema elements (kept in sync with Phase 3)
# ──────────────────────────────────────────────────────────────────

SUPPORTED_ENTITY_TYPES = [
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "journal_entry_items_ar",
    "payments_accounts_receivable",
    "business_partners",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
    "plants",
]

SUPPORTED_METRICS = [
    "customer_sales_order_count",
    "customer_billing_count",
    "customer_payment_count",
    "customer_journal_entry_count",
    "product_billing_document_count",
    "product_sales_order_count",
    "billing_document_item_count",
    "sales_order_item_count",
    "delivery_item_count",
    "plant_product_count",
]

SUPPORTED_INTEGRITY_CHECKS = [
    "delivered_not_billed",
    "billed_without_delivery",
    "orders_without_delivery",
    "billing_without_journal",
    "payments_without_journal_link",
    "disconnected_nodes",
    "incomplete_o2c_flows",
]

SUPPORTED_RELATIONSHIP_METRICS = [
    "summary",
    "top_degree",
    "uncertain",
]


# ──────────────────────────────────────────────────────────────────
# System prompt
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = f"""You are a structured query translator for an SAP Order-to-Cash (O2C) dataset.
Your ONLY job is to convert the user's natural language question into a valid JSON query object.

RULES:
1. You must ONLY output valid JSON. No explanations, no markdown, no prose.
2. You must ONLY use the supported query types, metrics, entity types, and checks listed below.
3. You must NEVER invent entity IDs, metrics, or relationship types.
4. You must NEVER answer from general knowledge. You are not a chatbot.
5. If the question is unrelated to the SAP O2C dataset, output: {{"action": "reject", "reason": "out_of_domain"}}
6. If the question is ambiguous or missing required info, output: {{"action": "clarify", "message": "..."}}
7. If you cannot map the question to a supported query, output: {{"action": "reject", "reason": "unsupported_query"}}

SUPPORTED QUERY TYPES:

1. aggregation — requires "metric" from the list below
   {{"query_type": "aggregation", "metric": "...", "order_by": "desc", "limit": 10}}

2. flow_trace — requires "entity_type" and "entity_id"
   {{"query_type": "flow_trace", "entity_type": "...", "entity_id": "...", "depth": 8}}

3. neighborhood — requires "entity_type" and "entity_id"
   {{"query_type": "neighborhood", "entity_type": "...", "entity_id": "...", "depth": 2}}

4. integrity_check — requires "check_type" from the list below
   {{"query_type": "integrity_check", "check_type": "..."}}

5. relationship — requires "metric" from the relationship metrics below
   {{"query_type": "relationship", "metric": "..."}}

SUPPORTED METRICS (for aggregation):
{chr(10).join(f'  - {m}' for m in SUPPORTED_METRICS)}

SUPPORTED INTEGRITY CHECKS:
{chr(10).join(f'  - {c}' for c in SUPPORTED_INTEGRITY_CHECKS)}

SUPPORTED RELATIONSHIP METRICS:
{chr(10).join(f'  - {m}' for m in SUPPORTED_RELATIONSHIP_METRICS)}

SUPPORTED ENTITY TYPES (for flow_trace and neighborhood):
{chr(10).join(f'  - {e}' for e in SUPPORTED_ENTITY_TYPES)}

IMPORTANT NOTES:
- For flow_trace and neighborhood, entity_id is the primary key value (e.g., "740506" for a sales order).
- If the user mentions an entity but does not provide a specific ID, you MUST ask for clarification.
- order_by defaults to "desc" and limit defaults to 10 unless stated otherwise.
- depth defaults to 2 for neighborhood and 8 for flow_trace unless stated otherwise.
- NEVER guess or fabricate an entity_id. If unknown, ask for clarification.
"""


# ──────────────────────────────────────────────────────────────────
# Few-shot examples
# ──────────────────────────────────────────────────────────────────

FEW_SHOT_EXAMPLES = [
    {
        "user": "Which products are associated with the highest number of billing documents?",
        "assistant": '{"query_type": "aggregation", "metric": "product_billing_document_count", "order_by": "desc", "limit": 10}',
    },
    {
        "user": "Which customers have the most sales orders?",
        "assistant": '{"query_type": "aggregation", "metric": "customer_sales_order_count", "order_by": "desc", "limit": 10}',
    },
    {
        "user": "Trace the full flow of sales order 740506",
        "assistant": '{"query_type": "flow_trace", "entity_type": "sales_order_headers", "entity_id": "740506", "depth": 8}',
    },
    {
        "user": "Show all nodes connected to billing document 90504243 within 2 hops",
        "assistant": '{"query_type": "neighborhood", "entity_type": "billing_document_headers", "entity_id": "90504243", "depth": 2}',
    },
    {
        "user": "Find deliveries that were not billed",
        "assistant": '{"query_type": "integrity_check", "check_type": "delivered_not_billed"}',
    },
    {
        "user": "Which relationships are uncertain in the graph?",
        "assistant": '{"query_type": "relationship", "metric": "uncertain"}',
    },
    {
        "user": "Tell me a joke",
        "assistant": '{"action": "reject", "reason": "out_of_domain"}',
    },
    {
        "user": "Trace the flow of a sales order",
        "assistant": '{"action": "clarify", "message": "Which sales order would you like to trace? Please provide the sales order number (e.g., 740506)."}',
    },
]


def build_messages(user_question: str) -> list[dict[str, str]]:
    """
    Build the full message list for the LLM call:
      system prompt + few-shot examples + user question.
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    for ex in FEW_SHOT_EXAMPLES:
        messages.append({"role": "user", "content": ex["user"]})
        messages.append({"role": "assistant", "content": ex["assistant"]})

    messages.append({"role": "user", "content": user_question})
    return messages
