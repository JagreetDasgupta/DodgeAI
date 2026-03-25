"""
schema_inference.py — Defines entity schemas from profiled data.

Maps each entity to a table name (snake_case), columns, primary keys,
foreign keys, and relationship metadata.  Outputs schema_summary.json.
"""

import json
import re
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────
# Canonical schema definitions — derived from data discovery
# ──────────────────────────────────────────────────────────────────

ENTITY_SCHEMAS: list[dict[str, Any]] = [
    # ─── Order Domain ─────────────────────────────────────────────
    {
        "table_name": "sales_order_headers",
        "source_directory": "sales_order_headers",
        "primary_key": ["sales_order"],
        "foreign_keys": [
            {"columns": ["sold_to_party"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "sales_order_items",
        "source_directory": "sales_order_items",
        "primary_key": ["sales_order", "sales_order_item"],
        "foreign_keys": [
            {"columns": ["sales_order"], "references_table": "sales_order_headers",
             "references_columns": ["sales_order"], "confidence": "high"},
            {"columns": ["material"], "references_table": "products",
             "references_columns": ["product"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "sales_order_schedule_lines",
        "source_directory": "sales_order_schedule_lines",
        "primary_key": ["sales_order", "sales_order_item", "schedule_line"],
        "foreign_keys": [
            {"columns": ["sales_order", "sales_order_item"],
             "references_table": "sales_order_items",
             "references_columns": ["sales_order", "sales_order_item"],
             "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Delivery Domain ──────────────────────────────────────────
    {
        "table_name": "outbound_delivery_headers",
        "source_directory": "outbound_delivery_headers",
        "primary_key": ["delivery_document"],
        "foreign_keys": [],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "outbound_delivery_items",
        "source_directory": "outbound_delivery_items",
        "primary_key": ["delivery_document", "delivery_document_item"],
        "foreign_keys": [
            {"columns": ["delivery_document"],
             "references_table": "outbound_delivery_headers",
             "references_columns": ["delivery_document"], "confidence": "high"},
            {"columns": ["reference_sd_document"],
             "references_table": "sales_order_headers",
             "references_columns": ["sales_order"], "confidence": "uncertain",
             "note": "ID format differs (delivery ref vs sales order number)"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Billing Domain ───────────────────────────────────────────
    {
        "table_name": "billing_document_headers",
        "source_directory": "billing_document_headers",
        "primary_key": ["billing_document"],
        "foreign_keys": [
            {"columns": ["sold_to_party"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "billing_document_items",
        "source_directory": "billing_document_items",
        "primary_key": ["billing_document", "billing_document_item"],
        "foreign_keys": [
            {"columns": ["billing_document"],
             "references_table": "billing_document_headers",
             "references_columns": ["billing_document"], "confidence": "high"},
            {"columns": ["reference_sd_document"],
             "references_table": "outbound_delivery_headers",
             "references_columns": ["delivery_document"], "confidence": "uncertain",
             "note": "Reference likely points to delivery, but format uncertain"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "billing_document_cancellations",
        "source_directory": "billing_document_cancellations",
        "primary_key": ["billing_document"],
        "foreign_keys": [
            {"columns": ["billing_document"],
             "references_table": "billing_document_headers",
             "references_columns": ["billing_document"], "confidence": "high",
             "note": "Cancellation document ID present in billing headers as well"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Finance Domain ───────────────────────────────────────────
    {
        "table_name": "journal_entry_items_ar",
        "source_directory": "journal_entry_items_accounts_receivable",
        "primary_key": ["accounting_document"],
        "foreign_keys": [
            {"columns": ["reference_document"],
             "references_table": "billing_document_headers",
             "references_columns": ["billing_document"], "confidence": "high"},
            {"columns": ["customer"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "payments_accounts_receivable",
        "source_directory": "payments_accounts_receivable",
        "primary_key": ["accounting_document"],
        "foreign_keys": [
            {"columns": ["customer"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
            {"columns": ["clearing_accounting_document"],
             "references_table": "journal_entry_items_ar",
             "references_columns": ["accounting_document"],
             "confidence": "uncertain",
             "note": "Clearing doc links payment to AR entry, but values may not always match"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Master Data: Business Partners ───────────────────────────
    {
        "table_name": "business_partners",
        "source_directory": "business_partners",
        "primary_key": ["business_partner"],
        "foreign_keys": [],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "business_partner_addresses",
        "source_directory": "business_partner_addresses",
        "primary_key": ["business_partner", "address_id"],
        "foreign_keys": [
            {"columns": ["business_partner"],
             "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "customer_company_assignments",
        "source_directory": "customer_company_assignments",
        "primary_key": ["customer", "company_code"],
        "foreign_keys": [
            {"columns": ["customer"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "customer_sales_area_assignments",
        "source_directory": "customer_sales_area_assignments",
        "primary_key": ["customer", "sales_organization", "distribution_channel", "division"],
        "foreign_keys": [
            {"columns": ["customer"], "references_table": "business_partners",
             "references_columns": ["business_partner"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Master Data: Products ────────────────────────────────────
    {
        "table_name": "products",
        "source_directory": "products",
        "primary_key": ["product"],
        "foreign_keys": [],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "product_descriptions",
        "source_directory": "product_descriptions",
        "primary_key": ["product", "language"],
        "foreign_keys": [
            {"columns": ["product"], "references_table": "products",
             "references_columns": ["product"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "product_plants",
        "source_directory": "product_plants",
        "primary_key": ["product", "plant"],
        "foreign_keys": [
            {"columns": ["product"], "references_table": "products",
             "references_columns": ["product"], "confidence": "high"},
            {"columns": ["plant"], "references_table": "plants",
             "references_columns": ["plant"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },
    {
        "table_name": "product_storage_locations",
        "source_directory": "product_storage_locations",
        "primary_key": ["product", "plant", "storage_location"],
        "foreign_keys": [
            {"columns": ["product", "plant"],
             "references_table": "product_plants",
             "references_columns": ["product", "plant"], "confidence": "high"},
        ],
        "columns_to_drop_if_all_null": True,
    },

    # ─── Master Data: Plants ──────────────────────────────────────
    {
        "table_name": "plants",
        "source_directory": "plants",
        "primary_key": ["plant"],
        "foreign_keys": [],
        "columns_to_drop_if_all_null": True,
    },
]


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def camel_to_snake(name: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case."""
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_schema_for_entity(source_directory: str) -> dict | None:
    """Look up the canonical schema definition by source directory name."""
    for s in ENTITY_SCHEMAS:
        if s["source_directory"] == source_directory:
            return s
    return None


def build_schema_summary() -> dict:
    """
    Build the full schema summary dict ready for JSON serialisation.
    Includes all entities, their fields, keys, FKs, and uncertainty flags.
    """
    entities = []
    for schema in ENTITY_SCHEMAS:
        fk_list = []
        for fk in schema.get("foreign_keys", []):
            fk_entry = {
                "columns": fk["columns"],
                "references_table": fk["references_table"],
                "references_columns": fk["references_columns"],
                "confidence": fk["confidence"],
            }
            if "note" in fk:
                fk_entry["note"] = fk["note"]
            fk_list.append(fk_entry)

        entities.append({
            "table_name": schema["table_name"],
            "source_directory": schema["source_directory"],
            "primary_key": schema["primary_key"],
            "foreign_keys": fk_list,
        })

    return {
        "dataset": "SAP Order-to-Cash",
        "entity_count": len(entities),
        "entities": entities,
    }


def save_schema_summary(output_path: Path) -> None:
    """Persist the schema summary as JSON."""
    summary = build_schema_summary()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"Schema summary saved to {output_path}")
