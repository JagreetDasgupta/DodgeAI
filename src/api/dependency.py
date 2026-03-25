"""
dependency.py — Shared service singletons for FastAPI dependency injection.
"""

from __future__ import annotations

import sys
from pathlib import Path
from functools import lru_cache

# Wire up import paths
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_NLQ_DIR = str(_PROJECT_ROOT / "src" / "nlq")
_QUERY_DIR = str(_PROJECT_ROOT / "src" / "query")

for _d in [_NLQ_DIR, _QUERY_DIR]:
    if _d not in sys.path:
        sys.path.insert(0, _d)

from chat_service import ChatService, create_service
from graph_queries import _load_graph, get_node, neighborhood as gq_neighborhood
from query_router import QueryEngine
from sql_queries import list_available_metrics
from anomaly_checks import list_available_checks

from config import DB_PATH, GRAPH_PATH

# Paths
DB_PATH_OBJ = Path(DB_PATH)
GRAPHML_PATH_OBJ = Path(GRAPH_PATH)
SCHEMA_PATH = _PROJECT_ROOT / "output" / "schema_summary.json"


@lru_cache(maxsize=4)
def get_chat_service(provider_name: str = "offline") -> ChatService:
    return create_service(DB_PATH_OBJ, GRAPHML_PATH_OBJ, provider_name=provider_name)


@lru_cache(maxsize=1)
def get_graph():
    return _load_graph(GRAPHML_PATH_OBJ)


def get_schema_summary() -> dict:
    import json
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)
