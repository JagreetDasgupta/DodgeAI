"""
routes_query.py — POST /query endpoint.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from models import QueryRequest, QueryResponse
from dependency import get_chat_service

router = APIRouter(tags=["Query"])


@router.post("/query", response_model=QueryResponse)
async def ask_question(req: QueryRequest):
    """
    Convert a natural language question into a structured query,
    execute it against the data engine, and return a grounded answer.
    """
    try:
        svc = get_chat_service(provider_name=req.provider)
        result = svc.ask(req.question)

        return QueryResponse(
            question=result["question"],
            action=result["action"] or "error",
            answer=result.get("answer"),
            structured_query=result.get("structured_query"),
            raw_response=result.get("raw_response"),
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {exc}")
