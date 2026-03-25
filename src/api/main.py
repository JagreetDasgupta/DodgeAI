"""
main.py — FastAPI application entry point.

Usage:
    cd "c:\\Users\\jagre\\OneDrive\\Desktop\\Dodge AI"
    uvicorn src.api.main:app --reload --port 8000

    # Or run directly:
    python src/api/main.py
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Wire up import paths before anything else
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_API_DIR = str(Path(__file__).resolve().parent)
_NLQ_DIR = str(_PROJECT_ROOT / "src" / "nlq")
_QUERY_DIR = str(_PROJECT_ROOT / "src" / "query")

for _d in [_API_DIR, _NLQ_DIR, _QUERY_DIR]:
    if _d not in sys.path:
        sys.path.insert(0, _d)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routes_query import router as query_router
from routes_graph import router as graph_router
from routes_meta import router as meta_router

# ──────────────────────────────────────────────────────────────────
# App
# ──────────────────────────────────────────────────────────────────

from config import DB_PATH
import os

app = FastAPI(
    title="SAP O2C Query System",
    description="Graph-Based Data Modeling and Query System — API Layer (Phase 5)",
    version="1.0.0",
)

print("DB PATH:", DB_PATH)
print("DB EXISTS:", os.path.exists(DB_PATH))

# CORS for frontend dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(query_router, prefix="/api")
app.include_router(graph_router, prefix="/api")
app.include_router(meta_router, prefix="/api")

# Mount frontend static files
_FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"
if _FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_FRONTEND_DIR)), name="static")

    @app.get("/")
    async def root():
        return FileResponse(str(_FRONTEND_DIR / "index.html"))
else:
    @app.get("/")
    async def root_info():
        return {
            "message": "SAP O2C Graph Query System",
            "version": "1.0.0",
            "features": [
                "Natural language queries",
                "Graph traversal",
                "Integrity checks",
                "LLM-powered query translation",
                "Multi-provider LLM support (Offline, Gemini, Groq)"
            ]
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
