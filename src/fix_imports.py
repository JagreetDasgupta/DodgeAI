import os
import re

SRC_DIR = os.path.dirname(os.path.abspath(__file__))

# Internal modules we know belong to this project
INTERNAL_MODULES = {
    # /src
    "config", "profiler", "schema_inference", "validator", "file_scanner",
    "transformer", "storage_writer", "test_groq", "test_llm", "test_phase3", "test_phase4",
    # /src/api
    "dependency", "routes_graph", "routes_meta", "routes_query",
    # /src/query
    "anomaly_checks", "graph_queries", "query_models", "query_router", "sql_queries",
    # /src/nlq
    "chat_service", "guardrails", "intent_classifier", "query_parser", "response_summarizer",
    # /src/graph
    "graph_loader"
}

# The parent mapping for these modules so we can construct `src.subfolder.module` 
# (Actually, the user gave examples: `from config import ... -> from src.config import ...`
# `from dependency import ... -> from src.api.dependency import ...`
# `from routes_query import ... -> from src.api.routes_query import ...`
# `from graph_queries import ... -> from src.query.graph_queries import ...`)

MODULE_MAPPING = {
    # Root
    "config": "src.config",
    "profiler": "src.profiler",
    "schema_inference": "src.schema_inference",
    "validator": "src.validator",
    "file_scanner": "src.file_scanner",
    "transformer": "src.transformer",
    "storage_writer": "src.storage_writer",
    "test_groq": "src.test_groq",
    "test_llm": "src.test_llm",
    "test_phase3": "src.test_phase3",
    "test_phase4": "src.test_phase4",
    # API
    "dependency": "src.api.dependency",
    "routes_graph": "src.api.routes_graph",
    "routes_meta": "src.api.routes_meta",
    "routes_query": "src.api.routes_query",
    # Query
    "anomaly_checks": "src.query.anomaly_checks",
    "graph_queries": "src.query.graph_queries",
    "query_models": "src.query.query_models",
    "query_router": "src.query.query_router",
    "sql_queries": "src.query.sql_queries",
    # NLQ
    "chat_service": "src.nlq.chat_service",
    "guardrails": "src.nlq.guardrails",
    "intent_classifier": "src.nlq.intent_classifier",
    "query_parser": "src.nlq.query_parser",
    "response_summarizer": "src.nlq.response_summarizer",
    # Graph
    "graph_loader": "src.graph.graph_loader"
}

def fix_imports():
    changes = []
    
    for root, _, files in os.walk(SRC_DIR):
        for f in files:
            if not f.endswith(".py"):
                continue
            if f == "fix_imports.py":
                continue
                
            filepath = os.path.join(root, f)
            with open(filepath, "r", encoding="utf-8") as file:
                content = file.read()
                
            new_content = content
            lines = new_content.split('\n')
            
            for i, line in enumerate(lines):
                # match `from X import Y`
                # be careful to only match it at the beginning of the line or with leading spaces
                match = re.match(r'^(\s*)from\s+([a-zA-Z0-9_]+)\s+import\s+(.*)', line)
                if match:
                    indent = match.group(1)
                    module_name = match.group(2)
                    imports = match.group(3)
                    
                    if module_name in MODULE_MAPPING:
                        new_module_name = MODULE_MAPPING[module_name]
                        new_line = f"{indent}from {new_module_name} import {imports}"
                        if new_line != line:
                            changes.append(f"{line.strip()} → {new_line.strip()}")
                            lines[i] = new_line
            
            # match `import X` where X is an internal module
            for i, line in enumerate(lines):
                match = re.match(r'^(\s*)import\s+([a-zA-Z0-9_]+)$', line)
                if match:
                    indent = match.group(1)
                    module_name = match.group(2)
                    if module_name in MODULE_MAPPING:
                        new_module_name = MODULE_MAPPING[module_name]
                        new_line = f"{indent}import {new_module_name}"
                        if new_line != line:
                            changes.append(f"{line.strip()} → {new_line.strip()}")
                            lines[i] = new_line
            
            modified = '\n'.join(lines)
            if modified != content:
                with open(filepath, "w", encoding="utf-8") as file:
                    file.write(modified)
                    
    for c in changes:
        print(c)

if __name__ == "__main__":
    fix_imports()
