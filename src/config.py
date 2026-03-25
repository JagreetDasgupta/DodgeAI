import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
DB_PATH = os.path.join(OUTPUT_DIR, "sap_o2c.db")
GRAPH_PATH = os.path.join(OUTPUT_DIR, "graph", "sap_o2c_graph.graphml")

if not os.path.exists(DB_PATH):
    print(f"[WARNING] Database not found at {DB_PATH}")
