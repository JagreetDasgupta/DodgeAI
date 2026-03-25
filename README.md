# Graph-Based Data Modeling and Query System

> **Phase 1**: ✅ Complete — Data Foundation  
> **Phase 2**: ✅ Complete — Graph Construction  
> **Phase 3**: ✅ Complete — Deterministic Query Engine  
> **Phase 4**: ✅ Complete — LLM Natural Language Layer  
> **Phase 5**: ✅ Complete — API + Frontend

## Overview

This project transforms a raw SAP Order-to-Cash (O2C) JSONL dataset into a clean relational database, layers a navigable graph on top, provides a deterministic query engine, adds LLM-powered natural language understanding, and exposes everything through a FastAPI backend with a minimal frontend.

---

## Design Decisions & Tradeoffs

### (a) Database Choice (SQLite vs RDBMS/NoSQL)
**Why SQLite?** SQLite was chosen for zero-configuration, embedded persistence. Given the dataset size (21,393 rows, representing ~50MB of normalized data), the entire relational model easily fits in a single memory-mapped file, making operations lightning-fast on a single node without the network overhead of a dedicated database server.
**Why not PostgreSQL or NoSQL?** While a traditional RDBMS (PostgreSQL) offers superior write-concurrency, this system is inherently read-heavy post-ingestion. A document-oriented NoSQL database was evaluated but rejected, as it would destroy the absolute requirement for strict schema enforcement, relationships, and ACID joins necessary for multi-table supply chain tracking.
**Tradeoffs:** The system trades horizontal write scalability for dramatic operational simplicity and zero-latency local analytical reads.

### (b) Graph Layer Justification
**Why SQL alone is insufficient:** Writing recursive SQL CTEs to trace a supply chain entity horizontally downstream (e.g., from an Order through multiple partial Deliveries, aggregated Bills, and split Payments across 19 interconnected tables) is computationally expensive, highly fragile, and practically unreadable.
**Why NetworkX:** Modeling the relationships as a geometric graph using NetworkX natively exposes topological primitives. Complex systemic questions suddenly become trivial graph algorithms.
**Why BFS for flow tracing:** Breadth-First Search perfectly models the step-wise chronological propagation of an Order-to-Cash process without deeply recursing into parallel unlinked subgraphs.
**Tradeoffs:** Materializing an overlapping NetworkX graph alongside an SQLite database duplicates memory utilization. However, this intentional duplication buys massive query performance acceleration by matching the data structure to the exact read pattern.

### (c) LLM Prompting Strategy
**LLM for Translation Only:** The supreme architectural directive of this system is that the LLM is **never** permitted to answer the user's question, execute queries, or generate dynamic SQL. It acts exclusively as a semantic translation layer mapping English intent into a rigidly constrained JSON payload representing a bounded `QueryRequest`.
**JSON Schema Enforcement:** Enforcing strict JSON outputs forces structural interface boundaries so the deterministic Python backend can deserialize parameters without ambiguity.
**Why deterministic QueryRequest:** By constraining the system to a fixed `QueryRequest` schema, the backend is rendered entirely immune to prompt-injection or SQL-injection attacks, and hallucination risk on the data itself is utterly eliminated.
**Temperature = 0:** The LLM's creativity is zeroed out to lock it into the most statistically probable semantic translation.
**Few-Shot Grounding:** Explicitly providing pre-solved examples inside the system prompt anchors the LLM to zero-in on correct entity identification and parameter extraction techniques before it processes the user's input.

### (d) System Guardrails Architecture
A defense-in-depth architecture strictly manages LLM unpredictability:
1. **Pre-LLM Filtering:** A regex bounds-check instantly rejects off-topic prompts (e.g., poems, code generation) mathematically before wasting expensive GPU tokens.
2. **Post-LLM Validation:** The returned JSON string is validated via strict Pydantic models. If the schema is broken, the pipeline is safely halted without executing malformed queries.
3. **Execution Constraints:** Hard clamps on parameter logic (e.g., `depth > 10` clamped to `10`) defend the Graph engine against accidental memory-exhaustion DoS attacks when executing BFS.
4. **Visualization Limits:** UI explicitly refuses to render > 180 nodes in full physics mode to prevent browser memory freezing.

---

## Phase 1 — Data Foundation

### Dataset

- **Source**: 49 JSONL part-files in `sap-order-to-cash-dataset/sap-o2c-data/`
- **Result**: 19 normalized tables, 21,393 rows in `output/sap_o2c.db`

| Domain | Entities | Records |
|--------|----------|---------|
| **Orders** | sales_order_headers, sales_order_items, sales_order_schedule_lines | 446 |
| **Deliveries** | outbound_delivery_headers, outbound_delivery_items | 223 |
| **Billing** | billing_document_headers, billing_document_items, billing_document_cancellations | 488 |
| **Finance** | journal_entry_items_ar, payments_accounts_receivable | 243 |
| **Master Data** | business_partners, addresses, customer assignments, products, plants, storage locations | 19,993 |

### Normalisation Applied

camelCase → snake_case · ISO dates → YYYY-MM-DD · nested time dicts → HH:MM:SS · numeric strings → floats · 100%-null columns dropped · lineage columns added

### Phase 1 Validation

```
58 PASS  |  1 WARN  |  0 FAIL
```

> **Why this matters:** Building a flawless, fully-typed normalization foundation guarantees that all upstream graph linkages and analytical SQL aggregations are structurally sound without requiring real-time data cleansing.

---

## Phase 2 — Graph Construction

### How the Graph Is Modeled

Each row in the 19 SQLite tables becomes a **node**. Each FK relationship from `schema_summary.json` produces **edges** between matching records.

**Node attributes**: stable ID (`table::pk_values`), node_type, human-readable label, source table, PK fields, metadata (amounts/dates/statuses), lineage.

**Edge attributes**: edge_type, confidence (`high` or `uncertain`), FK/ref columns, optional note.

### Graph Statistics

| Metric | Value |
|--------|-------|
| Total nodes | 21,393 |
| Total edges | 24,994 |
| Uncertain edges | 482 |
| Connected components | 1 (fully connected) |
| FK relationships covered | 21 / 21 |
| Disconnected nodes | 0 |

### How Uncertain Relationships Are Handled

Three FK relationships are marked `uncertain` in the schema:

1. **outbound_delivery_items → sales_order_headers** (via `reference_sd_document`): 137 edges — ID format may differ
2. **billing_document_items → outbound_delivery_headers** (via `reference_sd_document`): 245 edges — reference likely points to delivery
3. **payments_accounts_receivable → journal_entry_items_ar** (via `clearing_accounting_document`): 100 edges — not all values match

These are preserved as edges with `confidence: "uncertain"` — never dropped, never hallucinated. They appear in all exports and can be filtered programmatically.

> **Why this matters:** Converting rigid SQL foreign keys into probabilistic weighted graph edges allows the system to bridge imperfect real-world semantic references (e.g., loosely linked payments to bulk accounting ledgers) without mathematically corrupting strict join paths.

---

## Phase 3 — Deterministic Query Engine

### How Queries Are Routed

The `QueryEngine` accepts a `QueryRequest` object and routes it based on `query_type`:

| Query Type | Handler | Backend |
|---|---|---|
| `aggregation` | `sql_queries.py` | SQLite |
| `flow_trace` | `graph_queries.py` | NetworkX graph |
| `neighborhood` | `graph_queries.py` | NetworkX graph |
| `integrity_check` | `anomaly_checks.py` | SQL + graph |
| `relationship` | `graph_queries.py` | NetworkX graph |

### When SQL vs Graph Is Used

- **SQL (Aggregations & Filters):** Triggered for quantitative counts (GROUP BY), specific record lookups, and cross-table anomaly checks (LEFT JOINs) where columnar arithmetic and strict intersection logic are mathematically necessary.
- **Graph (Topology & Flow):** Triggered for supply-chain flow tracing (BFS), ego-neighborhood extraction, degree analysis, and connected component scanning where lateral systemic relationships dictate the access pattern.
- **Hybrid (Complex Integrity):** Checks like "delivered but not billed" use SQL for isolated precision, while broad questions like "disconnected core flows" rely heavily on generic graph traversal.

### Available Metrics (10)

`customer_sales_order_count`, `customer_billing_count`, `customer_payment_count`, `customer_journal_entry_count`, `product_billing_document_count`, `product_sales_order_count`, `billing_document_item_count`, `sales_order_item_count`, `delivery_item_count`, `plant_product_count`

### Available Integrity Checks (7)

`delivered_not_billed`, `billed_without_delivery`, `orders_without_delivery`, `billing_without_journal`, `payments_without_journal_link`, `disconnected_nodes`, `incomplete_o2c_flows`

### Demo Results

19 queries executed: **13 OK · 0 no-results · 6 expected errors**

Key findings:
- 3 deliveries not billed, 14 orders without delivery, 20 unlinked payments
- 8 incomplete O2C flows detected
- Customer `320000083` has highest degree (434 connections) and most sales orders (72)

---

## Phase 4 — LLM Natural Language Layer

### Architecture Diagram

```mermaid
flowchart TD
    User([User]) -->|Natural Language Question| API[FastAPI Endpoint]
    API --> Service[Chat Service]
    
    subgraph Natural Language Processing
        Service --> PreCheck{Pre-Guardrail}
        PreCheck -->|Off-topic| Reject[Reject Response]
        PreCheck -->|Valid| LLM[LLM Provider]
        LLM -->|Translates to| JSON[{Structured JSON}]
        JSON --> Parser[Query Parser]
        Parser --> PostCheck{Post-Guardrail}
    end
    
    subgraph Deterministic Execution
        PostCheck -->|Valid Request| Engine[Query Engine]
        Engine --> Router{Query Router}
        Router -->|Aggregations\nIntegrity| SQL[(SQLite DB)]
        Router -->|Flow Trace\nNeighborhood| Graph((NetworkX Graph))
        SQL --> Results[Result Formatter]
        Graph --> Results
    end
    
    Results --> Summarizer[Response Summarizer]
    Summarizer -->|Data-Grounded NL Answer| API
```

### Design Decisions

1. **LLM only translates (No hallucination risk):** The LLM is **never** the source of truth for answering questions or traversing data. It only translates natural language into a strict structured `QueryRequest` JSON. All data aggregation, flow tracing, and relationship analysis happen exclusively on the deterministic backend.
2. **Deterministic Execution:** The `QueryEngine` executes extremely specific, pre-programmed Python functions based directly on the validated JSON. There is no SQL string generation, meaning unpredictable query paths and injection risks are physically impossible.
3. **Strict Guardrails as a System:** See the **Guardrails Architecture** outlined above (Pre-check filtering, boundary clamping, and post-check schema validation) controlling all interactions dynamically.

### Pipeline Flow

```
User question → Pre-guardrail → LLM classify → JSON parse → Validate
              → Post-guardrail → QueryEngine → Summarize → Answer
```

### Providers

The provider is abstract (`LLMProvider` protocol). Swap implementations dynamically:

| Provider | Usage | CLI Flag | Requires |
|----------|-------|----------|----------|
| `OfflineProvider` | Rule-based keyword matching (default, fast, no API) | (None) | - |
| `GroqProvider` | LLaMA-based fast inference (`llama-3.3-70b-versatile`) | `--groq` | `GROQ_API_KEY` |
| `GeminiProvider` | Google Gemini 2.0 Flash (free tier) | `--gemini` | `GEMINI_API_KEY` |

---

## Phase 6 — Interactive Graph Visualization

A lightweight `vis-network` interactive graph layer is integrated into the UI.

### Architecture

```
User → NLQ → QueryEngine → API → Graph JSON → vis-network → UI
```

### Design Restrictions & Performance
To absolutely prevent browser lockups from massive geometry payloads, the UI **never** renders the entire graph. The visualization enforces **subgraph-only rendering** explicitly scoped to bounding contexts dynamically returned by the query engine:
1. **Neighborhoods:** e.g., "Show neighbors of business partner X"
2. **O2C Flow Traces:** e.g., "Trace sales order Y"

Both queries execute a strict server-side node threshold crop (`node_limit: 400`) before data ever hits the client, while the frontend aggressively filters disconnected entities to guarantee smooth 60fps interaction during demonstrations.

### UI Features
- **Hierarchical vs Explore Modes:** The system defaults to a hierarchical *Flow Mode* representing the true Left-to-Right directional nature of the O2C pipeline (Orders → Deliveries → Billing → Finance). Users can actively toggle into *Explore Mode* to unleash dynamic spring-physics.
- **Vis-network Usage:** High performance, interactive network charts utilizing `vis-network.js` via CDN.
- **Interactive Inspection:** Clicking a node automatically expands a sliding metadata panel with full JSON payloads without cluttering the chat history.

### Demo Instructions
To experience the visualization in action via Natural Language, try explicitly asking the system:
- *"Trace the flow of sales order 740506"*
- *"Show neighbors of business partner 320000083"*
- *"What is connected to billing document 502011?"*

---

### Demo Results

15 test cases: **15 PASS · 0 FAIL** (offline mode)

---

## Phase 7 — API + Frontend

### Architecture

The API is a **thin wrapper only** — it does not reimplement any business logic. It calls Phase 4's `ChatService.ask()` for NL queries and Phase 2's graph functions for graph inspection.

### Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/query` | NL question → structured query → grounded answer |
| `GET` | `/api/graph/node/{node_id}` | Fetch a single node |
| `GET` | `/api/graph/neighbors/{node_id}?depth=2` | Ego-subgraph extraction |
| `GET` | `/api/graph/subgraph/{node_id}?depth=2` | Alias for neighbors |
| `GET` | `/api/graph/metadata` | Graph stats (nodes, edges, types) |
| `GET` | `/api/health` | Health check |
| `GET` | `/api/schema` | Full schema summary |
| `GET` | `/api/supported-queries` | All supported metrics/checks/entities |

### Frontend

Served at `http://localhost:8000/` — dark-themed split-panel layout:
- **Left**: Graph metadata explorer (node/edge counts, entity types, metrics)
- **Right**: Chat interface with quick-ask chips and formatted responses

### Test Results

8/8 endpoints passed: health, graph metadata, supported queries, aggregation, integrity, rejection, node lookup, neighbors.

---

## Running

```bash
cd "c:\Users\jagre\OneDrive\Desktop\Dodge AI"
python src/main.py              # Phase 1
python src/graph/main.py        # Phase 2
python src/query/main.py        # Phase 3
python src/nlq/main.py           # Phase 4

# Phase 5 — API server
cd src/api
set PYTHONPATH=.;../nlq;../query
python -c "import uvicorn; uvicorn.run('main:app', host='0.0.0.0', port=8000)"
# Then open http://localhost:8000
```

## Code Structure

```
src/
├── [Phase 1] file_scanner / profiler / schema_inference / transformer
│            storage_writer / validator / main
├── graph/   [Phase 2] graph_loader / graph_builder / graph_traversal
│                     graph_exporter / graph_validator / main
├── query/   [Phase 3] query_models / sql_queries / graph_queries
│                     anomaly_checks / result_formatter / query_router / main
├── nlq/     [Phase 4] llm_prompting / intent_classifier / query_parser
│                     guardrails / response_summarizer / chat_service / main
└── api/     [Phase 5] main / models / dependency
                      routes_query / routes_graph / routes_meta
                      frontend/index.html
```

## Assumptions

1. `businessPartner = customer` — same ID space used across tables
2. `referenceSdDocument` — likely upstream doc reference, format uncertain → **uncertain FK**
3. `clearingAccountingDocument` — may link payment to AR entry, not all match → **uncertain FK**
4. Composite keys follow SAP conventions (document + item number)
