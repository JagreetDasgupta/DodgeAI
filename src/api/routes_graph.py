"""
routes_graph.py — Graph inspection endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from models import NodeResponse, NeighborhoodResponse, GraphMetadataResponse, FlowTraceResponse
from dependency import get_graph, GRAPHML_PATH
from graph_queries import get_node, neighborhood as gq_neighborhood, trace_flow, to_visjs_format, resolve_node_id

router = APIRouter(prefix="/graph", tags=["Graph"])


@router.get("/node/{node_id}", response_model=NodeResponse)
async def get_graph_node(node_id: str):
    """Fetch a single node by ID."""
    G = get_graph()
    data = get_node(G, node_id)
    if data is None:
        return NodeResponse(status="not_found", node_id=node_id)
    return NodeResponse(status="ok", node_id=node_id, data=data)


@router.get("/neighbors/{node_id}", response_model=NeighborhoodResponse)
async def get_neighbors(node_id: str, depth: int = Query(2, ge=1, le=5)):
    """Get the neighborhood subgraph around a node."""
    G = get_graph()
    resolved_id = resolve_node_id(G, node_id)
    if not resolved_id:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Node not found",
                "details": f"No graph node matches ID {node_id}",
                "hint": "Try specifying entity type (e.g., sales_order_headers)"
            }
        )

    result = gq_neighborhood(GRAPHML_PATH, resolved_id, depth=depth)
    if result.get("status") == "not_found":
        return NeighborhoodResponse(status="not_found", center_node=resolved_id, depth=depth)
    return NeighborhoodResponse(
        status="ok",
        center_node=result["center_node"],
        depth=result["depth"],
        node_count=result["node_count"],
        edge_count=result["edge_count"],
        nodes=result["nodes"][:100],    # Cap for API response
        edges=result["edges"][:200],
        visjs_data=to_visjs_format(result["nodes"], result["edges"]),
    )


@router.get("/subgraph/{node_id}", response_model=NeighborhoodResponse)
async def get_subgraph(node_id: str, depth: int = Query(2, ge=1, le=5)):
    """Alias for /neighbors — extracts the ego-subgraph."""
    return await get_neighbors(node_id, depth)


@router.get("/flow/{node_id}", response_model=FlowTraceResponse)
async def get_flow_trace(node_id: str, depth: int = Query(8, ge=1, le=10)):
    """Get the full O2C flow trace around a node, tailored for vis.js visualization."""
    G = get_graph()
    resolved_id = resolve_node_id(G, node_id)
    if not resolved_id:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Node not found",
                "details": f"No graph node matches ID {node_id}",
                "hint": "Try specifying entity type (e.g., sales_order_headers)"
            }
        )

    result = trace_flow(GRAPHML_PATH, resolved_id, depth=depth)
    if result.get("status") == "not_found":
        return FlowTraceResponse(status="not_found", start_node=resolved_id, depth=depth)
    
    # We need to rebuild the full edges payload for vis.js since trace_flow compresses it
    G = get_graph()
    flow_steps = result.get("flow_steps", [])
    
    # Collect all IDs in the flow
    found_ids = set()
    for step in flow_steps:
        found_ids.update(step.get("node_ids", []))
        
    flow_nodes = []
    for nid in found_ids:
        if nid in G:
            flow_nodes.append({"node_id": nid, **dict(G.nodes[nid])})
            
    # Extract structural subgraph edges
    flow_edges = []
    for u, v, data in G.edges(data=True):
        if u in found_ids and v in found_ids:
            flow_edges.append({
                "source": u, "target": v,
                "edge_type": data.get("edge_type", ""),
                "confidence": data.get("confidence", ""),
            })

    return FlowTraceResponse(
        status="ok",
        start_node=result["start_node"],
        depth=depth,
        total_nodes_reached=result["total_nodes_reached"],
        flow_steps=flow_steps,
        gaps=result.get("gaps", []),
        visjs_data=to_visjs_format(flow_nodes, flow_edges, max_nodes=400),
    )


@router.get("/metadata", response_model=GraphMetadataResponse)
async def get_metadata():
    """Return high-level graph statistics."""
    G = get_graph()

    entity_types = sorted({d.get("node_type", "") for _, d in G.nodes(data=True)} - {""})
    edge_types_set: dict[str, int] = {}
    uncertain = 0
    for _, _, d in G.edges(data=True):
        et = d.get("edge_type", "unknown")
        edge_types_set[et] = edge_types_set.get(et, 0) + 1
        if d.get("confidence") == "uncertain":
            uncertain += 1

    return GraphMetadataResponse(
        total_nodes=G.number_of_nodes(),
        total_edges=G.number_of_edges(),
        entity_types=entity_types,
        edge_types=sorted(edge_types_set.keys()),
        uncertain_edge_count=uncertain,
    )
