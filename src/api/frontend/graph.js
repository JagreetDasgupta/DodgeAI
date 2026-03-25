// graph.js — Clean, Structured Graph Rendering using vis-network

const COLOR_MAP = {
    "sales_order_headers": "#4CAF50",          // Green
    "sales_order_items": "#81C784",            // Light Green
    "outbound_delivery_headers": "#2196F3",    // Blue
    "outbound_delivery_items": "#64B5F6",      // Light Blue
    "billing_document_headers": "#FF9800",     // Orange
    "billing_document_items": "#FFB74D",       // Light Orange
    "journal_entry_items_ar": "#9C27B0",       // Purple
    "payments_accounts_receivable": "#E91E63", // Pink
    "business_partners": "#607D8B",            // Blue Grey
    "plants": "#00BCD4",                       // Cyan
    "unknown": "#8b949e"                       // Gray
};

const LEVEL_MAP = {
    "business_partners": 0,
    "sales_order_headers": 1,
    "sales_order_items": 2,
    "outbound_delivery_items": 3,
    "outbound_delivery_headers": 4,
    "billing_document_items": 5,
    "billing_document_headers": 6,
    "journal_entry_items_ar": 7,
    "payments_accounts_receivable": 8,
    "plants": 9
};

let network = null;
let currentNodes = null;
let currentEdges = null;
let isHierarchical = true;

/**
 * Render the graph in the given container
 */
function renderGraph(container, visjsData) {
    if (network) {
        network.destroy();
        network = null;
    }

    if (!visjsData || !visjsData.nodes || visjsData.nodes.length === 0) {
        container.innerHTML = '<div style="padding: 24px; color: #8b949e; text-align: center; font-size: 14px;">Graph available for this query type only</div>';
        return;
    }

    // Limit nodes for extreme performance safeguard (even if backend caps at 400)
    let rawNodes = visjsData.nodes;
    let rawEdges = visjsData.edges;
    // Filter out floating (disconnected) nodes — they add visual noise
    const connectedNodeIds = new Set();
    rawEdges.forEach(edge => {
        connectedNodeIds.add(edge.from);
        connectedNodeIds.add(edge.to);
    });
    rawNodes = rawNodes.filter(node => connectedNodeIds.has(node.id));

    if (rawNodes.length > 180) {
        rawNodes = rawNodes.slice(0, 180);
        const keptIds = new Set(rawNodes.map(n => n.id));
        rawEdges = rawEdges.filter(e => keptIds.has(e.from) && keptIds.has(e.to));
        
        // Show partial graph warning UI
        const warning = document.createElement("div");
        warning.innerHTML = "⚠️ Showing partial graph (180 nodes) for performance.";
        warning.style.position = "absolute";
        warning.style.top = "8px";
        warning.style.right = "8px";
        warning.style.background = "#30363d";
        warning.style.color = "#FFB74D";
        warning.style.fontSize = "12px";
        warning.style.padding = "4px 8px";
        warning.style.borderRadius = "4px";
        warning.style.zIndex = "1000";
        container.appendChild(warning);
    }

    // Apply colors, labels, and hierarchical levels
    const processedNodes = rawNodes.map(node => {
        const type = node.group || "unknown";
        const color = COLOR_MAP[type] || COLOR_MAP["unknown"];
        
        // Clean up labels: only show the ID part, not the fully qualified `type::id`
        let shortLabel = node.label || node.id;
        if (shortLabel.includes("::")) {
            shortLabel = shortLabel.split("::")[1];
        }

        return {
            ...node,
            label: shortLabel,
            level: LEVEL_MAP[type] !== undefined ? LEVEL_MAP[type] : 10,
            color: { 
                background: "#fff", 
                border: color, 
                highlight: { background: "#f6f8fa", border: color },
                hover: { background: "#f6f8fa", border: color }
            },
            font: { color: "#000", size: 14, strokeWidth: 2, strokeColor: "#fff" },
            shape: "dot",
            size: 20,
            borderWidth: 2
        };
    });

    currentNodes = new vis.DataSet(processedNodes);
    currentEdges = new vis.DataSet(rawEdges);

    const data = {
        nodes: currentNodes,
        edges: currentEdges
    };

    const options = getOptions();

    network = new vis.Network(container, data, options);

    // Auto-focus root node
    network.once("afterDrawing", function() {
        if (processedNodes.length > 0) {
            network.focus(processedNodes[0].id, {
                scale: 1.1,
                animation: true
            });
        }
    });

    // Control initial zoom so it doesn't get too tiny
    network.once("stabilizationIterationsDone", function () {
        const scale = network.getScale();
        const clamped = Math.max(0.6, Math.min(scale, 1.2));
        network.moveTo({
            scale: clamped,
            animation: true
        });
    });

    // Node click event for metadata popup and highlight
    network.on("click", function (params) {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const clickedNode = processedNodes.find(n => n.id === nodeId);
            
            // Highlight selected node + neighbors + edges
            const connectedNodes = network.getConnectedNodes(nodeId);
            const connectedEdges = network.getConnectedEdges(nodeId);
            network.selectNodes([nodeId, ...connectedNodes]);
            network.selectEdges(connectedEdges);
            
            if (clickedNode && clickedNode.data) {
                showNodeMetadata(clickedNode);
            }
        } else {
            hideNodeMetadata();
            network.unselectAll();
        }
    });
}

function getOptions() {
    const baseEdges = {
        width: 1.5,
        color: { color: "#888", highlight: "#000", hover: "#000" },
        font: { color: "#888", size: 10, align: "middle", strokeWidth: 0 },
        arrows: { to: { enabled: true, scaleFactor: 0.5 } },
        smooth: { type: "cubicBezier", forceDirection: isHierarchical ? "horizontal" : "none", roundness: 0.4 }
    };

    if (isHierarchical) {
        return {
            nodes: { scaling: { label: { enabled: true, min: 8, max: 18 } } },
            edges: baseEdges,
            layout: {
                hierarchical: {
                    enabled: true,
                    direction: "LR",         // Left-To-Right
                    sortMethod: "directed",
                    levelSeparation: 120,
                    nodeSpacing: 90,
                    treeSpacing: 120
                }
            },
            physics: false, // Totally disable chaotic physics in Flow Mode
            interaction: { hover: true, tooltipDelay: 200, zoomView: true, dragView: true, navigationButtons: true }
        };
    } else {
        // "Explore Mode"
        return {
            nodes: { scaling: { label: { enabled: true, min: 8, max: 18 } } },
            edges: { ...baseEdges, smooth: { type: "continuous" } },
            layout: { hierarchical: { enabled: false } },
            physics: {
                enabled: true,
                solver: "forceAtlas2Based",
                forceAtlas2Based: { gravitationalConstant: -50, centralGravity: 0.01, springLength: 100, springConstant: 0.08, damping: 0.4 },
                stabilization: { iterations: 150 }
            },
            interaction: { hover: true, tooltipDelay: 200, zoomView: true, dragView: true, navigationButtons: true }
        };
    }
}

function toggleGraphLayout() {
    if (!network) return;
    isHierarchical = !isHierarchical;
    network.setOptions(getOptions());
    if (isHierarchical) {
        network.fit({ animation: true });
        
        setTimeout(() => {
            const currentScale = network.getScale();
            network.moveTo({
                scale: Math.min(currentScale, 0.9),
                animation: true
            });
        }, 300);
    }
}

function showNodeMetadata(node) {
    const detailPanel = document.getElementById("node-details");
    const container = document.getElementById("graph-container");
    if (!detailPanel) return;

    let html = `<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #30363d;">
        <div>
            <h4 style="color: #fff; font-size: 15px; margin: 0; padding-bottom: 4px;">${node.label}</h4>
            <span style="font-size: 10px; background: ${node.color.border}; padding: 3px 6px; border-radius: 4px; color: #fff; text-transform: uppercase;">${node.group}</span>
        </div>
    </div>`;
    
    html += `<table style="width: 100%; border-collapse: collapse; font-size: 13px;">`;
    // Force ID to top
    html += `
        <tr style="border-bottom: 1px solid #21262d;">
            <td style="padding: 6px 0; color: #8b949e; width: 40%;">ID</td>
            <td style="padding: 6px 0; color: #c9d1d9; font-weight: 500; word-break: break-all;">${node.id}</td>
        </tr>
    `;
    // Other data
    for (const [key, value] of Object.entries(node.data)) {
        html += `
            <tr style="border-bottom: 1px solid #21262d;">
                <td style="padding: 6px 0; color: #8b949e; width: 40%; word-break: break-all;">${key.replace(/_/g, " ")}</td>
                <td style="padding: 6px 0; color: #c9d1d9; font-weight: 500;">${value}</td>
            </tr>
        `;
    }
    html += `</table>`;

    detailPanel.innerHTML = html;
    detailPanel.style.display = "block";
    
    // Slight width adjustment to make room for the panel nicely
    container.style.width = "calc(100% - 300px)";
}

function hideNodeMetadata() {
    const detailPanel = document.getElementById("node-details");
    const container = document.getElementById("graph-container");
    if (detailPanel) detailPanel.style.display = "none";
    if (container) container.style.width = "100%";
}
