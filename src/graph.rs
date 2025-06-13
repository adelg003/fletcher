use petgraph::{
    algo::is_cyclic_directed,
    graph::{DiGraph, GraphError, NodeIndex},
};
use poem::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

/// Build a Directed Graph from Nodes and Edges
pub fn build_graph<N, E>(nodes: Vec<N>, edges: Vec<(N, N, E)>) -> Result<DiGraph<N, E>, GraphError>
where
    N: Eq + Hash + Clone,
    E: Eq + Hash,
{
    // Dedup all nodes and edges
    let nodes: HashSet<N> = nodes.into_iter().collect();
    let edges: HashSet<(N, N, E)> = edges.into_iter().collect();

    // Our working data
    let mut graph: DiGraph<N, E> = DiGraph::new();
    let mut node_map: HashMap<N, NodeIndex> = HashMap::new();

    // Add Nodes to graph and map the nodes index to its value
    for node in nodes {
        let idx: NodeIndex = graph.try_add_node(node.clone())?;
        node_map.insert(node, idx);
    }

    // For each edge
    for (from, to, edge) in edges {
        // Lookup the node indexes to add to the graph
        let from_idx: Option<&NodeIndex> = node_map.get(&from);
        let to_idx: Option<&NodeIndex> = node_map.get(&to);

        // We should never miss hash table lookup if we have good edges
        if let (Some(&from_idx), Some(&to_idx)) = (from_idx, to_idx) {
            // Add edges to graph
            graph.try_add_edge(from_idx, to_idx, edge)?;
        } else {
            // We had an edge that was not covered by the nodes
            return Err(GraphError::NodeOutBounds);
        }
    }

    Ok(graph)
}

/// Check if a directed graph is also a dag
pub fn check_if_dag<N, E>(graph: &DiGraph<N, E>) -> Result<(), poem::Error> {
    // Is our graph a valid dag (aka not cyclical)?
    if is_cyclic_directed(graph) {
        return Err(poem::Error::from_string(
            "Dependency graph is cyclical, so invalid",
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }

    Ok(())
}
