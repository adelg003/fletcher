use crate::error::{Error, Result};
use petgraph::{
    algo::is_cyclic_directed,
    graph::{DiGraph, GraphError, NodeIndex},
};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::Sized,
};

/// Trait for building graphs that are valid dags
pub trait Dag<N, E> {
    /// Build a Directed Graph from Nodes and Edges and ensure it is acyclic
    fn build_dag(nodes: HashSet<N>, edges: HashSet<(N, N, E)>) -> Result<Self>
    where
        Self: Sized;
}

impl<N, E> Dag<N, E> for DiGraph<N, E>
where
    N: Eq + Hash + Clone,
    E: Eq + Hash,
{
    fn build_dag(nodes: HashSet<N>, edges: HashSet<(N, N, E)>) -> Result<Self> {
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
                return Err(GraphError::NodeOutBounds.into());
            }
        }

        // Is our graph a valid dag (aka not cyclical)?
        validate_acyclic(&graph)?;

        Ok(graph)
    }
}

/// Check if a directed graph is also a dag
fn validate_acyclic<N, E>(graph: &DiGraph<N, E>) -> Result<()> {
    // Is our graph a valid dag (aka not cyclical)?
    if is_cyclic_directed(graph) {
        Err(Error::Cyclical)
    } else {
        Ok(())
    }
}
