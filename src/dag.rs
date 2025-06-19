use crate::error::{Error, Result};
use petgraph::{
    algo::is_cyclic_directed,
    graph::{DiGraph, GraphError, NodeIndex},
    visit::Dfs,
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

    /// Return all nodes downstream of a given node.
    fn downstream_nodes(&self, start_node: N) -> HashSet<N>;
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

    fn downstream_nodes(&self, start_node: N) -> HashSet<N> {
        // Nodes we have already sean
        let mut visited = HashSet::<N>::new();

        // Find the index of the node we want to start with
        let start_idx: Option<NodeIndex> = self
            .node_indices()
            .find(|idx: &NodeIndex| self.node_weight(*idx) == Some(&start_node));

        // If our nodes is not in the graph, just return an empty set.
        let start_idx: NodeIndex = match start_idx {
            Some(start_idx) => start_idx,
            None => return visited,
        };

        // Lets iterate over all the downstream nodes via a depth first search (DFS)
        let mut dfs = Dfs::new(self, start_idx);
        while let Some(idx) = dfs.next(self) {
            // Skip the first node
            if idx != start_idx {
                // Add node weight to ones we have seen
                if let Some(node) = self.node_weight(idx) {
                    visited.insert(node.clone());
                }
            }
        }

        // Return list of all nodes we have visited on our walk down the graph
        visited
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
