use crate::error::{Error, Result};
use petgraph::{
    Direction,
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

    /// Check if a directed graph is also a dag
    fn validate_acyclic(&self) -> Result<()>;

    /// Find the index in a graph for a node value
    fn find_node_index(&self, node: N) -> Option<NodeIndex>;

    /// Return all nodes downstream of a given node.
    fn downstream_nodes(&self, start_node: N) -> HashSet<N>;

    /// Return nodes that lead into the target node
    fn parent_nodes(&self, target: N) -> HashSet<N>;
}

impl<N, E> Dag<N, E> for DiGraph<N, E>
where
    N: Eq + Hash + Copy,
    E: Eq + Hash,
{
    fn build_dag(nodes: HashSet<N>, edges: HashSet<(N, N, E)>) -> Result<Self> {
        // Our working data
        let mut graph: DiGraph<N, E> = DiGraph::new();
        let mut node_map: HashMap<N, NodeIndex> = HashMap::new();

        // Add Nodes to graph and map the nodes index to its value
        for node in nodes {
            let idx: NodeIndex = graph.try_add_node(node)?;
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
        graph.validate_acyclic()?;

        Ok(graph)
    }

    fn validate_acyclic(&self) -> Result<()> {
        // Is our graph a valid dag (aka not cyclical)?
        if is_cyclic_directed(self) {
            Err(Error::Cyclical)
        } else {
            Ok(())
        }
    }

    fn find_node_index(&self, node: N) -> Option<NodeIndex> {
        self.node_indices()
            .find(|idx: &NodeIndex| self.node_weight(*idx) == Some(&node))
    }

    fn downstream_nodes(&self, start_node: N) -> HashSet<N> {
        // Nodes we have already seen
        let mut visited = HashSet::<N>::new();

        // Find the index of the node we want to start with
        let start_idx: NodeIndex = match self.find_node_index(start_node) {
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
                    visited.insert(*node);
                }
            }
        }

        // Return list of all nodes we have visited on our walk down the graph
        visited
    }

    fn parent_nodes(&self, target_node: N) -> HashSet<N> {
        // Find where our target lives in the graph
        let target_idx: NodeIndex = match self.find_node_index(target_node) {
            Some(target_idx) => target_idx,
            None => return HashSet::new(),
        };

        // Return the "incoming neighbors" (aka parents) for the node
        self.neighbors_directed(target_idx, Direction::Incoming)
            .filter_map(|parent_idx: NodeIndex| self.node_weight(parent_idx))
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Test building a valid DAG with nodes and edges
    #[test]
    fn test_build_dag_success() {
        // Test: Can we build a dag?
        let nodes: HashSet<u32> = [1, 2, 3, 4].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [(1, 2, "edge1"), (2, 3, "edge2"), (2, 4, "edge3")]
            .iter()
            .copied()
            .collect();

        let result = DiGraph::build_dag(nodes, edges);
        assert!(result.is_ok());

        let dag = result.unwrap();
        assert_eq!(dag.node_count(), 4);
        assert_eq!(dag.edge_count(), 3);
    }

    /// Test rejection of self-loop edges
    #[test]
    fn test_build_dag_rejects_self_loop() {
        // Test: Do we reject if an edge has the same parent and child?
        let nodes: HashSet<u32> = [1, 2].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 1, "self_loop"), // Self-loop
        ]
        .iter()
        .copied()
        .collect();

        let result = DiGraph::build_dag(nodes, edges);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Cyclical));
    }

    /// Test rejection of cyclical graphs
    #[test]
    fn test_build_dag_rejects_cycle() {
        // Test: Do we reject dags that are cyclical?
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 2, "edge1"),
            (2, 3, "edge2"),
            (3, 1, "edge3"), // Creates a cycle
        ]
        .iter()
        .copied()
        .collect();

        let result = DiGraph::build_dag(nodes, edges);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Cyclical));
    }

    /// Test handling of invalid edge references
    #[test]
    fn test_build_dag_invalid_edge() {
        // Test edge references non-existent node
        let nodes: HashSet<u32> = [1, 2].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 3, "invalid_edge"), // Node 3 doesn't exist
        ]
        .iter()
        .copied()
        .collect();

        let result = DiGraph::build_dag(nodes, edges);
        assert!(result.is_err());
    }

    /// Test finding index of existing node
    #[test]
    fn test_find_node_index_existing_node() {
        // Test: If given a node value, can we find its index in the graph?
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [(1, 2, "edge1")].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let index = dag.find_node_index(2);

        assert!(index.is_some());

        // Verify the index actually points to the correct node
        let found_index = index.unwrap();
        assert_eq!(dag.node_weight(found_index), Some(&2));
    }

    /// Test finding index of non-existing node
    #[test]
    fn test_find_node_index_non_existing_node() {
        let nodes: HashSet<u32> = [1, 2].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let index = dag.find_node_index(5); // Node 5 doesn't exist
        assert!(index.is_none());
    }

    /// Test downstream nodes from simple tree structure
    #[test]
    fn test_downstream_nodes_simple_tree() {
        // Test: If given a dag, can we return all the children and subsequent generations from the start_node?
        let nodes: HashSet<u32> = [1, 2, 3, 4, 5].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 2, "edge1"),
            (1, 3, "edge2"),
            (2, 4, "edge3"),
            (3, 5, "edge4"),
        ]
        .iter()
        .copied()
        .collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let downstream = dag.downstream_nodes(1);
        let expected: HashSet<u32> = [2, 3, 4, 5].iter().copied().collect();
        assert_eq!(downstream, expected);
    }

    /// Test downstream nodes from complex DAG structure
    #[test]
    fn test_downstream_nodes_complex_dag() {
        // Test with a more complex DAG structure
        let nodes: HashSet<u32> = [1, 2, 3, 4, 5, 6].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 2, "edge1"),
            (1, 3, "edge2"),
            (2, 4, "edge3"),
            (3, 4, "edge4"), // Multiple paths to node 4
            (4, 5, "edge5"),
            (3, 6, "edge6"),
        ]
        .iter()
        .copied()
        .collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let downstream = dag.downstream_nodes(1);
        let expected: HashSet<u32> = [2, 3, 4, 5, 6].iter().copied().collect();
        assert_eq!(downstream, expected);

        // Test from intermediate node
        let downstream_from_3 = dag.downstream_nodes(3);
        let expected_from_3: HashSet<u32> = [4, 5, 6].iter().copied().collect();
        assert_eq!(downstream_from_3, expected_from_3);
    }

    /// Test downstream nodes from leaf node
    #[test]
    fn test_downstream_nodes_leaf_node() {
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> =
            [(1, 2, "edge1"), (2, 3, "edge2")].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let downstream = dag.downstream_nodes(3); // Leaf node
        assert!(downstream.is_empty());
    }

    /// Test downstream nodes for non-existing node
    #[test]
    fn test_downstream_nodes_non_existing_node() {
        let nodes: HashSet<u32> = [1, 2].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let downstream = dag.downstream_nodes(5); // Non-existing node
        assert!(downstream.is_empty());
    }

    /// Test parent nodes with multiple parents
    #[test]
    fn test_parent_nodes_simple_case() {
        // Test: Can we return the parents of a node, and just the direct parents?
        let nodes: HashSet<u32> = [1, 2, 3, 4].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [
            (1, 3, "edge1"),
            (2, 3, "edge2"), // Multiple parents for node 3
            (3, 4, "edge3"),
        ]
        .iter()
        .copied()
        .collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let parents = dag.parent_nodes(3);
        let expected: HashSet<u32> = [1, 2].iter().copied().collect();
        assert_eq!(parents, expected);
    }

    /// Test parent nodes with single parent
    #[test]
    fn test_parent_nodes_single_parent() {
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> =
            [(1, 2, "edge1"), (2, 3, "edge2")].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let parents = dag.parent_nodes(2);
        let expected: HashSet<u32> = [1].iter().copied().collect();
        assert_eq!(parents, expected);
    }

    /// Test parent nodes for root node
    #[test]
    fn test_parent_nodes_root_node() {
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> =
            [(1, 2, "edge1"), (1, 3, "edge2")].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let parents = dag.parent_nodes(1); // Root node has no parents
        assert!(parents.is_empty());
    }

    /// Test parent nodes for non-existing node
    #[test]
    fn test_parent_nodes_non_existing_node() {
        let nodes: HashSet<u32> = [1, 2].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> = [].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();

        let parents = dag.parent_nodes(5); // Non-existing node
        assert!(parents.is_empty());
    }

    /// Test validation of acyclic graph
    #[test]
    fn test_validate_acyclic_valid_dag() {
        let nodes: HashSet<u32> = [1, 2, 3].iter().copied().collect();
        let edges: HashSet<(u32, u32, &str)> =
            [(1, 2, "edge1"), (2, 3, "edge2")].iter().copied().collect();

        let dag = DiGraph::build_dag(nodes, edges).unwrap();
        assert!(dag.validate_acyclic().is_ok());
    }
}
