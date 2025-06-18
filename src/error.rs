use crate::model::DataProductId;
use petgraph::graph::GraphError;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Custom Error Type for Fletcher
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dependency graph contains a cycle (not a valid DAG)
    #[error("Graph is cyclical")]
    Cyclical,

    /// Duplicate data products in parameter
    #[error("Duplicate data-product id in parameter: {0}")]
    Duplicate(DataProductId),

    /// Duplicate dependencies in parameter
    #[error("Duplicate dependency in parameter: {0} -> {1}")]
    DuplicateDependencies(DataProductId, DataProductId),

    /// Error from Petgraph::Graph
    #[error("Petgraph error: {0}")]
    Graph(#[from] GraphError),

    /// Data Product is locked
    #[error("Data product is locked: {0}")]
    Locked(DataProductId),

    /// Data Product not found
    #[error("Data product not found for: {0}")]
    Missing(DataProductId),

    /// Errors from SQLx
    #[error("Error from SQLx: {0}")]
    Sqlx(#[from] sqlx::Error),
}
