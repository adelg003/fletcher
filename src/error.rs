use crate::model::DataProductId;
use petgraph::graph::GraphError;
use thiserror::Error;

/// Result Type we will for Fletcher
pub type Result<T> = std::result::Result<T, Error>;

/// Custom Error Type for Fletcher
#[derive(Debug, Error)]
pub enum Error {
    /// Graph that as cyclical, aka not valid dag
    #[error("Graph is cyclical")]
    Cyclical,

    /// Duplicate data products in parameter
    #[error("Duplicate data products ids in paramiter: {0}")]
    DuplicateDataProduct(DataProductId),

    /// Duplicate dependencies in parameter
    #[error("Duplicate dependencies in paramiter: {0} => {1}")]
    DuplicateDependencies(DataProductId, DataProductId),

    /// Error from Petgraph
    #[error("Error from Petgraph: {0}")]
    Graph(#[from] GraphError),

    /// Data Product not found
    #[error("Data product not found for: {0}")]
    MissingDataProduct(DataProductId),

    /// Errors from SQLx
    #[error("Error from SQLx: {0}")]
    Sqlx(#[from] sqlx::Error),
}
