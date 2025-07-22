use crate::model::{DataProductId, State};
use petgraph::graph::GraphError;
use poem::error::{BadRequest, Forbidden, InternalServerError, NotFound, UnprocessableEntity};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Custom Error Type for Fletcher
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dependency graph contains a cycle (not a valid DAG)
    #[error("The requested state for {0} is invalid: {1}")]
    BadState(DataProductId, State),

    /// The dependency graph contains a cycle (not a valid DAG)
    #[error("Graph is cyclical")]
    Cyclical,

    /// Data Product is locked
    #[error("Data product is locked: {0}")]
    Disabled(DataProductId),

    /// Duplicate data products in parameter
    #[error("Duplicate data-product id in parameter: {0}")]
    Duplicate(DataProductId),

    /// Duplicate dependencies in parameter
    #[error("Duplicate dependency in parameter: {0} -> {1}")]
    DuplicateDependencies(DataProductId, DataProductId),

    /// Error from Petgraph::Graph
    #[error("Petgraph error: {0}")]
    Graph(#[from] GraphError),

    /// Data Product not found
    #[error("Data product not found for: {0}")]
    Missing(DataProductId),

    /// Dataset Pause error
    #[error("Dataset '{0}' pause state is already set to: {1}")]
    Pause(DataProductId, bool),

    /// Errors from SQLx
    #[error("Error from SQLx: {0}")]
    Sqlx(#[from] sqlx::Error),
}

impl Error {
    /// Map Crate Error to Poem Error
    pub fn into_poem_error(self) -> poem::Error {
        match self {
            Error::BadState(_, _) => BadRequest(self),
            Error::Disabled(_) => Forbidden(self),
            Error::Duplicate(_) => BadRequest(self),
            Error::DuplicateDependencies(_, _) => BadRequest(self),
            Error::Cyclical => UnprocessableEntity(self),
            Error::Graph(error) => InternalServerError(error),
            Error::Missing(_) => NotFound(self),
            Error::Pause(_, _) => BadRequest(self),
            Error::Sqlx(sqlx::Error::RowNotFound) => NotFound(sqlx::Error::RowNotFound),
            Error::Sqlx(sqlx::Error::Database(error)) if error.constraint().is_some() => {
                BadRequest(error)
            }
            Error::Sqlx(error) => InternalServerError(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use poem::http::StatusCode;
    use uuid::Uuid;

    /// Test into_poem_error maps BadState to BadRequest
    #[test]
    fn test_into_poem_error_bad_state() {
        let data_product_id = Uuid::new_v4();
        let error = Error::BadState(data_product_id, State::Success);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::BAD_REQUEST,
            "BadState error should map to BadRequest"
        );
    }

    /// Test into_poem_error maps Cyclical to UnprocessableEntity
    #[test]
    fn test_into_poem_error_cyclical() {
        let error = Error::Cyclical;
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "Cyclical error should map to UnprocessableEntity"
        );
    }

    /// Test into_poem_error maps Disabled to Forbidden
    #[test]
    fn test_into_poem_error_disabled() {
        let data_product_id = Uuid::new_v4();
        let error = Error::Disabled(data_product_id);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::FORBIDDEN,
            "Disabled error should map to Forbidden"
        );
    }

    /// Test into_poem_error maps Duplicate to BadRequest
    #[test]
    fn test_into_poem_error_duplicate() {
        let data_product_id = Uuid::new_v4();
        let error = Error::Duplicate(data_product_id);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::BAD_REQUEST,
            "Duplicate error should map to BadRequest"
        );
    }

    /// Test into_poem_error maps DuplicateDependencies to BadRequest
    #[test]
    fn test_into_poem_error_duplicate_dependencies() {
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let error = Error::DuplicateDependencies(parent_id, child_id);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::BAD_REQUEST,
            "DuplicateDependencies error should map to BadRequest"
        );
    }

    /// Test into_poem_error maps Graph error to InternalServerError
    #[test]
    fn test_into_poem_error_graph() {
        let error = Error::Graph(GraphError::NodeOutBounds);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "Graph error should map to InternalServerError"
        );
    }

    /// Test into_poem_error maps Missing to NotFound
    #[test]
    fn test_into_poem_error_missing() {
        let data_product_id = Uuid::new_v4();
        let error = Error::Missing(data_product_id);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::NOT_FOUND,
            "Missing error should map to NotFound"
        );
    }

    /// Test into_poem_error maps Pause to BadRequest
    #[test]
    fn test_into_poem_error_pause() {
        let data_product_id = Uuid::new_v4();
        let error = Error::Pause(data_product_id, true);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::BAD_REQUEST,
            "Pause error should map to BadRequest"
        );
    }

    /// Test into_poem_error maps Sqlx RowNotFound to NotFound
    #[test]
    fn test_into_poem_error_sqlx_row_not_found() {
        let error = Error::Sqlx(sqlx::Error::RowNotFound);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::NOT_FOUND,
            "Sqlx RowNotFound error should map to NotFound"
        );
    }
}
