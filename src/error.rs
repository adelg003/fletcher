use crate::{
    auth::Role,
    model::{DataProductId, State},
};
use bcrypt::BcryptError;
use jsonwebtoken::errors::Error as JwtError;
use petgraph::graph::GraphError;
use poem::error::{
    BadRequest, Forbidden, InternalServerError, NotFound, Unauthorized, UnprocessableEntity,
};

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Custom Error Type for Fletcher
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dependency graph contains a cycle (not a valid DAG)
    #[error("The requested state for '{0}' is invalid: '{1}'")]
    BadState(DataProductId, State),

    /// Error from
    #[error(transparent)]
    Bcrypt(#[from] BcryptError),

    /// The dependency graph contains a cycle (not a valid DAG)
    #[error("Graph is cyclical")]
    Cyclical,

    /// Data Product is locked
    #[error("Data product is locked: '{0}'")]
    Disabled(DataProductId),

    /// Duplicate data products in parameter
    #[error("Duplicate data-product id in parameter: '{0}'")]
    Duplicate(DataProductId),

    /// Duplicate dependencies in parameter
    #[error("Duplicate dependency in parameter: '{0}' -> '{1}'")]
    DuplicateDependencies(DataProductId, DataProductId),

    /// Error from Petgraph::Graph
    #[error(transparent)]
    Graph(#[from] GraphError),

    /// Trying to log in with an invalid key
    #[error("Attepting to log in with an invalid key")]
    InvalidKey,

    /// Trying to log in as an invalid service
    #[error("Attepting to log in as a unknown service: '{0}'")]
    InvalidService(String),

    /// Error from JsonWebToken
    #[error(transparent)]
    Jwt(#[from] JwtError),

    /// Data Product not found
    #[error("Data product not found for: '{0}'")]
    Missing(DataProductId),

    /// Dataset Pause error
    #[error("Dataset '{0}' pause state is already set to: '{1}'")]
    Pause(DataProductId, bool),

    /// Missing the needed role access
    #[error("Service account '{0}' is missing the following role: '{1}'")]
    Role(String, Role),

    /// Errors from SQLx
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// This error should never be reachable
    #[error("This error should not have been reachable")]
    Unreachable,
}

impl Error {
    /// Convert Fletcher Error to Poem Error
    pub fn into_poem_error(self) -> poem::Error {
        match self {
            Error::BadState(_, _) => BadRequest(self),
            Error::Bcrypt(err) => InternalServerError(err),
            Error::Disabled(_) => Forbidden(self),
            Error::Duplicate(_) => BadRequest(self),
            Error::DuplicateDependencies(_, _) => BadRequest(self),
            Error::Cyclical => UnprocessableEntity(self),
            Error::Graph(err) => InternalServerError(err),
            Error::InvalidKey => Unauthorized(self),
            Error::InvalidService(_) => Unauthorized(self),
            Error::Jwt(err) => Forbidden(err),
            Error::Missing(_) => NotFound(self),
            Error::Pause(_, _) => BadRequest(self),
            Error::Role(_, _) => Forbidden(self),
            Error::Sqlx(sqlx::Error::RowNotFound) => NotFound(sqlx::Error::RowNotFound),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some() => {
                BadRequest(err)
            }
            Error::Sqlx(err) => InternalServerError(err),
            Error::Unreachable => InternalServerError(self),
        }
    }
}

/// Convert Fletcher Error to Poem Error
pub fn into_poem_error(err: Error) -> poem::Error {
    err.into_poem_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use poem::http::StatusCode;
    use pretty_assertions::assert_eq;
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

    /// Test into_poem_error maps Bcrypt error to InternalServerError
    #[test]
    fn test_into_poem_error_bcrypt() {
        let bcrypt_error = BcryptError::InvalidCost("5".to_string());
        let error = Error::Bcrypt(bcrypt_error);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "Bcrypt error should map to InternalServerError"
        );
    }

    /// Test into_poem_error maps InvalidKey to Unauthorized
    #[test]
    fn test_into_poem_error_invalid_key() {
        let error = Error::InvalidKey;
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::UNAUTHORIZED,
            "InvalidKey error should map to Unauthorized"
        );
    }

    /// Test into_poem_error maps InvalidService to Unauthorized
    #[test]
    fn test_into_poem_error_invalid_service() {
        let error = Error::InvalidService("test_service".to_string());
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::UNAUTHORIZED,
            "InvalidService error should map to Unauthorized"
        );
    }

    /// Test into_poem_error maps Jwt error to Forbidden
    #[test]
    fn test_into_poem_error_jwt() {
        // Create a JWT error by trying to decode an invalid token
        let config = crate::load_config().unwrap();
        let jwt_error = jsonwebtoken::decode::<serde_json::Value>(
            "invalid.jwt.token",
            &config.decoding_key,
            &jsonwebtoken::Validation::default(),
        )
        .unwrap_err();

        let error = Error::Jwt(jwt_error);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::FORBIDDEN,
            "Jwt error should map to Forbidden"
        );
    }

    /// Test into_poem_error maps Role error to Forbidden
    #[test]
    fn test_into_poem_error_role() {
        let error = Error::Role("test_service".to_string(), Role::Publish);
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::FORBIDDEN,
            "Role error should map to Forbidden"
        );
    }

    /// Test into_poem_error maps Unreachable to InternalServerError
    #[test]
    fn test_into_poem_error_unreachable() {
        let error = Error::Unreachable;
        let poem_error = error.into_poem_error();
        assert_eq!(
            poem_error.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unreachable error should map to InternalServerError"
        );
    }
}
