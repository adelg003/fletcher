use crate::model::{PlanDag, PlanDagParam};
use poem::error::{BadRequest, InternalServerError, NotFound};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Map SQLx to Poem Errors
pub fn sqlx_to_poem_error(err: sqlx::Error) -> poem::Error {
    match err {
        sqlx::Error::RowNotFound => NotFound(err),
        sqlx::Error::Database(err) if err.constraint().is_some() => BadRequest(err),
        err => InternalServerError(err),
    }
}

/// Add a Plan Dag to the DB
pub async fn plan_dag_add(
    tx: &mut Transaction<'_, Postgres>,
    plan_dag_param: PlanDagParam,
    username: &str,
) -> Result<PlanDag, poem::Error> {
    // Write our Plan Dag to the DB
    plan_dag_param
        .upsert(tx, username)
        .await
        .map_err(sqlx_to_poem_error)
}

/// Read a Plan Dag from the DB
pub async fn plan_dag_read(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<PlanDag, poem::Error> {
    PlanDag::from_dataset_id(id, tx)
        .await
        .map_err(sqlx_to_poem_error)
}
#[cfg(test)]
mod tests {
    use super::*;
    use poem::http::StatusCode;
    use sqlx::Error as SqlxError;
    use uuid::Uuid;
    use std::borrow::Cow;

    #[test]
    fn test_sqlx_to_poem_error_row_not_found() {
        let sqlx_error = SqlxError::RowNotFound;
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_sqlx_to_poem_error_database_constraint_violation() {
        // Create a database error that represents a constraint violation
        let sqlx_error = SqlxError::Database(Box::new(TestDatabaseError::new(
            "constraint violation",
            Some("unique_constraint".to_string()),
        )));
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_sqlx_to_poem_error_database_no_constraint() {
        // Create a database error without constraint (should be internal server error)
        let sqlx_error = SqlxError::Database(Box::new(TestDatabaseError::new(
            "some database error",
            None,
        )));
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_sqlx_to_poem_error_configuration_error() {
        let sqlx_error = SqlxError::Configuration("test configuration error".into());
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_sqlx_to_poem_error_decode_error() {
        let sqlx_error = SqlxError::Decode("decode error".into());
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_sqlx_to_poem_error_protocol_error() {
        let sqlx_error = SqlxError::Protocol("protocol error".into());
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_sqlx_to_poem_error_column_not_found() {
        let sqlx_error = SqlxError::ColumnNotFound("missing_column".to_string());
        let poem_error = sqlx_to_poem_error(sqlx_error);
        assert_eq!(poem_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Mock database error for testing
    #[derive(Debug)]
    struct TestDatabaseError {
        message: String,
        constraint_name: Option<String>,
    }

    impl TestDatabaseError {
        fn new(message: &str, constraint: Option<String>) -> Self {
            Self {
                message: message.to_string(),
                constraint_name: constraint,
            }
        }
    }

    impl std::fmt::Display for TestDatabaseError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl std::error::Error for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            &self.message
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some("23000".into()) // Generic constraint violation code
        }
        fn details(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn hint(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn table(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn column(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn constraint(&self) -> Option<Cow<'_, str>> {
            self.constraint_name.as_ref().map(|s| Cow::Borrowed(s.as_str()))
        }
    }

    // Integration-style tests belong in the `tests/` directory with proper DB setup.
}