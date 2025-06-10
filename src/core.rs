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
