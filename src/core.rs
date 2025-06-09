use crate::db::{PlanDagParam, plan_dag_select};
use chrono::{DateTime, Utc};
use poem::error::{InternalServerError, NotFound};
use poem_openapi::{Enum, Object};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
use uuid::Uuid;

/// Plan Dag Details
#[derive(Object)]
pub struct PlanDag {
    pub dataset: Dataset,
    pub data_products: Vec<DataProduct>,
    pub dependencies: Vec<Dependency>,
}

/// Dataset details
#[derive(Object)]
pub struct Dataset {
    pub id: Uuid,
    pub paused: bool,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// The Types of compute OaaS can call
#[derive(Clone, Copy, Enum, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "compute", rename_all = "lowercase")]
pub enum Compute {
    Cams,
    Dbxaas,
}

/// States a Data Product can be in
#[derive(Clone, Copy, Enum, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "state", rename_all = "lowercase")]
pub enum State {
    Waiting,
    Queued,
    Running,
    Success,
    Failed,
    Disabled,
}

/// Data Product details
#[derive(Object)]
pub struct DataProduct {
    pub id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// Dependency from one Data Product to another Data Product
#[derive(Object)]
pub struct Dependency {
    pub parent_id: String,
    pub child_id: String,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
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
        .map_err(InternalServerError)
}

/// Read a Plan Dag from the DB
pub async fn plan_dag_read(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
) -> Result<PlanDag, poem::Error> {
    // Map sqlx error to poem error
    match plan_dag_select(tx, dataset_id).await {
        Ok(plan_dag) => Ok(plan_dag),
        Err(sqlx::Error::RowNotFound) => Err(NotFound(sqlx::Error::RowNotFound)),
        Err(err) => Err(InternalServerError(err)),
    }
}
