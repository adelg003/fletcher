use crate::db::{plan_dag_select, plan_dag_upsert};
use chrono::{DateTime, Utc};
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

impl PlanDag {
    /// Pull the Plan Dag for a Dataset
    pub async fn from_dataset_id(
        id: Uuid,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Self, sqlx::Error> {
        plan_dag_select(tx, id).await
    }
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

/// Input for a Plan Dag
#[derive(Object)]
pub struct PlanDagParam {
    pub dataset: DatasetParam,
    pub data_products: Vec<DataProductParam>,
    pub dependencies: Vec<DependencyParam>,
}

impl PlanDagParam {
    /// Write the Plan Dag to the DB
    pub async fn upsert(
        self,
        tx: &mut Transaction<'_, Postgres>,
        username: &str,
    ) -> Result<PlanDag, sqlx::Error> {
        plan_dag_upsert(tx, self, username).await
    }
}

/// Input parameters for a Dataset
#[derive(Object)]
pub struct DatasetParam {
    pub id: Uuid,
    pub paused: bool,
    pub extra: Option<Value>,
}

/// Input parameters for a Data Product
#[derive(Object)]
pub struct DataProductParam {
    pub id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub extra: Option<Value>,
}

/// Input parameters for State
pub struct StateParam {
    pub dataset_id: Uuid,
    pub data_product_id: String,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
    pub extra: Option<Value>,
}

/// Input for adding a Dependency
#[derive(Object)]
pub struct DependencyParam {
    pub parent_id: String,
    pub child_id: String,
    pub extra: Option<Value>,
}
