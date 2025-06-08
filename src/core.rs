use chrono::{DateTime, Utc};
use poem_openapi::Enum;
use serde_json::Value;
use sqlx::Type;
use uuid::Uuid;

/// Plan Dag Details
struct PlanDag {
    dataset: Dataset,
    data_products: Vec<DataProduct>,
    dependencies: Vec<Dependency>,
}

/// Dataset details
pub struct Dataset {
    pub dataset_id: Uuid,
    pub paused: bool,
    pub extra: Value,
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
pub struct DataProduct {
    pub dataset_id: Uuid,
    pub data_product_id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Value,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Value,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// Dependency from one Data Product to another Data Product
pub struct Dependency {
    pub dataset_id: Uuid,
    pub parent_id: String,
    pub child_id: String,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}
