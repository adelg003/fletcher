use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Type;
use uuid::Uuid;

/// Dataset details
pub struct Dataset {
    dataset_id: Uuid,
    paused: bool,
    modified_by: String,
    modified_date: DateTime<Utc>,
}

/// The Types of compute OaaS can call
#[derive(Clone, Copy, Type)]
#[sqlx(type_name = "compute", rename_all = "lowercase")]
pub enum Compute {
    Cams,
    Dbxaas,
}

/// States a Data Product can be in
#[derive(Clone, Copy, Type)]
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
    dataset_id: Uuid,
    data_product_id: String,
    compute: Compute,
    name: String,
    version: String,
    eager: bool,
    passthrough: Value,
    state: State,
    run_id: Option<Uuid>,
    link: Option<String>,
    passback: Value,
    modified_by: String,
    modified_date: DateTime<Utc>,
}

/// Dependency from one Data Product to another Data Product
pub struct Dependency {
    dataset_id: Uuid,
    parent_id: String,
    child_id: String,
    modified_by: String,
    modified_date: DateTime<Utc>,
}
