use crate::{
    api::{DataProductApiParam, DependencyApiParam, PlanDagApiParam, PlanDagApiResponse},
    db::{
        DataProductDbParam, DatasetDbParam, DependencyDbParam, data_product_upsert, dataset_upsert,
    },
};
use chrono::{DateTime, Utc};
use poem::error::InternalServerError;
use poem_openapi::Enum;
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
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

/// Add a Plan Dag to the DB
pub async fn plan_dag_add(
    tx: &mut Transaction<'_, Postgres>,
    plan_dag_api: &PlanDagApiParam,
    username: &str,
) -> Result<PlanDagApiResponse, poem::Error> {
    let dataset_id: Uuid = plan_dag_api.dataset_id;

    // Map Plan Dag Params to DB structs
    let dataset_param = DatasetDbParam::from(plan_dag_api);
    let data_product_params: Vec<DataProductDbParam> = plan_dag_api
        .data_products
        .iter()
        .map(|dataset_param: &DataProductApiParam| {
            DataProductDbParam::from_api(&dataset_id, dataset_param)
        })
        .collect();
    let dependencies_params: Vec<DependencyDbParam> = plan_dag_api
        .dependencies
        .iter()
        .map(|dependency: &DependencyApiParam| DependencyDbParam::from_api(&dataset_id, dependency))
        .collect();

    // Write our data to the DB
    let dataset: Dataset = dataset_upsert(tx, &dataset_param, username)
        .await
        .map_err(InternalServerError)?;

    //let data_product_futures = data_product_params
    //    .iter()
    //    .map(|data_product| data_product_upsert(tx, data_product, username));

    todo!()
}
