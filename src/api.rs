use crate::core::{Compute, State};
use chrono::{DateTime, Utc};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{Object, OpenApi, Tags, payload::Json};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Tags)]
pub enum Tag {
    Auth,
    #[oai(rename = "Plan DAG")]
    PlanDag,
    State,
}

/// Format we will receiving the Plan Dag
#[derive(Object)]
struct PlanDagApiParam {
    dataset_id: Uuid,
    paused: bool,
    extra: Value,
    data_products: Vec<DataProductApiParam>,
    dependencies: Vec<DependencyApiParam>,
}

/// Formate we will receive the Data Product
#[derive(Object)]
struct DataProductApiParam {
    data_product_id: String,
    compute: Compute,
    name: String,
    version: String,
    eager: bool,
    passthrough: Value,
}

/// Formate we will receive Dependencies between Data Products
#[derive(Object)]
struct DependencyApiParam {
    parent_id: String,
    child_id: String,
}

/// Formate we will receive updates to State
struct StateApiParam {
    data_product_id: String,
    state: State,
    run_id: Option<Uuid>,
    link: Option<String>,
    passback: Value,
}

/// Format we will return the Plan Dag
#[derive(Object)]
struct PlanDagApiReturn {
    dataset_id: Uuid,
    paused: bool,
    extra: Value,
    modified_by: String,
    modified_date: DateTime<Utc>,
    data_products: Vec<DataProductApiReturn>,
    dependency: Vec<DependencyApiReturn>,
}

/// Formate we will return the Data Product
#[derive(Object)]
struct DataProductApiReturn {
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

/// Formate we will receive Dependencies between Data Products
#[derive(Object)]
struct DependencyApiReturn {
    parent_id: String,
    child_id: String,
    modified_by: String,
    modified_date: DateTime<Utc>,
}

/// Struct we will use to build our REST API
pub struct Api;

#[OpenApi]
impl Api {
    /// Register a Plan DAG
    #[oai(path = "/plan_dag", method = "post", tag = Tag::PlanDag)]
    async fn plan_dag_post(
        &self,
        Data(pool): Data<&PgPool>,
        Json(plan_dag): Json<PlanDagApiParam>,
    ) -> Result<Json<PlanDagApiReturn>, poem::Error> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        todo!()
    }
}
