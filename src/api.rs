use crate::core::{Compute, State, plan_dag_add};
use chrono::{DateTime, Utc};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{Object, OpenApi, Tags, payload::Json};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    Auth,
    #[oai(rename = "Plan DAG")]
    PlanDag,
    State,
}

/// Format we will receiving the Plan Dag
#[derive(Object)]
pub struct PlanDagApiParam {
    pub dataset_id: Uuid,
    pub paused: bool,
    pub extra: Value,
    pub data_products: Vec<DataProductApiParam>,
    pub dependencies: Vec<DependencyApiParam>,
}

/// Formate we will receive the Data Product
#[derive(Object)]
pub struct DataProductApiParam {
    pub data_product_id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Value,
}

/// Formate we will receive Dependencies between Data Products
#[derive(Object)]
pub struct DependencyApiParam {
    pub parent_id: String,
    pub child_id: String,
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
pub struct PlanDagApiResponse {
    dataset_id: Uuid,
    paused: bool,
    extra: Value,
    modified_by: String,
    modified_date: DateTime<Utc>,
    data_products: Vec<DataProductApiResponse>,
    dependency: Vec<DependencyApiResponse>,
}

/// Formate we will return the Data Product
#[derive(Object)]
struct DataProductApiResponse {
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
struct DependencyApiResponse {
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
        Json(param): Json<PlanDagApiParam>,
    ) -> Result<Json<PlanDagApiResponse>, poem::Error> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Add the plan dag to the DB
        let response: PlanDagApiResponse = plan_dag_add(&mut tx, &param, "dev_user").await?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        todo!()
    }
}
