use crate::core::{Compute, PlanDag, State, plan_dag_add};
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
    pub dataset: DatasetApiParam,
    pub data_products: Vec<DataProductApiParam>,
    pub dependencies: Vec<DependencyApiParam>,
}

/// Format we will receiving the Dataset
#[derive(Object)]
pub struct DatasetApiParam {
    pub dataset_id: Uuid,
    pub paused: bool,
    pub extra: Option<Value>,
}

/// Formate we will receive the Data Product
#[derive(Object)]
pub struct DataProductApiParam {
    pub data_product_id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub extra: Option<Value>,
}

/// Formate we will receive Dependencies between Data Products
#[derive(Object)]
pub struct DependencyApiParam {
    pub parent_id: String,
    pub child_id: String,
    pub extra: Option<Value>,
}

/// Formate we will receive updates to State
struct StateApiParam {
    data_product_id: String,
    state: State,
    run_id: Option<Uuid>,
    link: Option<String>,
    passback: Option<Value>,
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
    ) -> Result<Json<PlanDag>, poem::Error> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Add the plan dag to the DB
        let plan_dag: PlanDag = plan_dag_add(&mut tx, plan_dag, "dev_user").await?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan_dag))
    }
}
