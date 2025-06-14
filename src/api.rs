use crate::{
    core::{plan_dag_add, plan_dag_read},
    model::{Plan, PlanParam},
};
use poem::{
    error::{InternalServerError},
    web::Data,
};
use poem_openapi::{OpenApi, Tags, param::Path, payload::Json};
use sqlx::PgPool;
use uuid::Uuid;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    Auth,
    #[oai(rename = "Plan DAG")]
    Plan,
    State,
}

/// Struct we will use to build our REST API
pub struct Api;

#[OpenApi]
impl Api {
    /// Register a Plan DAG
    #[oai(path = "/plan", method = "post", tag = Tag::Plan)]
    async fn plan_dag_post(
        &self,
        Data(pool): Data<&PgPool>,
        Json(plan_dag): Json<PlanParam>,
    ) -> poem::Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Add the plan dag to the DB
        //TODO remove "dev_user"
        let plan_dag: Plan = plan_dag_add(&mut tx, plan_dag, "dev_user").await?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan_dag))
    }

    /// Read a Plan DAG
    #[oai(path = "/plan/:dataset_id", method = "get", tag = Tag::Plan)]
    async fn plan_dag_get(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<Uuid>,
    ) -> poem::Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Read the plan dag from the DB
        let plan_dag: Plan = plan_dag_read(&mut tx, dataset_id).await?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(plan_dag))
    }
}
