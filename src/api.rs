use crate::{
    core::{PlanDag, plan_dag_add},
    db::PlanDagParam,
};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{OpenApi, Tags, payload::Json};
use sqlx::PgPool;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    Auth,
    #[oai(rename = "Plan DAG")]
    PlanDag,
    State,
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
        Json(plan_dag): Json<PlanDagParam>,
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
