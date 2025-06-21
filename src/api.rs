use crate::{
    core::{plan_add, plan_read, states_edit},
    model::{DatasetId, Plan, PlanParam, StateParam},
};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{OpenApi, Tags, param::Path, payload::Json};
use sqlx::PgPool;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    Plan,
    State,
}

/// Struct we will use to build our REST API
pub struct Api;

#[OpenApi]
impl Api {
    /// Register a Plan
    #[oai(path = "/plan", method = "post", tag = Tag::Plan)]
    async fn plan_post(
        &self,
        Data(pool): Data<&PgPool>,
        Json(plan): Json<PlanParam>,
    ) -> poem::Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Add the plan to the DB
        let plan: Plan = plan_add(&mut tx, &plan, "placeholder_user").await?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Read a Plan
    #[oai(path = "/plan/:dataset_id", method = "get", tag = Tag::Plan)]
    async fn plan_get(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
    ) -> poem::Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Read the plan from the DB
        let plan: Plan = plan_read(&mut tx, dataset_id).await?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Update one or multiple data product states
    #[oai(path = "/data_product/:dataset_id", method = "put", tag = Tag::State)]
    async fn state_put(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
        Json(states): Json<Vec<StateParam>>,
    ) -> poem::Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Update data product states and return the updated plan
        let plan: Plan = states_edit(&mut tx, dataset_id, &states, "placeholder_user").await?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }
}
