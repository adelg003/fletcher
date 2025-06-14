use crate::{
    core::{plan_dag_add, plan_dag_read},
    model::{Plan, PlanParam},
};
use poem::{
    error::{InternalServerError, Result},
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
    #[oai(path = "/plan_dag", method = "post", tag = Tag::Plan)]
    /// Creates a new plan DAG entry in the database and returns the created plan.
    ///
    /// Accepts a JSON payload describing the plan DAG, inserts it into the database within a transaction, and returns the resulting plan as JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage within a Poem test context
    /// let api = Api::default();
    /// let plan_param = PlanParam { /* fields */ };
    /// let response = api.plan_dag_post(Data(pool), Json(plan_param)).await.unwrap();
    /// assert_eq!(response.0.id, /* expected id */);
    /// ```
    async fn plan_dag_post(
        &self,
        Data(pool): Data<&PgPool>,
        Json(plan_dag): Json<PlanParam>,
    ) -> Result<Json<Plan>> {
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
    #[oai(path = "/plan_dag/:dataset_id", method = "get", tag = Tag::Plan)]
    /// Retrieves a plan DAG by dataset ID.
    ///
    /// Initiates a database transaction to fetch the plan DAG associated with the specified dataset ID, then rolls back the transaction since the operation is read-only.
    ///
    /// # Parameters
    /// - `dataset_id`: The UUID of the dataset whose plan DAG is to be retrieved.
    ///
    /// # Returns
    /// A JSON response containing the retrieved `Plan`.
    ///
    /// # Examples
    ///
    /// ```
    /// // Assume `api` is an instance of Api and `pool` is a valid PgPool.
    /// let dataset_id = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
    /// let response = api.plan_dag_get(Data(&pool), Path(dataset_id)).await.unwrap();
    /// assert_eq!(response.0.dataset_id, dataset_id);
    /// ```
    async fn plan_dag_get(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<Uuid>,
    ) -> Result<Json<Plan>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Read the plan dag from the DB
        let plan_dag: Plan = plan_dag_read(&mut tx, dataset_id).await?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(plan_dag))
    }
}
