use crate::{
    core::{PlanDag, plan_dag_add, plan_dag_read},
    db::PlanDagParam,
};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{OpenApi, Tags, param::Path, payload::Json};
use sqlx::PgPool;
use uuid::Uuid;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    #[oai(rename = "Plan DAG")]
    PlanDag,
}

/// Struct we will use to build our REST API
pub struct Api;

#[OpenApi]
impl Api {
    /// Register a Plan DAG
    #[oai(path = "/plan_dag", method = "post", tag = Tag::PlanDag)]
    /// Creates a new Plan DAG entry in the database.
    ///
    /// Accepts a JSON payload representing the Plan DAG parameters, inserts the new Plan DAG into the database within a transaction, and returns the created Plan DAG as JSON.
    ///
    /// # Returns
    /// A JSON response containing the newly created Plan DAG.
    ///
    /// # Errors
    /// Returns an internal server error if the database transaction fails.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage within a Poem handler context
    /// let api = Api;
    /// let pool: PgPool = setup_test_pool().await;
    /// let param = PlanDagParam { /* fields */ };
    /// let response = api.plan_dag_post(Data(&pool), Json(param)).await.unwrap();
    /// assert_eq!(response.0, /* expected PlanDag */);
    /// ```
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

    /// Read a Plan DAG
    #[oai(path = "/plan_dag/:dataset_id", method = "get", tag = Tag::PlanDag)]
    /// Retrieves the Plan DAG associated with the specified dataset ID.
    ///
    /// Begins a database transaction and fetches the Plan DAG for the given dataset UUID.
    /// Returns the Plan DAG as a JSON response. If the transaction cannot be started, responds with an internal server error.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage within a Poem handler context:
    /// let response = api.plan_dag_get(Data(&pool), Path(dataset_id)).await?;
    /// assert_eq!(response.0.id, dataset_id);
    /// ```
    async fn plan_dag_get(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<Uuid>,
    ) -> Result<Json<PlanDag>, poem::Error> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Read the plan dag from the DB
        let plan_dag: PlanDag = plan_dag_read(&mut tx, dataset_id).await?;

        Ok(Json(plan_dag))
    }
}
