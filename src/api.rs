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

#[cfg(test)]
mod tests {
    use super::*;
    use poem::{
        http::StatusCode,
        test::{TestClient, TestResponse},
    };
    use poem_openapi::OpenApiService;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Test Plan Post
    #[sqlx::test]
    async fn test_plan_post(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let dp1_id = Uuid::new_v4().to_string();
        let dp2_id = Uuid::new_v4().to_string();

        // Payload to send into the API
        let param = json!({
            "dataset": {
                "id": dataset_id,
                "paused": false,
                "extra": {"test":"dataset"},
            },
            "data_products": [
                {
                    "id": dp1_id,
                    "compute": "cams",
                    "name": "data-product-1",
                    "version": "1.0.0",
                    "eager": true,
                    "passthrough": {"test":"passthrough1"},
                    "extra": {"test":"extra1"},
                },
                {
                    "id": dp2_id,
                    "compute": "dbxaas",
                    "name": "data-product-2",
                    "version": "2.0.0",
                    "eager": true,
                    "passthrough": {"test":"passthrough2"},
                    "extra": {"test":"extra2"},
                },
            ],
            "dependencies": [
                {
                    "parent_id": dp1_id,
                    "child_id": dp2_id,
                    "extra": {"test":"dependency"},
                }
            ]
        });

        // Test Client
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test Request
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;

        // Check status
        response.assert_status_is_ok();

        // Pull response json
        let test_json = response.json().await;
        let json_value = test_json.value();

        // Test response from API for Dataset
        let dataset = json_value.object().get("dataset").object();
        dataset.get("id").assert_string(&dataset_id);
        dataset.get("paused").assert_bool(false);
        dataset
            .get("extra")
            .object()
            .get("test")
            .assert_string("dataset");
        dataset.get("modified_by").assert_string("placeholder_user");

        // Test response from API for Data Products
        let data_products = json_value.object().get("data_products").object_array();
        let data_product_1 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp1_id)
            .unwrap();
        data_product_1.get("id").assert_string(&dp1_id);
        data_product_1.get("compute").assert_string("cams");
        data_product_1.get("name").assert_string("data-product-1");
        data_product_1.get("version").assert_string("1.0.0");
        data_product_1.get("eager").assert_bool(true);
        data_product_1
            .get("passthrough")
            .object()
            .get("test")
            .assert_string("passthrough1");
        data_product_1.get("state").assert_string("queued");
        data_product_1.get("run_id").assert_null();
        data_product_1.get("link").assert_null();
        data_product_1.get("passback").assert_null();
        data_product_1.get("passback").assert_null();
        data_product_1
            .get("extra")
            .object()
            .get("test")
            .assert_string("extra1");
        data_product_1
            .get("modified_by")
            .assert_string("placeholder_user");

        let data_product_2 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp2_id)
            .unwrap();
        data_product_2.get("id").assert_string(&dp2_id);
        data_product_2.get("compute").assert_string("dbxaas");
        data_product_2.get("name").assert_string("data-product-2");
        data_product_2.get("version").assert_string("2.0.0");
        data_product_2.get("eager").assert_bool(true);
        data_product_2
            .get("passthrough")
            .object()
            .get("test")
            .assert_string("passthrough2");
        data_product_2.get("state").assert_string("waiting");
        data_product_2.get("run_id").assert_null();
        data_product_2.get("link").assert_null();
        data_product_2.get("passback").assert_null();
        data_product_2.get("passback").assert_null();
        data_product_2
            .get("extra")
            .object()
            .get("test")
            .assert_string("extra2");
        data_product_2
            .get("modified_by")
            .assert_string("placeholder_user");

        // Test response from API for Dependencies
        let dependencies = json_value.object().get("dependencies").object_array();
        let dependency_1 = dependencies
            .iter()
            .find(|dp| {
                dp.get("parent_id").string() == dp1_id && dp.get("child_id").string() == dp2_id
            })
            .unwrap();
        dependency_1.get("parent_id").assert_string(&dp1_id);
        dependency_1.get("child_id").assert_string(&dp2_id);
        dependency_1
            .get("extra")
            .object()
            .get("test")
            .assert_string("dependency");
        dependency_1
            .get("modified_by")
            .assert_string("placeholder_user");
    }

    /// Test Plan Post if cyclical
    #[sqlx::test]
    async fn test_plan_post_cyclical(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let dp_id = Uuid::new_v4().to_string();

        // Payload to send into the API
        let param = json!({
            "dataset": {
                "id": dataset_id,
                "paused": false,
                "extra": {"test":"dataset"},
            },
            "data_products": [
                {
                    "id": dp_id,
                    "compute": "cams",
                    "name": "data-product-1",
                    "version": "1.0.0",
                    "eager": true,
                    "passthrough": {"test":"passthrough1"},
                    "extra": {"test":"extra1"},
                },
            ],
            "dependencies": [
                {
                    "parent_id": dp_id,
                    "child_id": dp_id, // Cyclical dependency
                    "extra": {"test":"dependency"},
                }
            ]
        });

        // Test Client
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test Request
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;

        // Check status
        response.assert_status(StatusCode::UNPROCESSABLE_ENTITY);
        response.assert_text("Graph is cyclical").await;
    }
}
