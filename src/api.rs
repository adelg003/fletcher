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
        test::{TestClient, TestJsonValue, TestResponse},
    };
    use poem_openapi::OpenApiService;
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Help function to return the parameter for plan post endpoint
    fn create_test_plan_param(dataset_id: Uuid, dp1_id: Uuid, dp2_id: Uuid) -> Value {
        json!({
            "dataset": {
                "id": dataset_id.to_string(),
                "paused": false,
                "extra": {"test":"dataset"},
            },
            "data_products": [
                {
                    "id": dp1_id.to_string(),
                    "compute": "cams",
                    "name": "data-product-1",
                    "version": "1.0.0",
                    "eager": true,
                    "passthrough": {"test":"passthrough1"},
                    "extra": {"test":"extra1"},
                },
                {
                    "id": dp2_id.to_string(),
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
                    "parent_id": dp1_id.to_string(),
                    "child_id": dp2_id.to_string(),
                    "extra": {"test":"dependency"},
                }
            ]
        })
    }

    /// Helper function to validate all the data elements of the standard plan
    fn validate_test_plan(
        json_value: &TestJsonValue,
        dataset_id: Uuid,
        dp1_id: Uuid,
        dp2_id: Uuid,
    ) {
        // Test response from API for Dataset
        let dataset = json_value.object().get("dataset").object();
        dataset.get("id").assert_string(&dataset_id.to_string());
        dataset.get("paused").assert_bool(false);
        dataset
            .get("extra")
            .object()
            .get("test")
            .assert_string("dataset");
        dataset.get("modified_by").assert_string("placeholder_user");

        // Test response from API for Data Products
        let data_products = json_value.object().get("data_products").object_array();
        assert_eq!(data_products.len(), 2);

        let dp1 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp1_id.to_string())
            .unwrap();
        dp1.get("id").assert_string(&dp1_id.to_string());
        dp1.get("compute").assert_string("cams");
        dp1.get("name").assert_string("data-product-1");
        dp1.get("version").assert_string("1.0.0");
        dp1.get("eager").assert_bool(true);
        dp1.get("passthrough")
            .object()
            .get("test")
            .assert_string("passthrough1");
        dp1.get("state").assert_string("queued");
        dp1.get("run_id").assert_null();
        dp1.get("link").assert_null();
        dp1.get("passback").assert_null();
        dp1.get("passback").assert_null();
        dp1.get("extra")
            .object()
            .get("test")
            .assert_string("extra1");
        dp1.get("modified_by").assert_string("placeholder_user");

        let dp2 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp2_id.to_string())
            .unwrap();
        dp2.get("id").assert_string(&dp2_id.to_string());
        dp2.get("compute").assert_string("dbxaas");
        dp2.get("name").assert_string("data-product-2");
        dp2.get("version").assert_string("2.0.0");
        dp2.get("eager").assert_bool(true);
        dp2.get("passthrough")
            .object()
            .get("test")
            .assert_string("passthrough2");
        dp2.get("state").assert_string("waiting");
        dp2.get("run_id").assert_null();
        dp2.get("link").assert_null();
        dp2.get("passback").assert_null();
        dp2.get("passback").assert_null();
        dp2.get("extra")
            .object()
            .get("test")
            .assert_string("extra2");
        dp2.get("modified_by").assert_string("placeholder_user");

        // Test response from API for Dependencies
        let dependencies = json_value.object().get("dependencies").object_array();
        assert_eq!(dependencies.len(), 1);

        let dep = dependencies
            .iter()
            .find(|dp| {
                dp.get("parent_id").string() == dp1_id.to_string()
                    && dp.get("child_id").string() == dp2_id.to_string()
            })
            .unwrap();
        dep.get("parent_id").assert_string(&dp1_id.to_string());
        dep.get("child_id").assert_string(&dp2_id.to_string());
        dep.get("extra")
            .object()
            .get("test")
            .assert_string("dependency");
        dep.get("modified_by").assert_string("placeholder_user");
    }

    /// Test Plan Post
    #[sqlx::test]
    async fn test_plan_post(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Payload to send into the API
        let param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

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

        // Test response from API
        validate_test_plan(&json_value, dataset_id, dp1_id, dp2_id);
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

    /// Test Plan Get - Success Case
    #[sqlx::test]
    async fn test_plan_get_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // First create a plan via POST
        let param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        // Test Client
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Now test the GET endpoint
        let response: TestResponse = cli
            .get(format!("/plan/{dataset_id}"))
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        // Pull response json
        let test_json = response.json().await;
        let json_value = test_json.value();

        // Validate Dataset
        validate_test_plan(&json_value, dataset_id, dp1_id, dp2_id);
    }

    /// Test Plan Get - Not Found Case
    #[sqlx::test]
    async fn test_plan_get_not_found(pool: PgPool) {
        let non_existent_dataset_id = Uuid::new_v4().to_string();
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        let response: TestResponse = cli
            .get(format!("/plan/{non_existent_dataset_id}"))
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text("no rows returned by a query that expected to return at least one row")
            .await;
    }

    /// Test State Put - Success Case
    #[sqlx::test]
    async fn test_state_put_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Update states
        let state_param = json!([
            {
                "id": dp1_id.to_string(),
                "state": "success",
                "run_id": "12345678-1234-1234-1234-123456789abc",
                "link": "https://example.com/run-123",
                "passback": {"status": "started"},
                "passback": {
                    "status": "finished",
                    "result": "success",
                },
            },
            {
                "id": dp2_id.to_string(),
                "state": "failed",
                "run_id": "12345678-1234-1234-1234-123456789def",
                "link": "https://example.com/run-456",
                "passback": {
                    "status": "finished",
                    "result": "failed",
                },
            }
        ]);

        let response: TestResponse = cli
            .put(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&state_param)
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();

        let data_products = json_value.object().get("data_products").object_array();

        let dp1 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp1_id.to_string())
            .unwrap();
        dp1.get("id").assert_string(&dp1_id.to_string());
        dp1.get("state").assert_string("success");
        dp1.get("run_id")
            .assert_string("12345678-1234-1234-1234-123456789abc");
        dp1.get("link").assert_string("https://example.com/run-123");
        dp1.get("passback")
            .object()
            .get("status")
            .assert_string("finished");
        dp1.get("passback")
            .object()
            .get("result")
            .assert_string("success");

        let dp2 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp2_id.to_string())
            .unwrap();
        dp2.get("id").assert_string(&dp2_id.to_string());
        dp2.get("state").assert_string("queued");
        dp2.get("run_id")
            .assert_string("12345678-1234-1234-1234-123456789def");
        dp2.get("link").assert_string("https://example.com/run-456");
        dp2.get("passback")
            .object()
            .get("status")
            .assert_string("finished");
        dp2.get("passback")
            .object()
            .get("result")
            .assert_string("failed");
    }

    /// Test State Put - Dataset Not Found Case
    #[sqlx::test]
    async fn test_state_put_dataset_not_found(pool: PgPool) {
        let non_existent_dataset_id = Uuid::new_v4().to_string();
        let dp_id = Uuid::new_v4().to_string();
        let state_param = json!([
            {
                "id": dp_id,
                "state": "running",
                "run_id": "12345678-1234-1234-1234-123456789abc",
                "link": "https://example.com/run-123",
                "passback": {"status": "started"},
            }
        ]);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let response: TestResponse = cli
            .put(format!("/data_product/{non_existent_dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&state_param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text("no rows returned by a query that expected to return at least one row")
            .await;
    }

    /// Test State Put - Data Product Not Found Case
    #[sqlx::test]
    async fn test_state_put_data_product_not_found(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let missing_dp_id = Uuid::new_v4();

        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        let state_param = json!([
            {
                "id": missing_dp_id.to_string(),
                "state": "running",
                "run_id": "12345678-1234-1234-1234-123456789abc",
                "link": "https://example.com/run-123",
                "passback": {"status": "started"},
            }
        ]);

        let response: TestResponse = cli
            .put(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&state_param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text(format!("Data product not found for: {missing_dp_id}"))
            .await;
    }

    /// Test Plan Post - Invalid JSON Payload (Missing Required Fields)
    #[sqlx::test]
    async fn test_plan_post_invalid_payload(pool: PgPool) {
        let param = json!({
            "dataset": {
                "id": "invalid-uuid",
                "paused": false,
            },
        });

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
        response.assert_text("parse request payload error: failed to parse \"string_uuid\": invalid \
            character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found \
            `i` at 1 (occurred while parsing \"DatasetParam\") (occurred while parsing \"PlanParam\")"
        ).await;
    }

    /// Test Plan Post - Empty Data Products Array
    #[sqlx::test]
    async fn test_plan_post_empty_data_products(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let param = json!({
            "dataset": {
                "id": dataset_id,
                "paused": false,
                "extra": {"test":"dataset"},
            },
            "data_products": [],
            "dependencies": []
        });

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;

        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();

        let data_products = json_value.object().get("data_products").object_array();
        assert_eq!(data_products.len(), 0);

        let dependencies = json_value.object().get("dependencies").object_array();
        assert_eq!(dependencies.len(), 0);
    }

    /// Test Plan Post - Duplicate Data Product IDs in Payload
    #[sqlx::test]
    async fn test_plan_post_duplicate_data_product_ids(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let dp_id = Uuid::new_v4().to_string();

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
                {
                    "id": dp_id,
                    "compute": "dbxaas",
                    "name": "data-product-2",
                    "version": "2.0.0",
                    "eager": false,
                    "passthrough": {"test":"passthrough2"},
                    "extra": {"test":"extra2"},
                },
            ],
            "dependencies": []
        });

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
        response
            .assert_text(format!("Duplicate data-product id in parameter: {dp_id}"))
            .await;
    }

    /// Test Plan Post - Missing Data Product for Dependency
    #[sqlx::test]
    async fn test_plan_post_missing_data_product_for_dependency(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let dp_id = Uuid::new_v4().to_string();
        let missing_dp_id = Uuid::new_v4().to_string();

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
                    "parent_id": missing_dp_id,
                    "child_id": dp_id,
                    "extra": {"test":"dependency"},
                },
            ]
        });

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);
        let response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&param)
            .data(pool)
            .send()
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        response
            .assert_text(format!("Data product not found for: {missing_dp_id}"))
            .await;
    }
}
