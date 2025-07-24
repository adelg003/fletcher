use crate::{
    Config,
    auth::{Authenticated, JwtAuth, RemoteLogin, Role, authenticate},
    core::{
        clear_edit, data_product_read, disable_drop, plan_add, plan_pause_edit, plan_read,
        plan_search_read, states_edit,
    },
    error::into_poem_error,
    model::{DataProduct, DataProductId, DatasetId, Plan, PlanParam, SearchReturn, StateParam},
};
use poem::{error::InternalServerError, web::Data};
use poem_openapi::{
    OpenApi, Tags,
    param::{Path, Query},
    payload::Json,
};
use sqlx::PgPool;

/// Tags to show in Swagger Page
#[derive(Tags)]
pub enum Tag {
    Authenticate,
    #[oai(rename = "Data Product")]
    DataProduct,
    Plan,
}

/// Struct we will use to build our REST API
pub struct Api;

#[OpenApi]
impl Api {
    /// Authenticate a remote service and provide a JWT
    #[oai(path = "/authenticate", method = "post", tag = Tag::Authenticate)]
    async fn authenticate(
        &self,
        Data(config): Data<&Config>,
        Json(cred): Json<RemoteLogin>,
    ) -> poem::Result<Json<Authenticated>> {
        let auth: Authenticated = authenticate(&cred, config).map_err(into_poem_error)?;

        Ok(Json(auth))
    }

    /// Register a Plan
    #[oai(path = "/plan", method = "post", tag = Tag::Plan)]
    async fn plan_post(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Json(plan): Json<PlanParam>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Publish).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Add the plan to the DB
        let plan: Plan = plan_add(&mut tx, &plan, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

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
        let plan: Plan = plan_read(&mut tx, dataset_id)
            .await
            .map_err(into_poem_error)?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Search for a plan
    #[oai(path = "/plan/search", method = "get", tag = Tag::Plan)]
    async fn plan_search_get(
        &self,
        Data(pool): Data<&PgPool>,
        Query(search_by): Query<String>,
        Query(page): Query<u32>,
    ) -> poem::Result<Json<SearchReturn>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Search for plans
        let search: SearchReturn = plan_search_read(&mut tx, &search_by, page)
            .await
            .map_err(into_poem_error)?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(search))
    }

    /// Pause a plan
    #[oai(path = "/plan/pause/:dataset_id", method = "put", tag = Tag::Plan)]
    async fn plan_pause_put(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Pause).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Pause a Plan
        let plan: Plan = plan_pause_edit(&mut tx, dataset_id, true, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

        // Commit transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Unpause a plan
    #[oai(path = "/plan/unpause/:dataset_id", method = "put", tag = Tag::Plan)]
    async fn plan_unpause_put(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Pause).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Unpause a Plan
        let plan: Plan = plan_pause_edit(&mut tx, dataset_id, false, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

        // Commit transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Retrieve a Data Product
    #[oai(path = "/data_product/:dataset_id/:data_product_id", method = "get", tag = Tag::DataProduct)]
    async fn data_product_get(
        &self,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
        Path(data_product_id): Path<DataProductId>,
    ) -> poem::Result<Json<DataProduct>> {
        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Read the data product from the DB
        let data_product: DataProduct = data_product_read(&mut tx, dataset_id, data_product_id)
            .await
            .map_err(into_poem_error)?;

        // Rollback transaction (read-only operation)
        tx.rollback().await.map_err(InternalServerError)?;

        Ok(Json(data_product))
    }

    /// Update one or multiple data product states
    #[oai(path = "/data_product/update/:dataset_id", method = "put", tag = Tag::DataProduct)]
    async fn state_put(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
        Json(states): Json<Vec<StateParam>>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Update).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Update data product states and return the updated plan
        let plan: Plan = states_edit(&mut tx, dataset_id, &states, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Clear one or multiple data products and clear all their downsteam data products.
    #[oai(path = "/data_product/clear/:dataset_id", method = "put", tag = Tag::DataProduct)]
    async fn clear_put(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
        Json(data_product_ids): Json<Vec<DataProductId>>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Update).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Clear Data Products and clear all downsteam data products.
        let plan: Plan = clear_edit(&mut tx, dataset_id, &data_product_ids, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

        // Commit Transaction
        tx.commit().await.map_err(InternalServerError)?;

        Ok(Json(plan))
    }

    /// Disable one or multiple data product
    #[oai(path = "/data_product/:dataset_id", method = "delete", tag = Tag::DataProduct)]
    async fn disable_delete(
        &self,
        auth: JwtAuth,
        Data(pool): Data<&PgPool>,
        Path(dataset_id): Path<DatasetId>,
        Json(data_product_ids): Json<Vec<DataProductId>>,
    ) -> poem::Result<Json<Plan>> {
        auth.check_role(Role::Disable).map_err(into_poem_error)?;

        // Start Transaction
        let mut tx = pool.begin().await.map_err(InternalServerError)?;

        // Mark Data Products as disabled.
        let plan: Plan = disable_drop(&mut tx, dataset_id, &data_product_ids, "placeholder_user")
            .await
            .map_err(into_poem_error)?;

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
        assert_eq!(
            data_products.len(),
            2,
            "Should have 2 data products in the API response"
        );

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
        dp2.get("extra")
            .object()
            .get("test")
            .assert_string("extra2");
        dp2.get("modified_by").assert_string("placeholder_user");

        // Test response from API for Dependencies
        let dependencies = json_value.object().get("dependencies").object_array();
        assert_eq!(
            dependencies.len(),
            1,
            "Should have 1 dependency in the API response"
        );

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

    /// Test Data Product Get - Success Case
    #[sqlx::test]
    async fn test_data_product_get_success(pool: PgPool) {
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

        // Now test the GET data_product endpoint for dp1
        let response: TestResponse = cli
            .get(format!("/data_product/{dataset_id}/{dp1_id}"))
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        // Pull response json
        let test_json = response.json().await;
        let json_value = test_json.value();

        // Validate the data product response
        json_value
            .object()
            .get("id")
            .assert_string(&dp1_id.to_string());
        json_value.object().get("compute").assert_string("cams");
        json_value
            .object()
            .get("name")
            .assert_string("data-product-1");
        json_value.object().get("version").assert_string("1.0.0");
        json_value.object().get("eager").assert_bool(true);
        json_value
            .object()
            .get("passthrough")
            .object()
            .get("test")
            .assert_string("passthrough1");
        json_value.object().get("state").assert_string("queued");
        json_value.object().get("run_id").assert_null();
        json_value.object().get("link").assert_null();
        json_value.object().get("passback").assert_null();
        json_value
            .object()
            .get("extra")
            .object()
            .get("test")
            .assert_string("extra1");
        json_value
            .object()
            .get("modified_by")
            .assert_string("placeholder_user");
    }

    /// Test Data Product Get - Not Found Case
    #[sqlx::test]
    async fn test_data_product_get_not_found(pool: PgPool) {
        let non_existent_dataset_id = Uuid::new_v4();
        let non_existent_data_product_id = Uuid::new_v4();
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        let response: TestResponse = cli
            .get(format!(
                "/data_product/{non_existent_dataset_id}/{non_existent_data_product_id}"
            ))
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
            },
        ]);

        let response: TestResponse = cli
            .put(format!("/data_product/update/{dataset_id}"))
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
            .put(format!("/data_product/update/{non_existent_dataset_id}"))
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
            .put(format!("/data_product/update/{dataset_id}"))
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
        assert_eq!(
            data_products.len(),
            0,
            "Empty plan should have 0 data products"
        );

        let dependencies = json_value.object().get("dependencies").object_array();
        assert_eq!(
            dependencies.len(),
            0,
            "Empty plan should have 0 dependencies"
        );
    }

    /// Test Plan Post - Duplicate Data Product IDs in Payload
    #[sqlx::test]
    async fn test_plan_post_duplicate_data_product_ids(pool: PgPool) {
        let dataset_id = Uuid::new_v4().to_string();
        let dp_id = Uuid::new_v4().to_string();

        let param = json!({
            "dataset": {
                "id": dataset_id,
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
        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text(format!("Data product not found for: {missing_dp_id}"))
            .await;
    }

    // Test clear_put

    /// Test clear_put - Success Case: Clear data product to queued state
    #[sqlx::test]
    async fn test_clear_put_success_queued_state(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Update dp1 to success so dp2 can be cleared to queued
        let state_param = json!([
            {
                "id": dp1_id.to_string(),
                "state": "success",
                "run_id": "12345678-1234-1234-1234-123456789abc",
                "link": "https://example.com/run-123",
                "passback": {
                    "status": "finished",
                    "result": "success",
                },
            }
        ]);

        let state_response: TestResponse = cli
            .put(format!("/data_product/update/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&state_param)
            .data(pool.clone())
            .send()
            .await;

        state_response.assert_status_is_ok();

        // Now clear dp2 (should go to queued since dp1 is success)
        let clear_param = json!([dp2_id.to_string()]);
        let response: TestResponse = cli
            .put(format!("/data_product/clear/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&clear_param)
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();

        let data_products = json_value.object().get("data_products").object_array();
        let dp2 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp2_id.to_string())
            .unwrap();
        dp2.get("state").assert_string("queued");
    }

    /// Test clear_put - Downstream children updated to waiting state
    #[sqlx::test]
    async fn test_clear_put_downstream_waiting_state(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        // Create plan with dp1 -> dp2 -> dp3 dependency chain
        let create_param = json!({
            "dataset": {
                "id": dataset_id.to_string(),
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
                {
                    "id": dp3_id.to_string(),
                    "compute": "cams",
                    "name": "data-product-3",
                    "version": "3.0.0",
                    "eager": true,
                    "passthrough": {"test":"passthrough3"},
                    "extra": {"test":"extra3"},
                },
            ],
            "dependencies": [
                {
                    "parent_id": dp1_id.to_string(),
                    "child_id": dp2_id.to_string(),
                    "extra": {"test":"dependency1"},
                },
                {
                    "parent_id": dp2_id.to_string(),
                    "child_id": dp3_id.to_string(),
                    "extra": {"test":"dependency2"},
                }
            ]
        });

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Pause the plan
        let pause_response: TestResponse = cli
            .put(format!("/plan/pause/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .data(pool.clone())
            .send()
            .await;
        pause_response.assert_status_is_ok();

        // Set dp2 and dp3 to success
        let state_param = json!([
            {
                "id": dp2_id.to_string(),
                "state": "success",
                "run_id": "175286e4-8499-4c03-802e-b55e8646ac11",
                "link": "https://example.com/run-456",
                "passback": {
                    "status": "finished",
                    "result": "success",
                },
            },
            {
                "id": dp3_id.to_string(),
                "state": "success",
                "run_id": "8663c153-17ec-4ed2-b417-d1ae7ef827b2",
                "link": "https://example.com/run-789",
                "passback": {
                    "status": "finished",
                    "result": "success",
                },
            }
        ]);

        let state_response: TestResponse = cli
            .put(format!("/data_product/update/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&state_param)
            .data(pool.clone())
            .send()
            .await;
        state_response.assert_status_is_ok();

        // Clear dp1 (should reset dp2 and dp3 to waiting)
        let clear_param = json!([dp1_id.to_string()]);
        let response: TestResponse = cli
            .put(format!("/data_product/clear/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&clear_param)
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();

        let data_products = json_value.object().get("data_products").object_array();

        // dp1 should be waiting (reset)
        let dp1 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp1_id.to_string())
            .unwrap();
        dp1.get("state").assert_string("waiting");

        // dp2 should be waiting (downstream cleared)
        let dp2 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp2_id.to_string())
            .unwrap();
        dp2.get("state").assert_string("waiting");

        // dp3 should be waiting (downstream cleared)
        let dp3 = data_products
            .iter()
            .find(|dp| dp.get("id").string() == dp3_id.to_string())
            .unwrap();
        dp3.get("state").assert_string("waiting");
    }

    /// Test clear_put - Non-existent Data Product
    #[sqlx::test]
    async fn test_clear_put_nonexistent_data_product(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let nonexistent_dp_id = Uuid::new_v4();

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

        // Try to clear non-existent data product
        let clear_param = json!([nonexistent_dp_id.to_string()]);
        let response: TestResponse = cli
            .put(format!("/data_product/clear/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&clear_param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text(format!("Data product not found for: {nonexistent_dp_id}"))
            .await;
    }

    /// Test clear_put - Disabled Data Product
    #[sqlx::test]
    async fn test_clear_put_disabled_data_product(pool: PgPool) {
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

        // First disable dp1
        let disable_param = json!([dp1_id.to_string()]);
        let disable_response: TestResponse = cli
            .delete(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&disable_param)
            .data(pool.clone())
            .send()
            .await;
        disable_response.assert_status_is_ok();

        // Try to clear disabled data product
        let clear_param = json!([dp1_id.to_string()]);
        let response: TestResponse = cli
            .put(format!("/data_product/clear/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&clear_param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::FORBIDDEN);
        response
            .assert_text(format!("Data product is locked: {dp1_id}"))
            .await;
    }

    /// Test disable_delete - Success Case
    #[sqlx::test]
    async fn test_disable_delete_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Test disable_delete endpoint
        let disable_param = json!([dp1_id.to_string()]);
        let response: TestResponse = cli
            .delete(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&disable_param)
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
        dp1.get("state").assert_string("disabled");
    }

    /// Test disable_delete - Non-existent Data Product
    #[sqlx::test]
    async fn test_disable_delete_nonexistent_data_product(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let nonexistent_dp_id = Uuid::new_v4();

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

        // Try to disable non-existent data product
        let disable_param = json!([nonexistent_dp_id.to_string()]);
        let response: TestResponse = cli
            .delete(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&disable_param)
            .data(pool)
            .send()
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
        response
            .assert_text(format!("Data product not found for: {nonexistent_dp_id}"))
            .await;
    }

    /// Test disable_delete - Already Disabled Data Product
    #[sqlx::test]
    async fn test_disable_delete_already_disabled(pool: PgPool) {
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

        // First disable
        let disable_param = json!([dp1_id.to_string()]);
        let first_response: TestResponse = cli
            .delete(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&disable_param)
            .data(pool.clone())
            .send()
            .await;
        first_response.assert_status_is_ok();

        // Try to disable again
        let second_response: TestResponse = cli
            .delete(format!("/data_product/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&disable_param)
            .data(pool)
            .send()
            .await;

        second_response.assert_status(StatusCode::FORBIDDEN);
        second_response
            .assert_text(format!("Data product is locked: {dp1_id}"))
            .await;
    }

    /// Test Plan Pause Put - Success Case
    #[sqlx::test]
    async fn test_plan_pause_put_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan (unpaused by default)
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Verify plan is initially unpaused
        let initial_json = create_response.json().await;
        let initial_value = initial_json.value();
        initial_value
            .object()
            .get("dataset")
            .object()
            .get("paused")
            .assert_bool(false);

        // Test pause endpoint
        let pause_response: TestResponse = cli
            .put(format!("/plan/pause/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .data(pool)
            .send()
            .await;
        pause_response.assert_status_is_ok();

        // Verify plan is now paused
        let pause_json = pause_response.json().await;
        let pause_value = pause_json.value();
        pause_value
            .object()
            .get("dataset")
            .object()
            .get("paused")
            .assert_bool(true);
    }
    /// Test Plan Search Get - Success Case
    #[sqlx::test]
    async fn test_plan_search_get_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Search for the plan using dataset_id as search term
        let response: TestResponse = cli
            .get(format!("/plan/search?search_by={dataset_id}&page=0"))
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        // Pull response json
        let test_json = response.json().await;
        let json_value = test_json.value();

        // Verify we get back search results
        let search_return = json_value.object();
        let results = search_return.get("rows").array();
        assert!(!results.is_empty(), "Search results should not be empty");

        // Verify the structure of the search results
        let first_result = results.get(0).object();
        first_result
            .get("dataset_id")
            .assert_string(&dataset_id.to_string());
        first_result.get("modified_date").assert_not_null();
    }

    /// Test Plan Search Get - Empty Results Case
    #[sqlx::test]
    async fn test_plan_search_get_empty_results(pool: PgPool) {
        let non_existent_search = "non-existent-search-term";
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        let response: TestResponse = cli
            .get(format!(
                "/plan/search?search_by={non_existent_search}&page=0",
            ))
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();

        // Verify empty results
        let search_return = json_value.object();
        let results = search_return.get("rows").array();
        assert!(
            results.is_empty(),
            "Search results should be empty for non-existent search term"
        );
    }

    /// Test Plan Search Get - Pagination
    #[sqlx::test]
    async fn test_plan_search_get_pagination(pool: PgPool) {
        // Create multiple plans to test pagination
        let dataset_id_1 = Uuid::new_v4();
        let dataset_id_2 = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();
        let dp4_id = Uuid::new_v4();

        let create_param_1 = create_test_plan_param(dataset_id_1, dp1_id, dp2_id);
        let create_param_2 = create_test_plan_param(dataset_id_2, dp3_id, dp4_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create first plan
        let create_response_1: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param_1)
            .data(pool.clone())
            .send()
            .await;
        create_response_1.assert_status_is_ok();

        // Create second plan
        let create_response_2: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param_2)
            .data(pool.clone())
            .send()
            .await;
        create_response_2.assert_status_is_ok();

        // Test page 1
        let response_page_1: TestResponse = cli
            .get("/plan/search?search_by=data-product&page=0")
            .data(pool.clone())
            .send()
            .await;
        response_page_1.assert_status_is_ok();

        // Test page 2
        let response_page_2: TestResponse = cli
            .get("/plan/search?search_by=data-product&page=1")
            .data(pool)
            .send()
            .await;
        response_page_2.assert_status_is_ok();

        // Both pages should return successfully (specific result validation depends on implementation)
        let page_1_json = response_page_1.json().await;
        let page_1_search_return = page_1_json.value().object();
        let page_1_results = page_1_search_return.get("rows").array();

        let page_2_json = response_page_2.json().await;
        let page_2_search_return = page_2_json.value().object();
        let page_2_results = page_2_search_return.get("rows").array();

        // At least one page should have results
        assert!(
            !page_1_results.is_empty() || !page_2_results.is_empty(),
            "At least one page should have results"
        );
    }

    /// Test Plan Search Get - Missing Query Parameters
    #[sqlx::test]
    async fn test_plan_search_get_missing_params(pool: PgPool) {
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test missing search_by parameter
        let response_missing_search: TestResponse = cli
            .get("/plan/search?page=1")
            .data(pool.clone())
            .send()
            .await;
        response_missing_search.assert_status(StatusCode::BAD_REQUEST);

        // Test missing page parameter
        let response_missing_page: TestResponse = cli
            .get("/plan/search?search_by=test")
            .data(pool.clone())
            .send()
            .await;
        response_missing_page.assert_status(StatusCode::BAD_REQUEST);

        // Test missing both parameters
        let response_missing_both: TestResponse = cli.get("/plan/search").data(pool).send().await;
        response_missing_both.assert_status(StatusCode::BAD_REQUEST);
    }

    /// Test Plan Search Get - Invalid Page Parameter
    #[sqlx::test]
    async fn test_plan_search_get_invalid_page(pool: PgPool) {
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test invalid page parameter (non-numeric)
        let response: TestResponse = cli
            .get("/plan/search?search_by=test&page=invalid")
            .data(pool)
            .send()
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
    }

    /// Test Plan Search Get - Negative Page Parameter
    #[sqlx::test]
    async fn test_plan_search_get_negative_page(pool: PgPool) {
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test page -1 (should be handled appropriately by the implementation)
        let response: TestResponse = cli
            .get("/plan/search?search_by=test&page=-1")
            .data(pool)
            .send()
            .await;
        // The response depends on how the backend handles page=-1
        // This test ensures the endpoint does not crash
        response.assert_status(StatusCode::BAD_REQUEST);
    }

    /// Test Plan Search Get - Empty Search Term
    #[sqlx::test]
    async fn test_plan_search_get_empty_search_term(pool: PgPool) {
        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Test empty search term
        let response: TestResponse = cli
            .get("/plan/search?search_by=&page=1")
            .data(pool)
            .send()
            .await;
        response.assert_status_is_ok();

        let test_json = response.json().await;
        let json_value = test_json.value();
        let search_return = json_value.object();
        let results = search_return.get("rows").array();

        // Empty search term should return empty results
        results.assert_is_empty();
    }

    /// Test plan unpause - Success Case: Unpause a previously paused plan
    #[sqlx::test]
    async fn test_plan_unpause_put_success(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        // Create initial plan
        let create_param = create_test_plan_param(dataset_id, dp1_id, dp2_id);

        let ep = OpenApiService::new(Api, "test", "1.0");
        let cli = TestClient::new(ep);

        // Create the plan first
        let create_response: TestResponse = cli
            .post("/plan")
            .header("Content-Type", "application/json; charset=utf-8")
            .body_json(&create_param)
            .data(pool.clone())
            .send()
            .await;
        create_response.assert_status_is_ok();

        // Verify the plan is unpaused
        let create_json = create_response.json().await;
        let create_value = create_json.value();

        create_value
            .object()
            .get("dataset")
            .object()
            .get("paused")
            .assert_bool(false);

        // First pause the plan
        let pause_response: TestResponse = cli
            .put(format!("/plan/pause/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .data(pool.clone())
            .send()
            .await;
        pause_response.assert_status_is_ok();

        // Verify the plan is paused
        let pause_json = pause_response.json().await;
        let pause_value = pause_json.value();
        pause_value
            .object()
            .get("dataset")
            .object()
            .get("paused")
            .assert_bool(true);

        // Now unpause the plan
        let unpause_response: TestResponse = cli
            .put(format!("/plan/unpause/{dataset_id}"))
            .header("Content-Type", "application/json; charset=utf-8")
            .data(pool)
            .send()
            .await;
        unpause_response.assert_status_is_ok();

        // Verify the plan is unpaused
        let unpause_json = unpause_response.json().await;
        let unpause_value = unpause_json.value();

        // Validate the entire plan structure and that paused is false
        validate_test_plan(&unpause_value, dataset_id, dp1_id, dp2_id);

        // Specifically verify the paused state is false
        unpause_value
            .object()
            .get("dataset")
            .object()
            .get("paused")
            .assert_bool(false);
    }
}
