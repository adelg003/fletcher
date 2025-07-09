use crate::{
    error::Result,
    model::{
        Compute, DataProduct, DataProductId, DataProductParam, Dataset, DatasetId, DatasetParam,
        Dependency, DependencyParam, Search, State, StateParam,
    },
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};

/// Insert up Update a Dataset
pub async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: &DatasetParam,
    username: &str,
    modified_date: DateTime<Utc>,
) -> Result<Dataset> {
    // Default paused to false for new datasets
    let dataset = query_as!(
        Dataset,
        "INSERT INTO dataset (
            dataset_id,
            paused,
            extra,
            modified_by,
            modified_date
        ) VALUES (
            $1,
            false,
            $2,
            $3,
            $4
        ) ON CONFLICT (dataset_id) DO
        UPDATE SET
            extra = $2,
            modified_by = $3,
            modified_date = $4
        RETURNING
            dataset_id AS id,
            paused,
            extra,
            modified_by,
            modified_date",
        param.id,
        param.extra,
        username,
        modified_date,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dataset)
}

/// Pull a Dataset
pub async fn dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
) -> Result<Dataset> {
    // Select a dataset row
    let dataset = query_as!(
        Dataset,
        "SELECT
            dataset_id AS id,
            paused,
            extra,
            modified_by,
            modified_date
        FROM
            dataset
        WHERE
            dataset_id = $1",
        dataset_id,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dataset)
}

/// Pause / Un-pause a dataset
pub async fn dataset_pause_update(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    paused: bool,
    username: &str,
    modified_date: DateTime<Utc>,
) -> Result<Dataset> {
    let dataset = query_as!(
        Dataset,
        "UPDATE
            dataset
        SET
            paused = $2,
            modified_by = $3,
            modified_date = $4
        WHERE
            dataset_id = $1
        RETURNING
            dataset_id AS id,
            paused,
            extra,
            modified_by,
            modified_date",
        dataset_id,
        paused,
        username,
        modified_date,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dataset)
}

/// Insert or Update a Data Product
pub async fn data_product_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    param: &DataProductParam,
    username: &str,
    modified_date: DateTime<Utc>,
) -> Result<DataProduct> {
    let data_product = query_as!(
        DataProduct,
        r#"INSERT INTO data_product (
            dataset_id,
            data_product_id,
            compute,
            name,
            version,
            eager,
            passthrough,
            state,
            run_id,
            link,
            passback,
            extra,
            modified_by,
            modified_date
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14
        ) ON CONFLICT (dataset_id, data_product_id) DO
        UPDATE SET
            compute = $3,
            name = $4,
            version = $5,
            eager = $6,
            passthrough = $7,
            extra = $12,
            modified_by = $13,
            modified_date = $14
        RETURNING
            data_product_id AS id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            extra,
            modified_by,
            modified_date"#,
        dataset_id,
        param.id,
        param.compute as Compute,
        param.name,
        param.version,
        param.eager,
        param.passthrough,
        State::Waiting as State,
        None::<DatasetId>,
        None::<String>,
        None::<Value>,
        param.extra,
        username,
        modified_date,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(data_product)
}

/// Update the State of a Data Product
pub async fn state_update(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    param: &StateParam,
    username: &str,
    modified_date: DateTime<Utc>,
) -> Result<DataProduct> {
    let data_product = query_as!(
        DataProduct,
        r#"UPDATE
            data_product
        SET
            state = $3,
            run_id = $4,
            link = $5,
            passback = $6,
            modified_by = $7,
            modified_date = $8
        WHERE
            dataset_id = $1
            AND data_product_id = $2
        RETURNING
            data_product_id AS id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            extra,
            modified_by,
            modified_date"#,
        dataset_id,
        param.id,
        param.state as State,
        param.run_id,
        param.link,
        param.passback,
        username,
        modified_date,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(data_product)
}

/// Retrieve a Data Product
pub async fn data_product_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    data_product_id: DataProductId,
) -> Result<DataProduct> {
    let data_product = query_as!(
        DataProduct,
        r#"SELECT
            data_product_id AS id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            extra,
            modified_by,
            modified_date
        FROM
            data_product
        WHERE
            dataset_id = $1
            AND data_product_id = $2"#,
        dataset_id,
        data_product_id,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(data_product)
}

/// Retrieve all Data Products for a Dataset
pub async fn data_products_by_dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
) -> Result<Vec<DataProduct>> {
    let data_products = query_as!(
        DataProduct,
        r#"SELECT
            data_product_id AS id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            extra,
            modified_by,
            modified_date
        FROM
            data_product
        WHERE
            dataset_id = $1"#,
        dataset_id,
    )
    .fetch_all(&mut **tx)
    .await?;

    Ok(data_products)
}

/// Upsert a new Dependency between Data Products
pub async fn dependency_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    param: &DependencyParam,
    username: &str,
    modified_date: DateTime<Utc>,
) -> Result<Dependency> {
    let dependency = query_as!(
        Dependency,
        "INSERT INTO dependency (
            dataset_id,
            parent_id,
            child_id,
            extra,
            modified_by,
            modified_date
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6
        ) ON CONFLICT (dataset_id, parent_id, child_id) DO
        UPDATE SET
            extra = $4,
            modified_by = $5,
            modified_date = $6
        RETURNING
            parent_id,
            child_id,
            extra,
            modified_by,
            modified_date",
        dataset_id,
        param.parent_id,
        param.child_id,
        param.extra,
        username,
        modified_date,
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dependency)
}

/// Retrieve all Dependencies for a Dataset
pub async fn dependencies_by_dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
) -> Result<Vec<Dependency>> {
    let dependencies = query_as!(
        Dependency,
        "SELECT
            parent_id,
            child_id,
            extra,
            modified_by,
            modified_date
        FROM
            dependency
        WHERE
            dataset_id = $1",
        dataset_id,
    )
    .fetch_all(&mut **tx)
    .await?;

    Ok(dependencies)
}

/// Search all plans
pub async fn search_plans_select(
    tx: &mut Transaction<'_, Postgres>,
    search_by: &str,
    limit: u32,
    offset: u32,
) -> Result<Vec<Search>> {
    // Format seach_by so it supports SQL wildcars while allowing for save SQL preperation.
    let search_by: String = format!("%{search_by}%");

    // Pull all datasets that meet our query
    let rows = query_as!(
        Search,
        "SELECT
            ds.dataset_id,
            GREATEST(
                ds.modified_date,
                COALESCE(MAX(dp.modified_date), ds.modified_date),
                COALESCE(MAX(dep.modified_date), ds.modified_date)
            ) AS \"modified_date\"
        FROM
            dataset ds
        LEFT JOIN
            data_product dp
        ON
            ds.dataset_id = dp.dataset_id
        LEFT JOIN
            dependency dep
        ON
            dp.dataset_id = dep.dataset_id
            AND (
                dp.data_product_id = dep.parent_id
                OR dp.data_product_id = dep.child_id
            )
        WHERE
            ds.dataset_id::text ILIKE $1
            OR ds.extra::text ILIKE $1
            OR ds.modified_by ILIKE $1
            OR ds.modified_date::text ILIKE $1
            OR dp.data_product_id::text ILIKE $1
            OR dp.compute::text ILIKE $1
            OR dp.name ILIKE $1
            OR dp.version ILIKE $1
            OR dp.passthrough::text ILIKE $1
            OR dp.state::text ILIKE $1
            OR dp.run_id::text ILIKE $1
            OR dp.link ILIKE $1
            OR dp.passback::text ILIKE $1
            OR dp.extra::text ILIKE $1
            OR dp.modified_by ILIKE $1
            OR dp.modified_date::text ILIKE $1
            OR dep.extra::text ILIKE $1
            OR dep.modified_by ILIKE $1
            OR dep.modified_date::text ILIKE $1
        GROUP BY
            ds.dataset_id
        ORDER BY
            GREATEST(
                MAX(ds.modified_date),
                MAX(dp.modified_date),
                MAX(dep.modified_date)
            ) DESC
        LIMIT
            $2
        OFFSET
            $3",
        search_by,
        i64::from(limit),
        i64::from(offset),
    )
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Error;
    use chrono::Timelike;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Trim a DateTime to micro-seconds (aka the level that Postgres stores timestamps to)
    pub fn trim_to_microseconds(dt: DateTime<Utc>) -> DateTime<Utc> {
        let micros = dt.timestamp_subsec_micros();
        dt.with_nanosecond(micros * 1_000).unwrap()
    }

    /// Test Insert of new Dataset
    #[sqlx::test]
    async fn test_dataset_insert(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Inputs
        let param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        // Test our function
        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // Did we get what we wanted?
        assert_eq!(
            dataset,
            Dataset {
                id: param.id,
                paused: false,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test Update of new Dataset
    #[sqlx::test]
    async fn test_dataset_update(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Inputs
        let param = DatasetParam::default();
        let username = "test";
        let mut modified_date = Utc::now();

        // Setup first record
        dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // Update parameters
        modified_date = Utc::now();

        // Update to new values
        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // Did we get what we wanted?
        assert_eq!(
            dataset,
            Dataset {
                id: param.id,
                paused: false,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }
    /// Test Select of existing Dataset
    #[sqlx::test]
    async fn test_dataset_select(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Inputs
        let param = DatasetParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        // First insert a dataset to select later
        let inserted_dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // Now test dataset_select
        let selected_dataset = dataset_select(&mut tx, param.id).await.unwrap();

        // Did we get what we wanted?
        assert_eq!(selected_dataset, inserted_dataset);
        assert_eq!(
            selected_dataset,
            Dataset {
                id: param.id,
                paused: false,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test Select of non-existent Dataset returns error
    #[sqlx::test]
    async fn test_dataset_select_nonexistent(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Generate a random UUID that doesn't exist in the database
        let nonexistent_dataset_id = Uuid::new_v4();

        // Test that dataset_select returns an error for non-existent dataset
        let result = dataset_select(&mut tx, nonexistent_dataset_id).await;

        // Assert that we got the error we wanted
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test Insert of new Data Product
    #[sqlx::test]
    async fn test_data_product_insert(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset since data_product has a foreign key constraint
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Now create the data product
        let data_product_param = DataProductParam::default();

        // Test our function
        let data_product = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Did we get what we wanted?
        assert_eq!(
            data_product,
            DataProduct {
                id: data_product_param.id,
                compute: data_product_param.compute,
                name: data_product_param.name,
                version: data_product_param.version,
                eager: data_product_param.eager,
                passthrough: data_product_param.passthrough,
                state: State::Waiting,
                run_id: None,
                link: None,
                passback: None,
                extra: data_product_param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            },
        );
    }

    /// Test rejection when data product references non-existent dataset
    #[sqlx::test]
    async fn test_data_product_insert_nonexistent_dataset(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create data product with a fake dataset_id
        let fake_dataset_id = Uuid::new_v4();
        let data_product_param = DataProductParam::default();
        let username = "test";
        let modified_date = Utc::now();

        // Test our function - should fail with foreign key constraint
        let result = data_product_upsert(
            &mut tx,
            fake_dataset_id,
            &data_product_param,
            username,
            modified_date,
        )
        .await;

        // Should get a database error due to foreign key constraint
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test data_product_select can retrieve an existing data product
    #[sqlx::test]
    async fn test_data_product_select_success(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create a data product
        let data_product_param = DataProductParam::default();

        let inserted_data_product = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test data_product_select
        let selected_data_product = data_product_select(&mut tx, dataset.id, data_product_param.id)
            .await
            .unwrap();

        // Verify we got the same data product back
        assert_eq!(selected_data_product, inserted_data_product);
        assert_eq!(
            selected_data_product,
            DataProduct {
                id: data_product_param.id,
                compute: data_product_param.compute,
                name: data_product_param.name,
                version: data_product_param.version,
                eager: data_product_param.eager,
                passthrough: data_product_param.passthrough,
                state: State::Waiting,
                run_id: None,
                link: None,
                passback: None,
                extra: data_product_param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test data_product_select returns error for non-existent data product
    #[sqlx::test]
    async fn test_data_product_select_nonexistent(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Generate random UUIDs that do not exist in the database
        let nonexistent_dataset_id = Uuid::new_v4();
        let nonexistent_data_product_id = Uuid::new_v4();

        // Test that data_product_select returns an error for non-existent data product
        let result =
            data_product_select(&mut tx, nonexistent_dataset_id, nonexistent_data_product_id).await;

        // Assert that we got the error we expected
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test Update of existing Data Product
    #[sqlx::test]
    async fn test_data_product_update(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create initial data product
        let initial_param = DataProductParam::default();

        data_product_upsert(&mut tx, dataset.id, &initial_param, username, modified_date)
            .await
            .unwrap();

        // Now update with different values (same id, same dataset_id)
        let updated_param = DataProductParam {
            id: initial_param.id,             // Same ID to trigger update
            compute: Compute::Dbxaas,         // Changed
            name: "updated-name".to_string(), // Changed
            version: "2.0.0".to_string(),     // Changed
            eager: false,                     // Changed
            passthrough: Some(json!({"updated": "passthrough"})), // Changed
            extra: Some(json!({"updated": "extra"})), // Changed
        };

        let updated_modified_date = Utc::now();
        let updated_username = "updater";

        // Test the update
        let updated_data_product = data_product_upsert(
            &mut tx,
            dataset.id,
            &updated_param,
            updated_username,
            updated_modified_date,
        )
        .await
        .unwrap();

        // Verify the update worked
        assert_eq!(
            updated_data_product,
            DataProduct {
                id: updated_param.id,
                compute: updated_param.compute,
                name: updated_param.name,
                version: updated_param.version,
                eager: updated_param.eager,
                passthrough: updated_param.passthrough,
                state: State::Waiting,
                run_id: None,
                link: None,
                passback: None,
                extra: updated_param.extra,
                modified_by: updated_username.to_string(),
                modified_date: trim_to_microseconds(updated_modified_date),
            },
        );
    }

    /// Test successful update of Data Product State
    #[sqlx::test]
    async fn test_state_update_success(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create initial data product
        let data_product_param = DataProductParam::default();

        let initial_data_product = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Now update the state
        let state_param = StateParam {
            id: data_product_param.id,
            ..Default::default()
        };
        let updated_modified_date = Utc::now();
        let updated_username = "state_updater";

        // Test the state update
        let updated_data_product = state_update(
            &mut tx,
            dataset.id,
            &state_param,
            updated_username,
            updated_modified_date,
        )
        .await
        .unwrap();

        // Verify the state update worked
        assert_eq!(
            updated_data_product,
            DataProduct {
                id: initial_data_product.id,
                compute: initial_data_product.compute,
                name: initial_data_product.name,
                version: initial_data_product.version,
                eager: initial_data_product.eager,
                passthrough: initial_data_product.passthrough,
                state: state_param.state,
                run_id: state_param.run_id,
                link: state_param.link,
                passback: state_param.passback,
                extra: initial_data_product.extra,
                modified_by: updated_username.to_string(),
                modified_date: trim_to_microseconds(updated_modified_date),
            }
        );
    }

    /// Test rejection when updating state of non-existent data product
    #[sqlx::test]
    async fn test_state_update_nonexistent_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset so dataset exists but no data product
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Try to update state for a non-existent data product
        let fake_data_product_id = Uuid::new_v4();

        // Now update the state
        let state_param = StateParam {
            id: fake_data_product_id,
            ..Default::default()
        };

        // Test state update - should fail since data product doesn't exist
        let result = state_update(&mut tx, dataset.id, &state_param, username, modified_date).await;

        // Should get a RowNotFound error since the data product doesn't exist
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test data_products_by_dataset_select returns all data products for a dataset
    #[sqlx::test]
    async fn test_data_products_by_dataset_select(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create multiple data products for this dataset
        let data_product_1 = DataProductParam::default();
        let data_product_2 = DataProductParam {
            compute: Compute::Dbxaas,
            name: "data-product-2".to_string(),
            version: "2.0.0".to_string(),
            eager: false,
            passthrough: None,
            extra: Some(json!({"extra2": "data2"})),
            ..Default::default()
        };

        let inserted_dp1 = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_1,
            username,
            modified_date,
        )
        .await
        .unwrap();

        let inserted_dp2 = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_2,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test data_products_by_dataset_select
        let retrieved_data_products = data_products_by_dataset_select(&mut tx, dataset.id)
            .await
            .unwrap();

        // Verify we got both data products
        assert_eq!(retrieved_data_products.len(), 2);
        assert!(retrieved_data_products.contains(&inserted_dp1));
        assert!(retrieved_data_products.contains(&inserted_dp2));
    }

    /// Test dependency_upsert can add a new dependency
    #[sqlx::test]
    async fn test_dependency_upsert_insert(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create two data products to create a dependency between them
        let parent_param = DataProductParam::default();
        let child_param = DataProductParam {
            compute: Compute::Dbxaas,
            name: "child-data-product".to_string(),
            eager: false,
            ..Default::default()
        };

        let parent_dp =
            data_product_upsert(&mut tx, dataset.id, &parent_param, username, modified_date)
                .await
                .unwrap();
        let child_dp =
            data_product_upsert(&mut tx, dataset.id, &child_param, username, modified_date)
                .await
                .unwrap();

        // Create a dependency
        let dependency_param = DependencyParam {
            parent_id: parent_dp.id,
            child_id: child_dp.id,
            ..Default::default()
        };

        // Test dependency_upsert
        let dependency = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Verify the dependency was created correctly
        assert_eq!(
            dependency,
            Dependency {
                parent_id: dependency_param.parent_id,
                child_id: dependency_param.child_id,
                extra: dependency_param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test dependency_upsert can update an existing dependency
    #[sqlx::test]
    async fn test_dependency_upsert_update(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create two data products
        let parent_param = DataProductParam::default();
        let child_param = DataProductParam {
            compute: Compute::Dbxaas,
            name: "child-data-product".to_string(),
            eager: false,
            ..Default::default()
        };

        let parent_dp =
            data_product_upsert(&mut tx, dataset.id, &parent_param, username, modified_date)
                .await
                .unwrap();
        let child_dp =
            data_product_upsert(&mut tx, dataset.id, &child_param, username, modified_date)
                .await
                .unwrap();

        // Create initial dependency
        let initial_dependency_param = DependencyParam {
            parent_id: parent_dp.id,
            child_id: child_dp.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &initial_dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Update the dependency
        let updated_dependency_param = DependencyParam {
            parent_id: parent_dp.id,
            child_id: child_dp.id,
            ..Default::default()
        };

        let updated_modified_date = Utc::now();
        let updated_username = "updater";

        // Test dependency update
        let updated_dependency = dependency_upsert(
            &mut tx,
            dataset.id,
            &updated_dependency_param,
            updated_username,
            updated_modified_date,
        )
        .await
        .unwrap();

        // Verify the dependency was updated correctly
        assert_eq!(
            updated_dependency,
            Dependency {
                parent_id: updated_dependency_param.parent_id,
                child_id: updated_dependency_param.child_id,
                extra: updated_dependency_param.extra,
                modified_by: updated_username.to_string(),
                modified_date: trim_to_microseconds(updated_modified_date),
            }
        );
    }

    /// Test dependency_upsert rejects dependency with non-existent parent
    #[sqlx::test]
    async fn test_dependency_upsert_nonexistent_parent(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create only a child data product (no parent)
        let child_param = DataProductParam::default();

        let child_dp =
            data_product_upsert(&mut tx, dataset.id, &child_param, username, modified_date)
                .await
                .unwrap();

        // Try to create dependency with non-existent parent
        let dependency_param = DependencyParam {
            parent_id: Uuid::new_v4(), // Fake parent id
            child_id: child_dp.id,
            ..Default::default()
        };

        // Test dependency_upsert - should fail with foreign key constraint
        let result = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await;

        // Should get a database error due to foreign key constraint
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependency_upsert rejects dependency with non-existent child
    #[sqlx::test]
    async fn test_dependency_upsert_nonexistent_child(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create only a parent data product (no child)
        let parent_param = DataProductParam::default();

        let parent_dp =
            data_product_upsert(&mut tx, dataset.id, &parent_param, username, modified_date)
                .await
                .unwrap();

        // Try to create dependency with non-existent child
        let dependency_param = DependencyParam {
            parent_id: parent_dp.id,
            child_id: Uuid::new_v4(), // Fake child id
            ..Default::default()
        };

        // Test dependency_upsert - should fail with foreign key constraint
        let result = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await;

        // Should get a database error due to foreign key constraint
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependency_upsert rejects self-referencing dependency
    #[sqlx::test]
    async fn test_dependency_upsert_self_reference(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create a data product
        let data_product_param = DataProductParam::default();

        let data_product = data_product_upsert(
            &mut tx,
            dataset.id,
            &data_product_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Try to create a self-referencing dependency
        let dependency_param = DependencyParam {
            parent_id: data_product.id,
            child_id: data_product.id, // Same as parent - should be rejected
            ..Default::default()
        };

        // Test dependency_upsert - should fail with check constraint
        let result = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await;

        // Should get a database error due to check constraint
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependencies_by_dataset_select returns all dependencies for a dataset
    #[sqlx::test]
    async fn test_dependencies_by_dataset_select(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create a dataset
        let dataset_param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create three data products to create multiple dependencies
        let dp1_param = DataProductParam {
            name: "data-product-1".to_string(),
            ..Default::default()
        };

        let dp2_param = DataProductParam {
            compute: Compute::Dbxaas,
            name: "data-product-2".to_string(),
            eager: false,
            ..Default::default()
        };

        let dp3_param = DataProductParam {
            name: "data-product-3".to_string(),
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();
        let dp3 = data_product_upsert(&mut tx, dataset.id, &dp3_param, username, modified_date)
            .await
            .unwrap();

        // Create multiple dependencies: dp1 -> dp2, dp2 -> dp3
        let dependency1_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        let dependency2_param = DependencyParam {
            parent_id: dp2.id,
            child_id: dp3.id,
            ..Default::default()
        };

        let dep1 = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency1_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        let dep2 = dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency2_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test dependencies_by_dataset_select
        let retrieved_dependencies = dependencies_by_dataset_select(&mut tx, dataset.id)
            .await
            .unwrap();

        // Verify we got both dependencies
        assert_eq!(retrieved_dependencies.len(), 2);
        assert!(retrieved_dependencies.contains(&dep1));
        assert!(retrieved_dependencies.contains(&dep2));
    }

    /// Test dataset_pause_update setting pause to true
    #[sqlx::test]
    async fn test_dataset_pause_update_to_true(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset
        let param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // Verify dataset starts as not paused
        assert_eq!(dataset.paused, false);

        // Now pause the dataset
        let pause_username = "pause_user";
        let pause_modified_date = Utc::now();

        let paused_dataset = dataset_pause_update(
            &mut tx,
            dataset.id,
            true,
            pause_username,
            pause_modified_date,
        )
        .await
        .unwrap();

        // Verify the dataset is now paused
        assert_eq!(
            paused_dataset,
            Dataset {
                id: dataset.id,
                paused: true,
                extra: dataset.extra,
                modified_by: pause_username.to_string(),
                modified_date: trim_to_microseconds(pause_modified_date),
            }
        );
    }

    /// Test dataset_pause_update setting pause to false
    #[sqlx::test]
    async fn test_dataset_pause_update_to_false(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // First create a dataset
        let param = DatasetParam::default();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();

        // First pause the dataset
        let pause_username = "pause_user";
        let pause_modified_date = Utc::now();

        let paused_dataset = dataset_pause_update(
            &mut tx,
            dataset.id,
            true,
            pause_username,
            pause_modified_date,
        )
        .await
        .unwrap();

        // Verify it's paused
        assert_eq!(paused_dataset.paused, true);

        // Now unpause the dataset
        let unpause_username = "unpause_user";
        let unpause_modified_date = Utc::now();

        let unpaused_dataset = dataset_pause_update(
            &mut tx,
            dataset.id,
            false,
            unpause_username,
            unpause_modified_date,
        )
        .await
        .unwrap();

        // Verify the dataset is now unpaused
        assert_eq!(
            unpaused_dataset,
            Dataset {
                id: dataset.id,
                paused: false,
                extra: dataset.extra,
                modified_by: unpause_username.to_string(),
                modified_date: trim_to_microseconds(unpause_modified_date),
            }
        );
    }

    /// Test dataset_pause_update with nonexistent dataset returns error
    #[sqlx::test]
    async fn test_dataset_pause_update_nonexistent(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Generate a random UUID that doesn't exist in the database
        let nonexistent_dataset_id = Uuid::new_v4();
        let username = "test";
        let modified_date = Utc::now();

        // Test that dataset_pause_update returns an error for non-existent dataset
        let result = dataset_pause_update(
            &mut tx,
            nonexistent_dataset_id,
            true,
            username,
            modified_date,
        )
        .await;

        // Assert that we got the error we wanted
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test search_plans_select with basic search functionality
    #[sqlx::test]
    async fn test_search_plans_select_basic(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let dataset_param = DatasetParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create data products
        let dp1_param = DataProductParam {
            name: "test-product".to_string(),
            ..Default::default()
        };
        let dp2_param = DataProductParam {
            name: "another-product".to_string(),
            compute: Compute::Dbxaas,
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();

        // Create dependency
        let dependency_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test search by product name
        let results = search_plans_select(&mut tx, "test-product", 10, 0)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);
        assert!(results[0].modified_date.is_some());
    }

    /// Test search_plans_select with no matching results
    #[sqlx::test]
    async fn test_search_plans_select_no_results(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let dataset_param = DatasetParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create data products and dependency
        let dp1_param = DataProductParam::default();
        let dp2_param = DataProductParam {
            compute: Compute::Dbxaas,
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();

        let dependency_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test search with non-matching term
        let results = search_plans_select(&mut tx, "nonexistent-term", 10, 0)
            .await
            .unwrap();

        assert_eq!(results.len(), 0);
    }

    /// Test search_plans_select with empty search string
    #[sqlx::test]
    async fn test_search_plans_select_empty_search(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let dataset_param = DatasetParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create data products and dependency
        let dp1_param = DataProductParam::default();
        let dp2_param = DataProductParam {
            compute: Compute::Dbxaas,
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();

        let dependency_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test search with empty string (should match all)
        let results = search_plans_select(&mut tx, "", 10, 0).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);
    }

    /// Test search_plans_select with pagination
    #[sqlx::test]
    async fn test_search_plans_select_pagination(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create multiple datasets to test pagination
        let username = "test_user";
        let modified_date = Utc::now();

        let mut datasets = Vec::new();
        for i in 0..3 {
            let dataset_param = DatasetParam {
                id: Uuid::new_v4(),
                extra: Some(json!({"name": format!("dataset-{}", i)})),
            };

            let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
                .await
                .unwrap();
            datasets.push(dataset.clone());

            // Create data products and dependencies for each dataset
            let dp1_param = DataProductParam::default();
            let dp2_param = DataProductParam {
                compute: Compute::Dbxaas,
                ..Default::default()
            };

            let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
                .await
                .unwrap();
            let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
                .await
                .unwrap();

            let dependency_param = DependencyParam {
                parent_id: dp1.id,
                child_id: dp2.id,
                ..Default::default()
            };

            dependency_upsert(
                &mut tx,
                dataset.id,
                &dependency_param,
                username,
                modified_date,
            )
            .await
            .unwrap();
        }

        // Test pagination with limit
        let results = search_plans_select(&mut tx, "dataset", 2, 0).await.unwrap();
        assert_eq!(results.len(), 2);

        // Test pagination with offset
        let results = search_plans_select(&mut tx, "dataset", 2, 1).await.unwrap();
        assert_eq!(results.len(), 2);

        // Test pagination with offset beyond available results
        let results = search_plans_select(&mut tx, "dataset", 10, 10)
            .await
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    /// Test search_plans_select case-insensitive search
    #[sqlx::test]
    async fn test_search_plans_select_case_insensitive(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let dataset_param = DatasetParam::default();
        let username = "TestUser";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create data products with uppercase names
        let dp1_param = DataProductParam {
            name: "TEST-PRODUCT".to_string(),
            ..Default::default()
        };
        let dp2_param = DataProductParam {
            name: "ANOTHER-PRODUCT".to_string(),
            compute: Compute::Dbxaas,
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();

        let dependency_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test case-insensitive search (lowercase search should match uppercase data)
        let results = search_plans_select(&mut tx, "test-product", 10, 0)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);

        // Test search by username (case-insensitive)
        let results = search_plans_select(&mut tx, "testuser", 10, 0)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);
    }

    /// Test search_plans_select across different fields
    #[sqlx::test]
    async fn test_search_plans_select_different_fields(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let dataset_param = DatasetParam {
            extra: Some(json!({"description": "unique-dataset-description"})),
            ..Default::default()
        };
        let username = "search_user";
        let modified_date = Utc::now();

        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();

        // Create data products with specific fields
        let dp1_param = DataProductParam {
            name: "search-product".to_string(),
            version: "unique-version-123".to_string(),
            ..Default::default()
        };
        let dp2_param = DataProductParam {
            compute: Compute::Dbxaas,
            ..Default::default()
        };

        let dp1 = data_product_upsert(&mut tx, dataset.id, &dp1_param, username, modified_date)
            .await
            .unwrap();
        let dp2 = data_product_upsert(&mut tx, dataset.id, &dp2_param, username, modified_date)
            .await
            .unwrap();

        let dependency_param = DependencyParam {
            parent_id: dp1.id,
            child_id: dp2.id,
            ..Default::default()
        };

        dependency_upsert(
            &mut tx,
            dataset.id,
            &dependency_param,
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Test search by dataset extra field
        let results = search_plans_select(&mut tx, "unique-dataset-description", 10, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);

        // Test search by data product version
        let results = search_plans_select(&mut tx, "unique-version-123", 10, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);

        // Test search by username
        let results = search_plans_select(&mut tx, "search_user", 10, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);

        // Test search by compute type
        let results = search_plans_select(&mut tx, "dbxaas", 10, 0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].dataset_id, dataset.id);
    }
}
