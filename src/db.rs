use crate::{
    error::Result,
    model::{
        Compute, DataProduct, DataProductParam, Dataset, DatasetId, DatasetParam, Dependency,
        DependencyParam, State, StateParam,
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
            $2,
            $3,
            $4,
            $5
        ) ON CONFLICT (dataset_id) DO
        UPDATE SET
            paused = $2,
            extra = $3,
            modified_by = $4,
            modified_date = $5
        RETURNING
            dataset_id AS id,
            paused,
            extra,
            modified_by,
            modified_date",
        param.id,
        param.paused,
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
            extra = $7,
            modified_by = $8,
            modified_date = $9
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
        param.extra,
        username,
        modified_date,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Trim a DateTime to micro-seconds (aka the level that Postgres stores timestamps to)
    fn trim_to_microseconds(dt: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let micros = dt.timestamp_subsec_micros();
        dt.with_nanosecond(micros * 1_000)
    }

    /// Test Insert of new Dataset
    #[sqlx::test]
    async fn test_dataset_insert(pool: PgPool) {
        // Inputs
        let param = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: Some(json!({"test": "data"})),
        };
        let username = "test";
        let modified_date = Utc::now();

        // Test our function
        let mut tx = pool.begin().await.unwrap();
        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Did we get what we wanted?
        assert_eq!(
            dataset,
            Dataset {
                id: param.id,
                paused: param.paused,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date).unwrap(),
            }
        );
    }

    /// Test Update of new Dataset
    #[sqlx::test]
    async fn test_dataset_update(pool: PgPool) {
        // Inputs
        let mut param = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: None,
        };
        let username = "test";
        let mut modified_date = Utc::now();

        // Setup first record
        let mut tx = pool.begin().await.unwrap();
        dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Update parameters
        param.paused = true;
        modified_date = Utc::now();

        // Update to new values
        let mut tx = pool.begin().await.unwrap();
        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Did we get what we wanted?
        assert_eq!(
            dataset,
            Dataset {
                id: param.id,
                paused: param.paused,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date).unwrap(),
            }
        );
    }
    /// Test Select of existing Dataset
    #[sqlx::test]
    async fn test_dataset_select(pool: PgPool) {
        // Inputs
        let param = DatasetParam {
            id: Uuid::new_v4(),
            paused: true,
            extra: Some(json!({"test": "data"})),
        };
        let username = "test_user";
        let modified_date = Utc::now();

        // First insert a dataset to select later
        let mut tx = pool.begin().await.unwrap();
        let inserted_dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Now test dataset_select
        let mut tx = pool.begin().await.unwrap();
        let selected_dataset = dataset_select(&mut tx, param.id).await.unwrap();
        tx.rollback().await.unwrap();

        // Did we get what we wanted?
        assert_eq!(selected_dataset, inserted_dataset);
        assert_eq!(
            selected_dataset,
            Dataset {
                id: param.id,
                paused: param.paused,
                extra: param.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date).unwrap(),
            }
        );
    }
}

/// Test Select of non-existent Dataset returns error
    #[sqlx::test]
    async fn test_dataset_select_nonexistent(pool: PgPool) {
        // Generate a random UUID that doesn't exist in the database
        let nonexistent_dataset_id = Uuid::new_v4();

        // Test that dataset_select returns an error for non-existent dataset
        let mut tx = pool.begin().await.unwrap();
        let result = dataset_select(&mut tx, nonexistent_dataset_id).await;
        tx.rollback().await.unwrap();

        // Assert that we got an error
        assert!(result.is_err(), "Expected error when selecting non-existent dataset");
    }
/// Test retrieving all data products for a dataset
    #[sqlx::test]
    async fn test_data_products_by_dataset_select(pool: PgPool) {
        // Create a dataset first
        let dataset_param = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: None,
        };
        let username = "test_user";
        let modified_date = Utc::now();

        let mut tx = pool.begin().await.unwrap();
        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Create multiple data products for this dataset
        let data_product_param1 = DataProductParam {
            id: Uuid::new_v4(),
            compute: Compute::Sql,
            name: "Test Product 1".to_string(),
            version: "1.0.0".to_string(),
            eager: false,
            passthrough: false,
            extra: Some(json!({"test": "data1"})),
        };
        let data_product_param2 = DataProductParam {
            id: Uuid::new_v4(),
            compute: Compute::Python,
            name: "Test Product 2".to_string(),
            version: "2.0.0".to_string(),
            eager: true,
            passthrough: true,
            extra: Some(json!({"test": "data2"})),
        };

        // Insert both data products
        let mut tx = pool.begin().await.unwrap();
        let inserted_product1 = data_product_upsert(&mut tx, dataset.id, &data_product_param1, username, modified_date)
            .await
            .unwrap();
        let inserted_product2 = data_product_upsert(&mut tx, dataset.id, &data_product_param2, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Test retrieving all data products for the dataset
        let mut tx = pool.begin().await.unwrap();
        let mut retrieved_products = data_products_by_dataset_select(&mut tx, dataset.id)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(retrieved_products.len(), 2);
        // Sort by id for deterministic comparison
        let mut expected = vec![inserted_product1, inserted_product2];
        expected.sort_by_key(|p| p.id);
        retrieved_products.sort_by_key(|p| p.id);
        assert_eq!(retrieved_products, expected);
    }

    /// Test retrieving data products for dataset with no data products (empty result)
    #[sqlx::test]
    async fn test_data_products_by_dataset_select_empty(pool: PgPool) {
        // Create a dataset with no data products
        let dataset_param = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: None,
        };
        let username = "test_user";
        let modified_date = Utc::now();

        let mut tx = pool.begin().await.unwrap();
        let dataset = dataset_upsert(&mut tx, &dataset_param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Test retrieving data products for dataset with no data products
        let mut tx = pool.begin().await.unwrap();
        let retrieved_products = data_products_by_dataset_select(&mut tx, dataset.id)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(retrieved_products.len(), 0);
        assert!(retrieved_products.is_empty());
    }

    /// Test that data_products_by_dataset_select only returns products for the specified dataset
    #[sqlx::test]
    async fn test_data_products_by_dataset_select_isolation(pool: PgPool) {
        let username = "test_user";
        let modified_date = Utc::now();

        // Create two different datasets
        let dataset_param1 = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: None,
        };
        let dataset_param2 = DatasetParam {
            id: Uuid::new_v4(),
            paused: false,
            extra: None,
        };
        let mut tx = pool.begin().await.unwrap();
        let dataset1 = dataset_upsert(&mut tx, &dataset_param1, username, modified_date)
            .await
            .unwrap();
        let dataset2 = dataset_upsert(&mut tx, &dataset_param2, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Insert one product into each dataset
        let data_product_param1 = DataProductParam {
            id: Uuid::new_v4(),
            compute: Compute::Sql,
            name: "Dataset1 Product".to_string(),
            version: "1.0.0".to_string(),
            eager: false,
            passthrough: false,
            extra: None,
        };
        let data_product_param2 = DataProductParam {
            id: Uuid::new_v4(),
            compute: Compute::Python,
            name: "Dataset2 Product".to_string(),
            version: "1.0.0".to_string(),
            eager: false,
            passthrough: false,
            extra: None,
        };
        let mut tx = pool.begin().await.unwrap();
        let inserted1 = data_product_upsert(&mut tx, dataset1.id, &data_product_param1, username, modified_date)
            .await
            .unwrap();
        let _ = data_product_upsert(&mut tx, dataset2.id, &data_product_param2, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Only dataset1's products should be returned
        let mut tx = pool.begin().await.unwrap();
        let result = data_products_by_dataset_select(&mut tx, dataset1.id)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], inserted1);
        assert_eq!(result[0].name, "Dataset1 Product");
    }