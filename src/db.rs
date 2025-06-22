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
    use crate::error::Error;
    use chrono::Timelike;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Trim a DateTime to micro-seconds (aka the level that Postgres stores timestamps to)
    fn trim_to_microseconds(dt: DateTime<Utc>) -> DateTime<Utc> {
        let micros = dt.timestamp_subsec_micros();
        dt.with_nanosecond(micros * 1_000).unwrap()
    }

    /// Helper function to create a test dataset with sensible defaults
    async fn create_test_dataset(
        pool: &PgPool,
        id: Option<Uuid>,
        paused: Option<bool>,
        extra: Option<serde_json::Value>,
        username: Option<&str>,
        modified_date: Option<DateTime<Utc>>,
    ) -> Dataset {
        let param = DatasetParam {
            id: id.unwrap_or_else(Uuid::new_v4),
            paused: paused.unwrap_or(false),
            extra: extra.or_else(|| Some(json!({"test":"dataset"}))),
        };
        let username = username.unwrap_or("test");
        let modified_date = modified_date.unwrap_or_else(Utc::now);

        let mut tx = pool.begin().await.unwrap();
        let dataset = dataset_upsert(&mut tx, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        dataset
    }

    /// Helper function to create a test data product with sensible defaults
    async fn create_test_data_product(
        pool: &PgPool,
        dataset_id: Uuid,
        id: Option<Uuid>,
        compute: Option<Compute>,
        name: Option<String>,
        version: Option<String>,
        eager: Option<bool>,
        passthrough: Option<serde_json::Value>,
        extra: Option<serde_json::Value>,
        username: Option<&str>,
        modified_date: Option<DateTime<Utc>>,
    ) -> DataProduct {
        let param = DataProductParam {
            id: id.unwrap_or_else(Uuid::new_v4),
            compute: compute.unwrap_or(Compute::Cams),
            name: name.unwrap_or_else(|| "test-data-product".to_string()),
            version: version.unwrap_or_else(|| "1.0.0".to_string()),
            eager: eager.unwrap_or(true),
            passthrough: passthrough.or_else(|| Some(json!({"test":"passthrough"}))),
            extra: extra.or_else(|| Some(json!({"test":"extra"}))),
        };
        let username = username.unwrap_or("test");
        let modified_date = modified_date.unwrap_or_else(Utc::now);

        let mut tx = pool.begin().await.unwrap();
        let data_product = data_product_upsert(&mut tx, dataset_id, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        data_product
    }

    /// Helper function to create a test dependency with sensible defaults
    async fn create_test_dependency(
        pool: &PgPool,
        dataset_id: Uuid,
        parent_id: Uuid,
        child_id: Uuid,
        extra: Option<serde_json::Value>,
        username: Option<&str>,
        modified_date: Option<DateTime<Utc>>,
    ) -> Dependency {
        let param = DependencyParam {
            parent_id,
            child_id,
            extra: extra.or_else(|| Some(json!({"test":"dependency"}))),
        };
        let username = username.unwrap_or("test");
        let modified_date = modified_date.unwrap_or_else(Utc::now);

        let mut tx = pool.begin().await.unwrap();
        let dependency = dependency_upsert(&mut tx, dataset_id, &param, username, modified_date)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        dependency
    }

    /// Helper function to create a test dataset and data product together
    async fn create_test_dataset_and_data_product(
        pool: &PgPool,
        dataset_options: Option<(Option<Uuid>, Option<bool>, Option<serde_json::Value>)>,
        data_product_options: Option<(Option<Uuid>, Option<Compute>, Option<String>, Option<String>, Option<bool>)>,
        username: Option<&str>,
        modified_date: Option<DateTime<Utc>>,
    ) -> (Dataset, DataProduct) {
        let username = username.unwrap_or("test");
        let modified_date = modified_date.unwrap_or_else(Utc::now);

        let (dataset_id, dataset_paused, dataset_extra) =
            dataset_options.unwrap_or((None, None, None));
        let dataset = create_test_dataset(
            pool,
            dataset_id,
            dataset_paused,
            dataset_extra,
            Some(username),
            Some(modified_date),
        )
        .await;

        let (dp_id, dp_compute, dp_name, dp_version, dp_eager) =
            data_product_options.unwrap_or((None, None, None, None, None));
        let data_product = create_test_data_product(
            pool,
            dataset.id,
            dp_id,
            dp_compute,
            dp_name,
            dp_version,
            dp_eager,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;

        (dataset, data_product)
    }

    /// Test Insert of new Dataset
    #[sqlx::test]
    async fn test_dataset_insert(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let extra_data = Some(json!({"test":"data"}));
        let username = "test";
        let modified_date = Utc::now();

        let dataset = create_test_dataset(
            &pool,
            Some(dataset_id),
            Some(false),
            extra_data.clone(),
            Some(username),
            Some(modified_date),
        )
        .await;

        assert_eq!(
            dataset,
            Dataset {
                id: dataset_id,
                paused: false,
                extra: extra_data,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test Update of new Dataset
    #[sqlx::test]
    async fn test_dataset_update(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let username = "test";
        let initial_modified_date = Utc::now();

        // Create initial dataset
        create_test_dataset(
            &pool,
            Some(dataset_id),
            Some(false),
            None,
            Some(username),
            Some(initial_modified_date),
        )
        .await;

        let updated_modified_date = Utc::now();
        let updated_dataset = create_test_dataset(
            &pool,
            Some(dataset_id),
            Some(true),
            None,
            Some(username),
            Some(updated_modified_date),
        )
        .await;

        assert_eq!(
            updated_dataset,
            Dataset {
                id: dataset_id,
                paused: true,
                extra: Some(json!({"test":"dataset"})),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(updated_modified_date),
            }
        );
    }

    /// Test Select of existing Dataset
    #[sqlx::test]
    async fn test_dataset_select(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let extra_data = Some(json!({"test":"data"}));
        let username = "test_user";
        let modified_date = Utc::now();

        let inserted_dataset = create_test_dataset(
            &pool,
            Some(dataset_id),
            Some(true),
            extra_data.clone(),
            Some(username),
            Some(modified_date),
        )
        .await;

        let mut tx = pool.begin().await.unwrap();
        let selected_dataset = dataset_select(&mut tx, dataset_id).await.unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(selected_dataset, inserted_dataset);
    }

    /// Test Select of non-existent Dataset returns error
    #[sqlx::test]
    async fn test_dataset_select_nonexistent(pool: PgPool) {
        let nonexistent_dataset_id = Uuid::new_v4();

        let mut tx = pool.begin().await.unwrap();
        let result = dataset_select(&mut tx, nonexistent_dataset_id).await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test Insert of new Data Product
    #[sqlx::test]
    async fn test_data_product_insert(pool: PgPool) {
        let data_product_id = Uuid::new_v4();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let data_product = create_test_data_product(
            &pool,
            dataset.id,
            Some(data_product_id),
            Some(Compute::Cams),
            Some("test-data-product".to_string()),
            Some("1.0.0".to_string()),
            Some(true),
            Some(json!({"test":"passthrough"})),
            Some(json!({"test":"extra"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        assert_eq!(
            data_product,
            DataProduct {
                id: data_product_id,
                compute: Compute::Cams,
                name: "test-data-product".to_string(),
                version: "1.0.0".to_string(),
                eager: true,
                passthrough: Some(json!({"test":"passthrough"})),
                state: State::Waiting,
                run_id: None,
                link: None,
                passback: None,
                extra: Some(json!({"test":"extra"})),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test rejection when data product references non-existent dataset
    #[sqlx::test]
    async fn test_data_product_insert_nonexistent_dataset(pool: PgPool) {
        let fake_dataset_id = Uuid::new_v4();
        let data_product_param = DataProductParam {
            id: Uuid::new_v4(),
            compute: Compute::Dbxaas,
            name: "test-data-product".to_string(),
            version: "1.0.0".to_string(),
            eager: false,
            passthrough: None,
            extra: None,
        };
        let username = "test";
        let modified_date = Utc::now();

        let mut tx = pool.begin().await.unwrap();
        let result = data_product_upsert(
            &mut tx,
            fake_dataset_id,
            &data_product_param,
            username,
            modified_date,
        )
        .await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test Update of existing Data Product
    #[sqlx::test]
    async fn test_data_product_update(pool: PgPool) {
        let data_product_id = Uuid::new_v4();
        let username = "test";
        let modified_date = Utc::now();

        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        create_test_data_product(
            &pool,
            dataset.id,
            Some(data_product_id),
            Some(Compute::Cams),
            Some("initial-name".to_string()),
            Some("1.0.0".to_string()),
            Some(true),
            Some(json!({"initial":"data"})),
            Some(json!({"initial":"extra"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        let updated_modified_date = Utc::now();
        let updated_username = "updater";
        let updated_data_product = create_test_data_product(
            &pool,
            dataset.id,
            Some(data_product_id),
            Some(Compute::Dbxaas),
            Some("updated-name".to_string()),
            Some("2.0.0".to_string()),
            Some(false),
            Some(json!({"updated":"passthrough"})),
            Some(json!({"updated":"extra"})),
            Some(updated_username),
            Some(updated_modified_date),
        )
        .await;

        assert_eq!(
            updated_data_product,
            DataProduct {
                id: data_product_id,
                compute: Compute::Dbxaas,
                name: "updated-name".to_string(),
                version: "2.0.0".to_string(),
                eager: false,
                passthrough: Some(json!({"updated":"passthrough"})),
                state: State::Waiting,
                run_id: None,
                link: None,
                passback: None,
                extra: Some(json!({"updated":"extra"})),
                modified_by: updated_username.to_string(),
                modified_date: trim_to_microseconds(updated_modified_date),
            }
        );
    }

    /// Test successful update of Data Product State
    #[sqlx::test]
    async fn test_state_update_success(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let initial_data_product = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;

        let state_param = StateParam {
            id: initial_data_product.id,
            state: State::Running,
            run_id: Some(Uuid::new_v4()),
            link: Some("https://example.com/run".to_string()),
            passback: Some(json!({"status":"running"})),
        };
        let updated_modified_date = Utc::now();
        let updated_username = "state_updater";

        let mut tx = pool.begin().await.unwrap();
        let updated_data_product = state_update(
            &mut tx,
            dataset.id,
            &state_param,
            updated_username,
            updated_modified_date,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

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
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;

        let fake_data_product_id = Uuid::new_v4();
        let state_param = StateParam {
            id: fake_data_product_id,
            state: State::Success,
            run_id: Some(Uuid::new_v4()),
            link: Some("https://example.com/completed".to_string()),
            passback: Some(json!({"status":"complete"})),
        };

        let mut tx = pool.begin().await.unwrap();
        let result = state_update(&mut tx, dataset.id, &state_param, username, modified_date).await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test data_products_by_dataset_select returns all data products for a dataset
    #[sqlx::test]
    async fn test_data_products_by_dataset_select(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let dp1 = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            Some(Compute::Cams),
            Some("data-product-1".to_string()),
            Some("1.0.0".to_string()),
            Some(true),
            Some(json!({"test1":"data1"})),
            Some(json!({"extra1":"data1"})),
            Some(username),
            Some(modified_date),
        )
        .await;
        let dp2 = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            Some(Compute::Dbxaas),
            Some("data-product-2".to_string()),
            Some("2.0.0".to_string()),
            Some(false),
            None,
            Some(json!({"extra2":"data2"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        let mut tx = pool.begin().await.unwrap();
        let retrieved = data_products_by_dataset_select(&mut tx, dataset.id)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(retrieved.len(), 2);
        assert!(retrieved.contains(&dp1));
        assert!(retrieved.contains(&dp2));
    }

    /// Test dependency_upsert can add a new dependency
    #[sqlx::test]
    async fn test_dependency_upsert_insert(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let parent = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let child = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let dependency = create_test_dependency(
            &pool,
            dataset.id,
            parent.id,
            child.id,
            Some(json!({"dependency":"test"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        assert_eq!(
            dependency,
            Dependency {
                parent_id: parent.id,
                child_id: child.id,
                extra: Some(json!({"dependency":"test"})),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test dependency_upsert can update an existing dependency
    #[sqlx::test]
    async fn test_dependency_upsert_update(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let parent = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let child = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        create_test_dependency(
            &pool,
            dataset.id,
            parent.id,
            child.id,
            Some(json!({"initial":"data"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        let updated = create_test_dependency(
            &pool,
            dataset.id,
            parent.id,
            child.id,
            Some(json!({"updated":"data"})),
            Some("updater"),
            Some(Utc::now()),
        )
        .await;

        assert_eq!(
            updated,
            Dependency {
                parent_id: parent.id,
                child_id: child.id,
                extra: Some(json!({"updated":"data"})),
                modified_by: "updater".to_string(),
                modified_date: trim_to_microseconds(updated.modified_date),
            }
        );
    }

    /// Test dependency_upsert rejects dependency with non-existent parent
    #[sqlx::test]
    async fn test_dependency_upsert_nonexistent_parent(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let child = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let fake_parent = Uuid::new_v4();
        let param = DependencyParam {
            parent_id: fake_parent,
            child_id: child.id,
            extra: Some(json!({"test":"dependency"})),
        };

        let mut tx = pool.begin().await.unwrap();
        let result = dependency_upsert(&mut tx, dataset.id, &param, username, modified_date).await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependency_upsert rejects dependency with non-existent child
    #[sqlx::test]
    async fn test_dependency_upsert_nonexistent_child(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let parent = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let fake_child = Uuid::new_v4();
        let param = DependencyParam {
            parent_id: parent.id,
            child_id: fake_child,
            extra: Some(json!({"test":"dependency"})),
        };

        let mut tx = pool.begin().await.unwrap();
        let result = dependency_upsert(&mut tx, dataset.id, &param, username, modified_date).await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependency_upsert rejects self-referencing dependency
    #[sqlx::test]
    async fn test_dependency_upsert_self_reference(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let dp = create_test_data_product(
            &pool,
            dataset.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let param = DependencyParam {
            parent_id: dp.id,
            child_id: dp.id,
            extra: Some(json!({"test":"self-reference"})),
        };

        let mut tx = pool.begin().await.unwrap();
        let result = dependency_upsert(&mut tx, dataset.id, &param, username, modified_date).await;
        tx.rollback().await.unwrap();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some(),
        ));
    }

    /// Test dependencies_by_dataset_select returns all dependencies for a dataset
    #[sqlx::test]
    async fn test_dependencies_by_dataset_select(pool: PgPool) {
        let username = "test";
        let modified_date = Utc::now();
        let dataset = create_test_dataset(&pool, None, None, None, Some(username), Some(modified_date)).await;
        let dp1 = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let dp2 = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let dp3 = create_test_data_product(
            &pool,
            dataset.id,
            Some(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(username),
            Some(modified_date),
        )
        .await;
        let dep1 = create_test_dependency(
            &pool,
            dataset.id,
            dp1.id,
            dp2.id,
            Some(json!({"dependency":"1-2"})),
            Some(username),
            Some(modified_date),
        )
        .await;
        let dep2 = create_test_dependency(
            &pool,
            dataset.id,
            dp2.id,
            dp3.id,
            Some(json!({"dependency":"2-3"})),
            Some(username),
            Some(modified_date),
        )
        .await;

        let mut tx = pool.begin().await.unwrap();
        let retrieved = dependencies_by_dataset_select(&mut tx, dataset.id).await.unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(retrieved.len(), 2);
        assert!(retrieved.contains(&dep1));
        assert!(retrieved.contains(&dep2));
    }
