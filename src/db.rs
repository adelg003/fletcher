use crate::core::{Compute, DataProduct, Dataset, Dependency, State};
use chrono::Utc;
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};
use uuid::Uuid;

/// Input parameters for a Dataset
struct DatasetDbParam {
    dataset_id: Uuid,
    paused: bool,
    extra: Value,
}

/// Input parameters for a Data Product
struct DataProductDbParam {
    dataset_id: Uuid,
    data_product_id: String,
    compute: Compute,
    name: String,
    version: String,
    eager: bool,
    passthrough: Value,
}

/// Input parameters for State
struct StateDbParam {
    dataset_id: Uuid,
    data_product_id: String,
    state: State,
    run_id: Option<Uuid>,
    link: Option<String>,
    passback: Value,
}

/// Input for adding a Dependency
struct DependencyDbParam {
    dataset_id: Uuid,
    parent_id: String,
    child_id: String,
}

/// Insert or Update a Dataset
async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: &DatasetDbParam,
    username: &str,
) -> Result<Dataset, sqlx::Error> {
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
            dataset_id,
            paused,
            extra,
            modified_by,
            modified_date",
        param.dataset_id,
        param.paused,
        param.extra,
        username,
        Utc::now(),
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dataset)
}

/// Pull a Dataset
async fn dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
) -> Result<Dataset, sqlx::Error> {
    // Upsert a dataset row
    let dataset = query_as!(
        Dataset,
        "SELECT
            dataset_id,
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
async fn data_product_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: &DataProductDbParam,
    username: &str,
) -> Result<DataProduct, sqlx::Error> {
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
            $13
        ) ON CONFLICT (dataset_id, data_product_id) DO
        UPDATE SET
            compute = $3,
            name = $4,
            version = $5,
            eager = $6,
            passthrough = $7,
            modified_by = $12,
            modified_date = $13
        RETURNING
            dataset_id,
            data_product_id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            modified_by,
            modified_date"#,
        param.dataset_id,
        param.data_product_id,
        param.compute as Compute,
        param.name,
        param.version,
        param.eager,
        param.passthrough,
        State::Waiting as State,
        None::<Uuid>,
        None::<String>,
        None::<Value>,
        username,
        Utc::now(),
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(data_product)
}

/// Update the State of a Data Product
async fn state_update(
    tx: &mut Transaction<'_, Postgres>,
    param: &StateDbParam,
    username: &str,
) -> Result<DataProduct, sqlx::Error> {
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
            dataset_id,
            data_product_id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
            modified_by,
            modified_date"#,
        param.dataset_id,
        param.data_product_id,
        param.state as State,
        param.run_id,
        param.link,
        param.passback,
        username,
        Utc::now(),
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(data_product)
}

/// Retrieve all Data Products for a Dataset
async fn data_products_by_dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
) -> Result<Vec<DataProduct>, sqlx::Error> {
    let data_products = query_as!(
        DataProduct,
        r#"SELECT
            dataset_id,
            data_product_id,
            compute AS "compute: Compute",
            name,
            version,
            eager,
            passthrough,
            state AS "state: State",
            run_id,
            link,
            passback,
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

/// Insert a new Dependency between Data Products
async fn dependency_insert(
    tx: &mut Transaction<'_, Postgres>,
    param: &DependencyDbParam,
    username: &str,
) -> Result<Dependency, sqlx::Error> {
    let dependency = query_as!(
        Dependency,
        "INSERT INTO dependency (
            dataset_id,
            parent_id,
            child_id,
            modified_by,
            modified_date
        ) VALUES (
            $1,
            $2,
            $3,
            $4,
            $5
        ) RETURNING
            dataset_id,
            parent_id,
            child_id,
            modified_by,
            modified_date",
        param.dataset_id,
        param.parent_id,
        param.child_id,
        username,
        Utc::now(),
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(dependency)
}

/// Retrieve all Dependencies for a Dataset
async fn dependencies_by_dataset_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
) -> Result<Vec<Dependency>, sqlx::Error> {
    let dependencies = query_as!(
        Dependency,
        "SELECT
            dataset_id,
            parent_id,
            child_id,
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
