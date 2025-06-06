use crate::core::{Compute, DataProduct, Dataset, State};
use chrono::Utc;
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};
use uuid::Uuid;

/// Input parameters for a Dataset
struct DatasetDbParam {
    dataset_id: Uuid,
    paused: bool,
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

/// Insert or Update a Dataset
async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: &DatasetDbParam,
    username: &str,
) -> Result<Dataset, sqlx::Error> {
    // Upsert a dataset
    let dataset = query_as!(
        Dataset,
        "INSERT INTO dataset (
            dataset_id,
            paused,
            modified_by,
            modified_date
        ) VALUES (
            $1,
            $2,
            $3,
            $4
        ) ON CONFLICT (dataset_id) DO
        UPDATE SET
            paused = $2,
            modified_by = $3,
            modified_date = $4
        RETURNING
            dataset_id,
            paused,
            modified_by,
            modified_date",
        param.dataset_id,
        param.paused,
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
    // Upsert a data product
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
