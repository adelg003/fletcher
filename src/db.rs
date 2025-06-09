use crate::core::{Compute, DataProduct, Dataset, Dependency, PlanDag, State};
use chrono::Utc;
use poem_openapi::Object;
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};
use uuid::Uuid;

/// Input for a Plan Dag
#[derive(Object)]
pub struct PlanDagParam {
    dataset: DatasetParam,
    data_products: Vec<DataProductParam>,
    dependencies: Vec<DependencyParam>,
}

impl PlanDagParam {
    /// Write the Plan Dag to the DB
    pub async fn upsert(
        self,
        tx: &mut Transaction<'_, Postgres>,
        username: &str,
    ) -> Result<PlanDag, sqlx::Error> {
        let dataset_id: Uuid = self.dataset.id;

        // Write our data to the DB for a Dataset
        let dataset: Dataset = dataset_upsert(tx, self.dataset, username).await?;

        // Write our data to the DB for Data Products
        let mut data_products: Vec<DataProduct> = Vec::new();
        for param in self.data_products {
            let data_product: DataProduct =
                data_product_upsert(tx, dataset_id, param, username).await?;

            data_products.push(data_product);
        }

        // Write our data to the DB for Dependencies
        let mut dependencies: Vec<Dependency> = Vec::new();
        for param in self.dependencies {
            let dependency: Dependency = dependency_upsert(tx, dataset_id, param, username).await?;

            dependencies.push(dependency);
        }

        Ok(PlanDag {
            dataset,
            data_products,
            dependencies,
        })
    }
}

/// Input parameters for a Dataset
#[derive(Object)]
struct DatasetParam {
    id: Uuid,
    paused: bool,
    extra: Option<Value>,
}

/// Input parameters for a Data Product
#[derive(Object)]
struct DataProductParam {
    id: String,
    compute: Compute,
    name: String,
    version: String,
    eager: bool,
    passthrough: Option<Value>,
    extra: Option<Value>,
}

/// Input for adding a Dependency
#[derive(Object)]
struct DependencyParam {
    parent_id: String,
    child_id: String,
    extra: Option<Value>,
}

/// Insert up Update a Dataset
async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: DatasetParam,
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
            dataset_id AS id,
            paused,
            extra,
            modified_by,
            modified_date",
        param.id,
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
async fn data_product_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
    param: DataProductParam,
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
        None::<Uuid>,
        None::<String>,
        None::<Value>,
        param.extra,
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
async fn dependency_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
    param: DependencyParam,
    username: &str,
) -> Result<Dependency, sqlx::Error> {
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

/// Read a Plan Dag from the DB
pub async fn plan_dag_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
) -> Result<PlanDag, sqlx::Error> {
    // Pull data elements
    let dataset: Dataset = dataset_select(tx, dataset_id).await?;
    let data_products: Vec<DataProduct> = data_products_by_dataset_select(tx, dataset_id).await?;
    let dependencies: Vec<Dependency> = dependencies_by_dataset_select(tx, dataset_id).await?;

    Ok(PlanDag {
        dataset,
        data_products,
        dependencies,
    })
}
