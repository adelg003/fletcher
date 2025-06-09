use crate::{
    api::{DataProductApiParam, DependencyApiParam, PlanDagApiParam},
    core::{Compute, DataProduct, Dataset, Dependency, PlanDag, State},
};
use chrono::Utc;
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};
use std::convert::From;
use uuid::Uuid;

/// Input for a Plan Dag
pub struct PlanDagDbParam {
    dataset: DatasetDbParam,
    data_products: Vec<DataProductDbParam>,
    dependencies: Vec<DependencyDbParam>,
}

impl PlanDagDbParam {
    /// Write the Plan Dag to the DB
    pub async fn upsert(
        self,
        tx: &mut Transaction<'_, Postgres>,
        username: &str,
    ) -> Result<PlanDag, sqlx::Error> {
        // Write our data to the DB for a Dataset
        let dataset: Dataset = dataset_upsert(tx, self.dataset, username).await?;

        // Write our data to the DB for Data Products
        let mut data_products: Vec<DataProduct> = Vec::new();
        for param in self.data_products {
            let data_product: DataProduct = data_product_upsert(tx, param, username).await?;

            data_products.push(data_product);
        }

        // Write our data to the DB for Dependencies
        let mut dependencies: Vec<Dependency> = Vec::new();
        for param in self.dependencies {
            let dependency: Dependency = dependency_upsert(tx, param, username).await?;

            dependencies.push(dependency);
        }

        Ok(PlanDag {
            dataset,
            data_products,
            dependencies,
        })
    }
}

impl From<PlanDagApiParam> for PlanDagDbParam {
    /// Convert a API Plan Dag Param to a DB Plan Dag Param
    fn from(dag: PlanDagApiParam) -> Self {
        // Convert Dataset params from API to DB format
        let dataset = DatasetDbParam {
            dataset_id: dag.dataset.dataset_id,
            paused: dag.dataset.paused,
            extra: dag.dataset.extra,
        };

        // Convert Data Product params from API to DB format
        let data_products: Vec<DataProductDbParam> = dag
            .data_products
            .into_iter()
            .map(|data_product: DataProductApiParam| DataProductDbParam {
                dataset_id: dag.dataset.dataset_id,
                data_product_id: data_product.data_product_id,
                compute: data_product.compute,
                name: data_product.name,
                version: data_product.version,
                eager: data_product.eager,
                passthrough: data_product.passthrough,
                extra: data_product.extra,
            })
            .collect();

        // Convert Dependency params from API to DB format
        let dependencies: Vec<DependencyDbParam> = dag
            .dependencies
            .into_iter()
            .map(|dependency: DependencyApiParam| DependencyDbParam {
                dataset_id: dag.dataset.dataset_id,
                parent_id: dependency.parent_id,
                child_id: dependency.child_id,
                extra: dependency.extra,
            })
            .collect();

        Self {
            dataset,
            data_products,
            dependencies,
        }
    }
}

/// Input parameters for a Dataset
struct DatasetDbParam {
    dataset_id: Uuid,
    paused: bool,
    extra: Option<Value>,
}

/// Input parameters for a Data Product
struct DataProductDbParam {
    dataset_id: Uuid,
    data_product_id: String,
    compute: Compute,
    name: String,
    version: String,
    eager: bool,
    passthrough: Option<Value>,
    extra: Option<Value>,
}

/// Input parameters for State
struct StateDbParam {
    dataset_id: Uuid,
    data_product_id: String,
    state: State,
    run_id: Option<Uuid>,
    link: Option<String>,
    passback: Option<Value>,
    extra: Option<Value>,
}

/// Input for adding a Dependency
struct DependencyDbParam {
    dataset_id: Uuid,
    parent_id: String,
    child_id: String,
    extra: Option<Value>,
}

/// Insert up Update a Dataset
async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: DatasetDbParam,
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
    param: DataProductDbParam,
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
            extra,
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
        param.extra,
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
    param: StateDbParam,
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
            extra = $7,
            modified_by = $8,
            modified_date = $9
        WHERE
            dataset_id = $1
            AND data_product_id = $2
        RETURNING
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
            extra,
            modified_by,
            modified_date"#,
        param.dataset_id,
        param.data_product_id,
        param.state as State,
        param.run_id,
        param.link,
        param.passback,
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
    param: DependencyDbParam,
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
        param.dataset_id,
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
