use crate::{
    error::Result,
    model::{
        Compute, DataProduct, DataProductParam, Dataset, DatasetId, DatasetParam, Dependency,
        DependencyParam, Plan, PlanParam, State, StateParam,
    },
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Postgres, Transaction, query_as};

/// Inserts a new dataset or updates an existing one in the database.
///
/// If a dataset with the given ID already exists, its fields are updated; otherwise, a new record is created. Returns the resulting `Dataset` record.
///
/// # Examples
///
/// ```
/// let param = DatasetParam { id: dataset_id, paused: false, extra: None };
/// let dataset = dataset_upsert(&mut tx, param, "alice", &Utc::now()).await?;
/// assert_eq!(dataset.id, dataset_id);
/// ```
async fn dataset_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: DatasetParam,
    username: &str,
    modified_date: &DateTime<Utc>,
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

/// Retrieves a dataset by its identifier.
///
/// Returns the `Dataset` record matching the provided `dataset_id`, or an error if not found.
///
/// # Examples
///
/// ```
/// let dataset = dataset_select(&mut tx, dataset_id).await?;
/// assert_eq!(dataset.id, dataset_id);
/// ```
async fn dataset_select(
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

/// Inserts or updates a data product record for a given dataset.
///
/// If a data product with the specified `dataset_id` and `data_product_id` exists, its fields are updated; otherwise, a new record is inserted. The state is set to `Waiting` and run-related fields are reset on upsert.
///
/// # Returns
/// The inserted or updated `DataProduct`.
///
/// # Examples
///
/// ```
/// let param = DataProductParam { /* fields */ };
/// let data_product = data_product_upsert(&mut tx, dataset_id, param, "alice", &Utc::now()).await?;
/// assert_eq!(data_product.name, "example");
/// ```
async fn data_product_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    param: DataProductParam,
    username: &str,
    modified_date: &DateTime<Utc>,
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

/// Updates the state and related metadata of a specific data product within a dataset.
///
/// Modifies the state, run ID, link, passback, extra metadata, and audit fields for the given data product, and returns the updated `DataProduct`.
///
/// # Examples
///
/// ```
/// let updated = state_update(&mut tx, state_param, "alice", &Utc::now()).await?;
/// assert_eq!(updated.state, state_param.state);
/// ```
async fn state_update(
    tx: &mut Transaction<'_, Postgres>,
    param: StateParam,
    username: &str,
    modified_date: &DateTime<Utc>,
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
        param.dataset_id,
        param.data_product_id,
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

/// Retrieves all data products associated with a given dataset.
///
/// Returns a vector of `DataProduct` records for the specified dataset ID.
///
/// # Examples
///
/// ```
/// # use crate::db::data_products_by_dataset_select;
/// # use crate::types::{DatasetId, DataProduct};
/// # async fn example(mut tx: sqlx::Transaction<'_, sqlx::Postgres>, dataset_id: DatasetId) {
/// let products: Vec<DataProduct> = data_products_by_dataset_select(&mut tx, dataset_id).await.unwrap();
/// assert!(!products.is_empty());
/// # }
/// ```
async fn data_products_by_dataset_select(
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

/// Inserts or updates a dependency between two data products within a dataset.
///
/// If a dependency with the same dataset, parent, and child IDs exists, its metadata is updated; otherwise, a new dependency is created.
///
/// # Returns
/// The inserted or updated `Dependency`.
///
/// # Examples
///
/// ```
/// let dependency = dependency_upsert(
///     &mut tx,
///     dataset_id,
///     dependency_param,
///     "alice",
///     &Utc::now()
/// ).await?;
/// assert_eq!(dependency.parent_id, dependency_param.parent_id);
/// ```
async fn dependency_upsert(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    param: DependencyParam,
    username: &str,
    modified_date: &DateTime<Utc>,
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

/// Retrieves all dependency records associated with a given dataset.
///
/// Returns a vector of `Dependency` structs for the specified dataset ID.
///
/// # Examples
///
/// ```
/// let dependencies = dependencies_by_dataset_select(&mut tx, dataset_id).await?;
/// assert!(!dependencies.is_empty());
/// ```
async fn dependencies_by_dataset_select(
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

/// Inserts or updates a complete plan, including its dataset, data products, and dependencies, in the database within a transaction.
///
/// This function performs an upsert operation for the dataset, all associated data products, and their dependencies as described in the provided `PlanParam`. All modifications are tracked with the current timestamp and the specified username. Returns the resulting `Plan` reflecting the persisted state.
///
/// # Examples
///
/// ```
/// # use crate::db::{plan_dag_upsert, PlanParam};
/// # use sqlx::{Postgres, Transaction};
/// # async fn example(mut tx: Transaction<'_, Postgres>, param: PlanParam) {
/// let username = "alice";
/// let plan = plan_dag_upsert(&mut tx, param, username).await.unwrap();
/// assert_eq!(plan.dataset.modified_by, "alice");
/// # }
/// ```
pub async fn plan_dag_upsert(
    tx: &mut Transaction<'_, Postgres>,
    param: PlanParam,
    username: &str,
) -> Result<Plan> {
    let dataset_id: DatasetId = param.dataset.id;
    let modified_date: DateTime<Utc> = Utc::now();

    // Write our data to the DB for a Dataset
    let dataset: Dataset = dataset_upsert(tx, param.dataset, username, &modified_date).await?;

    // Write our data to the DB for Data Products
    let mut data_products: Vec<DataProduct> = Vec::new();
    for param in param.data_products {
        let data_product: DataProduct =
            data_product_upsert(tx, dataset_id, param, username, &modified_date).await?;

        data_products.push(data_product);
    }

    // Write our data to the DB for Dependencies
    let mut dependencies: Vec<Dependency> = Vec::new();
    for param in param.dependencies {
        let dependency: Dependency =
            dependency_upsert(tx, dataset_id, param, username, &modified_date).await?;

        dependencies.push(dependency);
    }

    Ok(Plan {
        dataset,
        data_products,
        dependencies,
    })
}

/// Retrieves a complete plan, including the dataset, its data products, and their dependencies, from the database by dataset ID.
///
/// # Examples
///
/// ```
/// # use crate::db::{plan_dag_select, Plan};
/// # use crate::types::DatasetId;
/// # async fn example(mut tx: sqlx::Transaction<'_, sqlx::Postgres>, dataset_id: DatasetId) -> crate::Result<Plan> {
/// let plan = plan_dag_select(&mut tx, dataset_id).await?;
/// assert_eq!(plan.dataset.dataset_id, dataset_id);
/// # Ok(plan)
/// # }
/// ```
pub async fn plan_dag_select(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
) -> Result<Plan> {
    // Pull data elements
    let dataset: Dataset = dataset_select(tx, dataset_id).await?;
    let data_products: Vec<DataProduct> = data_products_by_dataset_select(tx, dataset_id).await?;
    let dependencies: Vec<Dependency> = dependencies_by_dataset_select(tx, dataset_id).await?;

    Ok(Plan {
        dataset,
        data_products,
        dependencies,
    })
}
