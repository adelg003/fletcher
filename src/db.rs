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
    /// Inserts or updates a complete Plan Dag—including dataset, data products, and dependencies—within a database transaction.
    ///
    /// Sequentially upserts the dataset, all associated data products, and their dependencies into the database. Returns the fully constructed `PlanDag` on success.
    ///
    /// # Arguments
    ///
    /// * `username` - The user performing the operation, recorded for modification metadata.
    ///
    /// # Returns
    ///
    /// Returns a `PlanDag` containing the upserted dataset, data products, and dependencies.
    ///
    /// # Errors
    ///
    /// Returns a `sqlx::Error` if any database operation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// let plan_dag_param = PlanDagParam { /* ... */ };
    /// let mut tx = pool.begin().await?;
    /// let plan_dag = plan_dag_param.upsert(&mut tx, "alice").await?;
    /// assert_eq!(plan_dag.dataset.id, plan_dag_param.dataset.id);
    /// ```
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

/// Inserts or updates a dataset in the database by its ID.
///
/// If a dataset with the given ID already exists, its `paused`, `extra`, `modified_by`, and `modified_date` fields are updated; otherwise, a new dataset is inserted.
/// 
/// # Returns
/// The upserted `Dataset` record.
///
/// # Examples
///
/// ```
/// let param = DatasetParam { id, paused: false, extra: None };
/// let dataset = dataset_upsert(&mut tx, param, "alice").await?;
/// assert_eq!(dataset.id, id);
/// ```
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

/// Retrieves a dataset by its unique identifier.
///
/// Queries the database for a dataset with the specified ID and returns the corresponding `Dataset` if found.
///
/// # Examples
///
/// ```
/// let dataset = dataset_select(&mut tx, dataset_id).await?;
/// assert_eq!(dataset.id, dataset_id);
/// ```
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

/// Inserts or updates a data product associated with a dataset.
///
/// If a data product with the given dataset and data product IDs exists, its fields are updated; otherwise, a new data product is created. The state is set to `Waiting`, and certain fields are initialized to default values.
///
/// # Returns
/// The upserted `DataProduct`.
///
/// # Examples
///
/// ```
/// let param = DataProductParam { /* fields */ };
/// let data_product = data_product_upsert(&mut tx, dataset_id, param, "alice").await?;
/// assert_eq!(data_product.name, "example");
/// ```
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

/// Retrieves all data products associated with the specified dataset.
///
/// # Parameters
/// - `dataset_id`: The unique identifier of the dataset whose data products are to be fetched.
///
/// # Returns
/// A vector of `DataProduct` instances belonging to the given dataset.
///
/// # Examples
///
/// ```
/// let products = data_products_by_dataset_select(&mut tx, dataset_id).await?;
/// assert!(!products.is_empty());
/// ```
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

/// Inserts or updates a dependency between two data products within a dataset.
///
/// If a dependency with the same dataset ID, parent ID, and child ID exists, its metadata and extra fields are updated; otherwise, a new dependency is created.
///
/// # Returns
///
/// The upserted `Dependency` object.
///
/// # Examples
///
/// ```
/// let dependency = dependency_upsert(&mut tx, dataset_id, param, "alice").await?;
/// assert_eq!(dependency.parent_id, param.parent_id);
/// ```
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

/// Retrieves all dependencies associated with a given dataset.
///
/// # Parameters
/// - `dataset_id`: The unique identifier of the dataset whose dependencies are to be fetched.
///
/// # Returns
/// A vector of `Dependency` objects representing all dependencies for the specified dataset.
///
/// # Examples
///
/// ```
/// let dependencies = dependencies_by_dataset_select(&mut tx, dataset_id).await?;
/// assert!(!dependencies.is_empty());
/// ```
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

/// Retrieves a complete Plan Dag for the specified dataset from the database.
///
/// Fetches the dataset, all associated data products, and their dependencies, returning them as a `PlanDag`.
///
/// # Examples
///
/// ```
/// let mut tx = pool.begin().await?;
/// let dag = plan_dag_select(&mut tx, dataset_id).await?;
/// assert_eq!(dag.dataset.id, dataset_id);
/// ```
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
