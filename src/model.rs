use crate::{
    db::{plan_dag_select, plan_dag_upsert},
    error::Result,
};
use chrono::{DateTime, Utc};
use poem_openapi::{Enum, Object};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
use std::{collections::HashSet, hash::Hash, marker::Copy};
use uuid::Uuid;

/// Type for Dataset ID
pub type DatasetId = Uuid;

/// Type for Data Product ID
pub type DataProductId = String;

/// Plan Dag Details
#[derive(Object)]
pub struct Plan {
    pub dataset: Dataset,
    pub data_products: Vec<DataProduct>,
    pub dependencies: Vec<Dependency>,
}

impl Plan {
    /// Retrieves the plan DAG for the specified dataset from the database.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the dataset.
    /// * `tx` - An active PostgreSQL transaction.
    ///
    /// # Returns
    ///
    /// Returns the `Plan` representing the dataset's plan DAG if found.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut tx = pool.begin().await?;
    /// let plan = Plan::from_dataset_id(dataset_id, &mut tx).await?;
    /// ```
    pub async fn from_dataset_id(
        id: DatasetId,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Self> {
        plan_dag_select(tx, id).await
    }

    /// Returns a vector of all data product IDs in the plan.
    ///
    /// # Examples
    ///
    /// ```
    /// let plan = Plan { /* fields omitted for brevity */ };
    /// let ids = plan.data_product_ids();
    /// assert!(ids.iter().all(|id| plan.data_products.iter().any(|dp| &dp.id == id)));
    /// ```
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|data_product: &DataProduct| data_product.id.clone())
            .collect()
    }

    /// Returns all dependency edges as tuples of parent and child data product IDs with a fixed weight.
    ///
    /// Each tuple represents a directed edge from a parent to a child data product in the plan's dependency graph. The third element is always `1`.
    ///
    /// # Returns
    /// A vector of tuples `(parent_id, child_id, 1)` for each dependency.
    ///
    /// # Examples
    ///
    /// ```
    /// let edges = plan.dependency_edges();
    /// for (parent, child, weight) in edges {
    ///     assert_eq!(weight, 1);
    /// }
    /// ```
    pub fn dependency_edges(&self) -> Vec<(DataProductId, DataProductId, u32)> {
        self.dependencies
            .iter()
            .map(|dependency: &Dependency| {
                (dependency.parent_id.clone(), dependency.child_id.clone(), 1)
            })
            .collect()
    }
}

/// Dataset details
#[derive(Object)]
pub struct Dataset {
    pub id: DatasetId,
    pub paused: bool,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// The Types of compute OaaS can call
#[derive(Clone, Copy, Enum, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "compute", rename_all = "lowercase")]
pub enum Compute {
    Cams,
    Dbxaas,
}

/// States a Data Product can be in
#[derive(Clone, Copy, Enum, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "state", rename_all = "lowercase")]
pub enum State {
    Waiting,
    Queued,
    Running,
    Success,
    Failed,
    Disabled,
}

/// Data Product details
#[derive(Object)]
pub struct DataProduct {
    pub id: DataProductId,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// Dependency from one Data Product to another Data Product
#[derive(Object)]
pub struct Dependency {
    pub parent_id: DataProductId,
    pub child_id: DataProductId,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// Input for a Plan Dag
#[derive(Object)]
pub struct PlanParam {
    pub dataset: DatasetParam,
    pub data_products: Vec<DataProductParam>,
    pub dependencies: Vec<DependencyParam>,
}

impl PlanParam {
    /// Inserts or updates the plan DAG in the database using the provided transaction and username.
    ///
    /// Returns the resulting `Plan` after the operation completes.
    ///
    /// # Examples
    ///
    /// ```
    /// let plan_param = PlanParam { /* fields */ };
    /// let plan = plan_param.upsert(&mut tx, "user1").await?;
    /// ```
    pub async fn upsert(self, tx: &mut Transaction<'_, Postgres>, username: &str) -> Result<Plan> {
        plan_dag_upsert(tx, self, username).await
    }

    /// Returns the first duplicate data product ID if any exist.
    ///
    /// Checks the list of data products for duplicate IDs and returns the first duplicate found, or `None` if all IDs are unique.
    pub fn has_dup_data_products(&self) -> Option<DataProductId> {
        // Pull the Data Product IDs
        let data_product_ids: Vec<DataProductId> = self
            .data_products
            .iter()
            .map(|param: &DataProductParam| param.id.clone())
            .collect();

        // Are there duplicate Data Product IDs?
        find_duplicate(&data_product_ids)
    }

    /// Returns the first duplicate dependency (parent-child pair) if any exist.
    ///
    /// Checks for duplicate dependencies in the plan by identifying repeated parent-child pairs among the dependencies.
    /// 
    /// # Returns
    /// 
    /// An `Option` containing the first duplicate `(parent_id, child_id)` pair if found, or `None` if all dependencies are unique.
    ///
    /// # Examples
    ///
    /// ```
    /// let param = PlanParam { /* ... */ };
    /// if let Some((parent, child)) = param.has_dup_dependencies() {
    ///     println!("Duplicate dependency: ({}, {})", parent, child);
    /// }
    /// ```
    pub fn has_dup_dependencies(&self) -> Option<(DataProductId, DataProductId)> {
        // Pull the Data Product IDs
        let parent_child_ids: Vec<(DataProductId, DataProductId)> = self
            .dependencies
            .iter()
            .map(|param: &DependencyParam| (param.parent_id.clone(), param.child_id.clone()))
            .collect();

        // Are there duplicate Data Product IDs?
        find_duplicate(&parent_child_ids)
    }

    /// Returns a vector of all data product IDs in the plan parameters.
    ///
    /// # Examples
    ///
    /// ```
    /// let params = PlanParam {
    ///     dataset: DatasetParam { id: Uuid::new_v4(), paused: false, extra: None },
    ///     data_products: vec![
    ///         DataProductParam { id: "dp1".to_string(), compute: Compute::Cams, name: "A".to_string(), version: "1.0".to_string(), eager: false, passthrough: None, extra: None },
    ///         DataProductParam { id: "dp2".to_string(), compute: Compute::Dbxaas, name: "B".to_string(), version: "1.0".to_string(), eager: false, passthrough: None, extra: None },
    ///     ],
    ///     dependencies: vec![],
    /// };
    /// let ids = params.data_product_ids();
    /// assert_eq!(ids, vec!["dp1".to_string(), "dp2".to_string()]);
    /// ```
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|data_product: &DataProductParam| data_product.id.clone())
            .collect()
    }

    /// Returns a vector of all parent data product IDs from the dependencies.
    ///
    /// # Examples
    ///
    /// ```
    /// let deps = vec![
    ///     DependencyParam { parent_id: "parent1".to_string(), child_id: "child1".to_string(), extra: None },
    ///     DependencyParam { parent_id: "parent2".to_string(), child_id: "child2".to_string(), extra: None },
    /// ];
    /// let plan_param = PlanParam {
    ///     dataset: DatasetParam { id: Uuid::new_v4(), paused: false, extra: None },
    ///     data_products: vec![],
    ///     dependencies: deps,
    /// };
    /// let parent_ids = plan_param.parent_ids();
    /// assert_eq!(parent_ids, vec!["parent1".to_string(), "parent2".to_string()]);
    /// ```
    pub fn parent_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.parent_id.clone())
            .collect()
    }

    /// Returns a vector of all child data product IDs from the plan's dependencies.
    ///
    /// # Examples
    ///
    /// ```
    /// let deps = vec![
    ///     DependencyParam { parent_id: "A".to_string(), child_id: "B".to_string(), extra: None },
    ///     DependencyParam { parent_id: "B".to_string(), child_id: "C".to_string(), extra: None },
    /// ];
    /// let plan_param = PlanParam {
    ///     dataset: DatasetParam { id: Uuid::new_v4(), paused: false, extra: None },
    ///     data_products: vec![],
    ///     dependencies: deps,
    /// };
    /// let child_ids = plan_param.child_ids();
    /// assert_eq!(child_ids, vec!["B".to_string(), "C".to_string()]);
    /// ```
    pub fn child_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.child_id.clone())
            .collect()
    }

    /// Returns all dependency edges as tuples of parent and child data product IDs with a fixed weight.
    ///
    /// Each tuple represents a directed edge from a parent to a child data product in the plan, with the third element always set to 1.
    ///
    /// # Returns
    /// A vector of tuples `(parent_id, child_id, 1)` for each dependency.
    ///
    /// # Examples
    ///
    /// ```
    /// let param = PlanParam {
    ///     dataset: DatasetParam { id: Uuid::new_v4(), paused: false, extra: None },
    ///     data_products: vec![],
    ///     dependencies: vec![
    ///         DependencyParam {
    ///             parent_id: "parent".to_string(),
    ///             child_id: "child".to_string(),
    ///             extra: None,
    ///         }
    ///     ],
    /// };
    /// let edges = param.dependency_edges();
    /// assert_eq!(edges, vec![("parent".to_string(), "child".to_string(), 1)]);
    /// ```
    pub fn dependency_edges(&self) -> Vec<(DataProductId, DataProductId, u32)> {
        self.dependencies
            .iter()
            .map(|dependency: &DependencyParam| {
                (dependency.parent_id.clone(), dependency.child_id.clone(), 1)
            })
            .collect()
    }
}

/// Returns the first duplicate element found in a slice, or `None` if all elements are unique.
///
/// # Examples
///
/// ```
/// let nums = vec![1, 2, 3, 2, 4];
/// assert_eq!(find_duplicate(&nums), Some(2));
///
/// let unique = vec!["a", "b", "c"];
/// assert_eq!(find_duplicate(&unique), None);
/// ```
fn find_duplicate<T>(vec: &[T]) -> Option<T>
where
    T: Eq + Hash + Clone,
{
    let mut seen = HashSet::new();
    for item in vec {
        if !seen.insert(item) {
            return Some(item.clone());
        }
    }
    None
}

/// Input parameters for a Dataset
#[derive(Object)]
pub struct DatasetParam {
    pub id: DatasetId,
    pub paused: bool,
    pub extra: Option<Value>,
}

/// Input parameters for a Data Product
#[derive(Object)]
pub struct DataProductParam {
    pub id: DataProductId,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub extra: Option<Value>,
}

/// Input parameters for State
pub struct StateParam {
    pub dataset_id: DatasetId,
    pub data_product_id: DataProductId,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
    pub extra: Option<Value>,
}

/// Input for adding a Dependency
#[derive(Object)]
pub struct DependencyParam {
    pub parent_id: DataProductId,
    pub child_id: DataProductId,
    pub extra: Option<Value>,
}
