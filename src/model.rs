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
    /// Pull the Plan Dag for a Dataset
    pub async fn from_dataset_id(
        id: DatasetId,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Self> {
        plan_dag_select(tx, id).await
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|data_product: &DataProduct| data_product.id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
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
    /// Write the Plan Dag to the DB
    pub async fn upsert(self, tx: &mut Transaction<'_, Postgres>, username: &str) -> Result<Plan> {
        plan_dag_upsert(tx, self, username).await
    }

    /// Check for duplicate Data Products
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

    /// Check for duplicate Dependencies
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

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|data_product: &DataProductParam| data_product.id.clone())
            .collect()
    }

    /// Return all Parent IDs
    pub fn parent_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.parent_id.clone())
            .collect()
    }

    /// Return all Child IDs
    pub fn child_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.child_id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn dependency_edges(&self) -> Vec<(DataProductId, DataProductId, u32)> {
        self.dependencies
            .iter()
            .map(|dependency: &DependencyParam| {
                (dependency.parent_id.clone(), dependency.child_id.clone(), 1)
            })
            .collect()
    }
}

/// Check if Vec has duplicates
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
