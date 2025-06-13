use crate::db::{plan_dag_select, plan_dag_upsert};
use chrono::{DateTime, Utc};
use poem_openapi::{Enum, Object};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
use std::{collections::HashSet, hash::Hash, marker::Copy};
use uuid::Uuid;

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
        id: Uuid,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<Self, sqlx::Error> {
        plan_dag_select(tx, id).await
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<String> {
        self.data_products
            .iter()
            .map(|data_product: &DataProduct| data_product.id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn dependency_edges(&self) -> Vec<(String, String, u32)> {
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
    pub id: Uuid,
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
    pub id: String,
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
    pub parent_id: String,
    pub child_id: String,
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
    pub async fn upsert(
        self,
        tx: &mut Transaction<'_, Postgres>,
        username: &str,
    ) -> Result<Plan, sqlx::Error> {
        plan_dag_upsert(tx, self, username).await
    }

    /// Check for duplicate Data Products
    pub fn has_dup_data_products(&self) -> bool {
        // Pull the Data Product IDs
        let data_product_ids: Vec<&String> = self
            .data_products
            .iter()
            .map(|param: &DataProductParam| &param.id)
            .collect();

        // Are there duplicate Data Product IDs?
        has_dups(&data_product_ids)
    }

    /// Check for duplicate Dependencies
    pub fn has_dup_dependencies(&self) -> bool {
        // Pull the Data Product IDs
        let parent_child_ids: Vec<(&String, &String)> = self
            .dependencies
            .iter()
            .map(|param: &DependencyParam| (&param.parent_id, &param.child_id))
            .collect();

        // Are there duplicate Data Product IDs?
        has_dups(&parent_child_ids)
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<String> {
        self.data_products
            .iter()
            .map(|data_product: &DataProductParam| data_product.id.clone())
            .collect()
    }

    /// Return all Parent IDs
    pub fn parent_ids(&self) -> Vec<String> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.parent_id.clone())
            .collect()
    }

    /// Return all Child IDs
    pub fn child_ids(&self) -> Vec<String> {
        self.dependencies
            .iter()
            .map(|dependencies: &DependencyParam| dependencies.child_id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn dependency_edges(&self) -> Vec<(String, String, u32)> {
        self.dependencies
            .iter()
            .map(|dependency: &DependencyParam| {
                (dependency.parent_id.clone(), dependency.child_id.clone(), 1)
            })
            .collect()
    }
}

/// Check if value is duplicated
fn has_dups<T: Eq + Hash>(vec: &[T]) -> bool {
    let set: HashSet<&T> = vec.iter().collect();
    set.len() < vec.len()
}

/// Input parameters for a Dataset
#[derive(Object)]
pub struct DatasetParam {
    pub id: Uuid,
    pub paused: bool,
    pub extra: Option<Value>,
}

/// Input parameters for a Data Product
#[derive(Object)]
pub struct DataProductParam {
    pub id: String,
    pub compute: Compute,
    pub name: String,
    pub version: String,
    pub eager: bool,
    pub passthrough: Option<Value>,
    pub extra: Option<Value>,
}

/// Input parameters for State
pub struct StateParam {
    pub dataset_id: Uuid,
    pub data_product_id: String,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
    pub extra: Option<Value>,
}

/// Input for adding a Dependency
#[derive(Object)]
pub struct DependencyParam {
    pub parent_id: String,
    pub child_id: String,
    pub extra: Option<Value>,
}
