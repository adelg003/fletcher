use crate::{
    db::{
        data_product_upsert, data_products_by_dataset_select, dataset_select, dataset_upsert,
        dependencies_by_dataset_select, dependency_upsert, state_update,
    },
    error::Result,
};
use chrono::{DateTime, Utc};
use poem_openapi::{Enum, Object};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
use std::collections::HashSet;
use uuid::Uuid;

/// Type for Dataset ID
pub type DatasetId = Uuid;

/// Type for Data Product ID
pub type DataProductId = String;

/// Plan Details
#[derive(Object)]
pub struct Plan {
    pub dataset: Dataset,
    pub data_products: Vec<DataProduct>,
    pub dependencies: Vec<Dependency>,
}

impl Plan {
    /// Pull the Plan for a Dataset
    pub async fn from_dataset_id(
        tx: &mut Transaction<'_, Postgres>,
        id: &DatasetId,
    ) -> Result<Self> {
        // Pull data elements
        let dataset: Dataset = dataset_select(tx, id).await?;
        let data_products: Vec<DataProduct> = data_products_by_dataset_select(tx, id).await?;
        let dependencies: Vec<Dependency> = dependencies_by_dataset_select(tx, id).await?;

        Ok(Plan {
            dataset,
            data_products,
            dependencies,
        })
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|dp: &DataProduct| dp.id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn dependency_edges(&self) -> Vec<(DataProductId, DataProductId, u32)> {
        self.dependencies
            .iter()
            .map(|dep: &Dependency| (dep.parent_id.clone(), dep.child_id.clone(), 1))
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

impl DataProduct {
    /// Update the State of a DataProduct
    pub async fn state_update(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        plan: &Plan,
        state: &StateParam,
        username: &str,
        modified_date: &DateTime<Utc>,
    ) -> Result<DataProduct> {
        state_update(
            tx,
            &plan.dataset.id,
            &self.id,
            state,
            username,
            modified_date,
        )
        .await
    }
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

/// Input for a Plan
#[derive(Object)]
pub struct PlanParam {
    pub dataset: DatasetParam,
    pub data_products: Vec<DataProductParam>,
    pub dependencies: Vec<DependencyParam>,
}

impl PlanParam {
    /// Write the Plan to the DB
    pub async fn upsert(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        username: &str,
        modified_date: &DateTime<Utc>,
    ) -> Result<Plan> {
        let dataset_id: &DatasetId = &self.dataset.id;

        // Write our data to the DB for a Dataset
        let dataset: Dataset = dataset_upsert(tx, &self.dataset, username, modified_date).await?;

        // Write our data to the DB for Data Products
        let mut data_products: Vec<DataProduct> = Vec::new();
        for data_product_param in &self.data_products {
            let data_product: DataProduct =
                data_product_upsert(tx, dataset_id, data_product_param, username, modified_date)
                    .await?;

            data_products.push(data_product);
        }

        // Write our data to the DB for Dependencies
        let mut dependencies: Vec<Dependency> = Vec::new();
        for dependency_param in &self.dependencies {
            let dependency: Dependency =
                dependency_upsert(tx, dataset_id, dependency_param, username, modified_date)
                    .await?;

            dependencies.push(dependency);
        }

        Ok(Plan {
            dataset,
            data_products,
            dependencies,
        })
    }

    /// Check for duplicate Data Products
    pub fn has_dup_data_products(&self) -> Option<DataProductId> {
        let mut seen = HashSet::new();

        for data_product in &self.data_products {
            if !seen.insert(&data_product.id) {
                return Some(data_product.id.clone());
            }
        }

        None
    }

    /// Check for duplicate Dependencies
    pub fn has_dup_dependencies(&self) -> Option<(DataProductId, DataProductId)> {
        let mut seen = HashSet::new();

        for dependency in &self.dependencies {
            if !seen.insert((&dependency.parent_id, &dependency.child_id)) {
                return Some((dependency.parent_id.clone(), dependency.child_id.clone()));
            }
        }

        None
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|dp: &DataProductParam| dp.id.clone())
            .collect()
    }

    /// Return all Parent IDs
    pub fn parent_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| dep.parent_id.clone())
            .collect()
    }

    /// Return all Child IDs
    pub fn child_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| dep.child_id.clone())
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn dependency_edges(&self) -> Vec<(DataProductId, DataProductId, u32)> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| (dep.parent_id.clone(), dep.child_id.clone(), 1))
            .collect()
    }
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
