use crate::{
    dag::Dag,
    db::{
        data_product_upsert, data_products_by_dataset_select, dataset_select, dataset_upsert,
        dependencies_by_dataset_select, dependency_upsert, state_update,
    },
    error::Result,
};
use chrono::{DateTime, Utc};
use petgraph::graph::DiGraph;
use poem_openapi::{Enum, Object};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Type};
use std::{collections::HashSet, fmt};
use uuid::Uuid;

/// Type for Dataset ID
pub type DatasetId = Uuid;

/// Type for Data Product ID
pub type DataProductId = Uuid;

/// Edge of the graph
pub type Edge = (DataProductId, DataProductId, u32);

/// Plan Details
#[derive(Clone, Debug, Object, PartialEq)]
pub struct Plan {
    pub dataset: Dataset,
    pub data_products: Vec<DataProduct>,
    pub dependencies: Vec<Dependency>,
}

impl Plan {
    /// Pull the Plan for a Dataset
    pub async fn from_dataset_id(
        tx: &mut Transaction<'_, Postgres>,
        id: DatasetId,
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

    /// Return all Data Product IDs (excludes Disabled)
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .filter_map(|dp: &DataProduct| match dp.state {
                State::Failed
                | State::Queued
                | State::Running
                | State::Success
                | State::Waiting => Some(dp.id),
                State::Disabled => None,
            })
            .collect()
    }

    /// Return all Parent / Child dependencies (excludes Disabled) // Return Option<Edge>
    pub fn edges(&self) -> Vec<Edge> {
        self.dependencies
            .iter()
            .filter_map(|dep: &Dependency| {
                let parent: &DataProduct = self.data_product(dep.parent_id)?;
                let child: &DataProduct = self.data_product(dep.child_id)?;

                // Only add edge if both parent and child are not disabled
                if parent.state != State::Disabled && child.state != State::Disabled {
                    Some((dep.parent_id, dep.child_id, 1)) // Return Option<Edge>
                } else {
                    None
                }
            })
            .collect()
    }

    /// Pull a single Data Product
    pub fn data_product(&self, id: DataProductId) -> Option<&DataProduct> {
        self.data_products
            .iter()
            .find(|dp: &&DataProduct| dp.id == id)
    }

    /// Pull a single Data Product in a mutable state
    pub fn data_product_mut(&mut self, id: DataProductId) -> Option<&mut DataProduct> {
        self.data_products
            .iter_mut()
            .find(|dp: &&mut DataProduct| dp.id == id)
    }

    // Generate Dag representation of the plan (Disabled nodes are not part of the dag.)
    pub fn to_dag(&self) -> Result<DiGraph<DataProductId, u32>> {
        // Pull Data Product details
        let data_product_ids: HashSet<DataProductId> =
            self.data_product_ids().into_iter().collect();

        // How do the data products relate?
        let edges: HashSet<Edge> = self.edges().into_iter().collect();

        // Build the dag
        DiGraph::<DataProductId, u32>::build_dag(data_product_ids, edges)
    }
}

/// Dataset details
#[derive(Clone, Debug, Object, PartialEq)]
pub struct Dataset {
    pub id: DatasetId,
    pub paused: bool,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// The Types of compute OaaS can call
#[derive(Clone, Copy, Debug, Enum, PartialEq, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "compute", rename_all = "lowercase")]
pub enum Compute {
    Cams,
    Dbxaas,
}

/// States a Data Product can be in
#[derive(Clone, Copy, Debug, Enum, PartialEq, Type)]
#[oai(rename_all = "lowercase")]
#[sqlx(type_name = "state", rename_all = "lowercase")]
pub enum State {
    Disabled,
    Failed,
    Queued,
    Running,
    Success,
    Waiting,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Disabled => write!(f, "disabled"),
            State::Failed => write!(f, "failed"),
            State::Queued => write!(f, "queued"),
            State::Running => write!(f, "running"),
            State::Success => write!(f, "success"),
            State::Waiting => write!(f, "waiting"),
        }
    }
}

/// Data Product details
#[derive(Clone, Debug, Object, PartialEq)]
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
    /// Update the State of a Data Product inplace
    pub async fn state_update(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        dataset_id: DatasetId,
        state: &StateParam,
        username: &str,
        modified_date: DateTime<Utc>,
    ) -> Result<()> {
        *self = state_update(tx, dataset_id, state, username, modified_date).await?;
        Ok(())
    }
}

/// Dependency from one Data Product to another Data Product
#[derive(Clone, Debug, Object, PartialEq)]
pub struct Dependency {
    pub parent_id: DataProductId,
    pub child_id: DataProductId,
    pub extra: Option<Value>,
    pub modified_by: String,
    pub modified_date: DateTime<Utc>,
}

/// Input for a Plan
#[derive(Clone, Object)]
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
        modified_date: DateTime<Utc>,
    ) -> Result<Plan> {
        let dataset_id: DatasetId = self.dataset.id;

        // Write our data to the DB for a Dataset
        let dataset: Dataset = dataset_upsert(tx, &self.dataset, username, modified_date).await?;

        // Write our data to the DB for Data Products
        for data_product_param in &self.data_products {
            data_product_upsert(tx, dataset_id, data_product_param, username, modified_date)
                .await?;
        }

        // Pull both new and old data products from the DB
        let data_products = data_products_by_dataset_select(tx, dataset_id).await?;

        // Write our data to the DB for Dependencies
        for dependency_param in &self.dependencies {
            dependency_upsert(tx, dataset_id, dependency_param, username, modified_date).await?;
        }

        // Pull both new and old dependencies from the DB
        let dependencies = dependencies_by_dataset_select(tx, dataset_id).await?;

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
                return Some(data_product.id);
            }
        }

        None
    }

    /// Check for duplicate Dependencies
    pub fn has_dup_dependencies(&self) -> Option<(DataProductId, DataProductId)> {
        let mut seen = HashSet::new();

        for dependency in &self.dependencies {
            if !seen.insert((dependency.parent_id, dependency.child_id)) {
                return Some((dependency.parent_id, dependency.child_id));
            }
        }

        None
    }

    /// Return all Data Product IDs
    pub fn data_product_ids(&self) -> Vec<DataProductId> {
        self.data_products
            .iter()
            .map(|dp: &DataProductParam| dp.id)
            .collect()
    }

    /// Return all Parent IDs
    pub fn parent_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| dep.parent_id)
            .collect()
    }

    /// Return all Child IDs
    pub fn child_ids(&self) -> Vec<DataProductId> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| dep.child_id)
            .collect()
    }

    /// Return all Parent / Child dependencies
    pub fn edges(&self) -> Vec<Edge> {
        self.dependencies
            .iter()
            .map(|dep: &DependencyParam| (dep.parent_id, dep.child_id, 1))
            .collect()
    }
}

/// Input parameters for a Dataset
#[derive(Clone, Object)]
pub struct DatasetParam {
    pub id: DatasetId,
    pub paused: bool,
    pub extra: Option<Value>,
}

/// Input parameters for a Data Product
#[derive(Clone, Object)]
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
#[derive(Clone, Object)]
pub struct StateParam {
    pub id: DataProductId,
    pub state: State,
    pub run_id: Option<Uuid>,
    pub link: Option<String>,
    pub passback: Option<Value>,
}

impl From<&mut DataProduct> for StateParam {
    fn from(data_product: &mut DataProduct) -> Self {
        StateParam {
            id: data_product.id,
            state: data_product.state,
            run_id: data_product.run_id,
            link: data_product.link.clone(),
            passback: data_product.passback.clone(),
        }
    }
}

/// Input for adding a Dependency
#[derive(Clone, Object)]
pub struct DependencyParam {
    pub parent_id: DataProductId,
    pub child_id: DataProductId,
    pub extra: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::tests::trim_to_microseconds, error::Error};
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use sqlx::PgPool;

    // Default implementations following db.rs patterns
    impl Default for PlanParam {
        fn default() -> Self {
            let dp1_param = DataProductParam::default();
            let dp2_param = DataProductParam {
                id: Uuid::new_v4(),
                ..Default::default()
            };

            let dependency_param = DependencyParam {
                parent_id: dp1_param.id,
                child_id: dp2_param.id,
                ..Default::default()
            };

            PlanParam {
                dataset: DatasetParam::default(),
                data_products: vec![dp1_param, dp2_param],
                dependencies: vec![dependency_param],
            }
        }
    }

    impl Default for DatasetParam {
        fn default() -> Self {
            Self {
                id: Uuid::new_v4(),
                paused: false,
                extra: Some(json!({"test":"dataset"})),
            }
        }
    }

    impl Default for DataProductParam {
        fn default() -> Self {
            Self {
                id: Uuid::new_v4(),
                compute: Compute::Cams,
                name: "test-data-product".to_string(),
                version: "1.0.0".to_string(),
                eager: true,
                passthrough: Some(json!({"test":"passthrough"})),
                extra: Some(json!({"test":"extra"})),
            }
        }
    }

    impl Default for StateParam {
        fn default() -> Self {
            Self {
                id: Uuid::new_v4(),
                state: State::Running,
                run_id: Some(Uuid::new_v4()),
                link: Some("https://example.com/run".to_string()),
                passback: Some(json!({"status":"running"})),
            }
        }
    }

    impl Default for DependencyParam {
        fn default() -> Self {
            Self {
                parent_id: Uuid::new_v4(),
                child_id: Uuid::new_v4(),
                extra: Some(json!({"test":"dependency"})),
            }
        }
    }

    // Helper function to create a test plan
    fn create_test_plan() -> Plan {
        let now = Utc::now();
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        Plan {
            dataset: Dataset {
                id: dataset_id,
                paused: false,
                extra: Some(json!({"test":"dataset"})),
                modified_by: "test_user".to_string(),
                modified_date: now,
            },
            data_products: vec![
                DataProduct {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "product_1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: false,
                    passthrough: Some(json!({"test":"passthrough1"})),
                    state: State::Queued,
                    run_id: Some(Uuid::new_v4()),
                    link: Some("http://test1.com".to_string()),
                    passback: Some(json!({"test":"passback1"})),
                    extra: Some(json!({"test":"extra1"})),
                    modified_by: "test_user".to_string(),
                    modified_date: now,
                },
                DataProduct {
                    id: dp2_id,
                    compute: Compute::Dbxaas,
                    name: "product_2".to_string(),
                    version: "2.0.0".to_string(),
                    eager: true,
                    passthrough: Some(json!({"test":"passthrough2"})),
                    state: State::Success,
                    run_id: Some(Uuid::new_v4()),
                    link: Some("http://test2.com".to_string()),
                    passback: Some(json!({"test":"passback2"})),
                    extra: Some(json!({"test":"extra2"})),
                    modified_by: "test_user".to_string(),
                    modified_date: now,
                },
            ],
            dependencies: vec![Dependency {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: Some(json!({"test":"dependency"})),
                modified_by: "test_user".to_string(),
                modified_date: now,
            }],
        }
    }

    // ------------ DB-layer tests (#[sqlx::test]) ------------

    /// Test Plan::from_dataset_id - Can we pull an existing plan if we have its dataset id?
    #[sqlx::test]
    async fn test_plan_from_dataset_id_success(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        // Insert test data
        let inserted_plan = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        // Test: Can we pull an existing plan if we have its dataset id?
        let retrieved_plan = Plan::from_dataset_id(&mut tx, inserted_plan.dataset.id)
            .await
            .unwrap();

        assert_eq!(retrieved_plan.dataset, inserted_plan.dataset);
        assert_eq!(retrieved_plan.dependencies, inserted_plan.dependencies);

        assert_eq!(retrieved_plan.data_products.len(), 2);
        assert!(
            retrieved_plan
                .data_products
                .contains(&inserted_plan.data_products[0])
        );
        assert!(
            retrieved_plan
                .data_products
                .contains(&inserted_plan.data_products[1])
        );
    }

    /// Test Plan::from_dataset_id - Do we get rejected if the dataset does not exist?
    #[sqlx::test]
    async fn test_plan_from_dataset_id_not_found(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Test: Do we get rejected if the dataset does not exist?
        let non_existent_id = Uuid::new_v4();
        let result = Plan::from_dataset_id(&mut tx, non_existent_id).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::Sqlx(sqlx::Error::RowNotFound),
        ));
    }

    /// Test DataProduct::state_update - Do we see an update to the Data Product in memory?
    #[sqlx::test]
    async fn test_data_product_state_update_memory(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        let plan = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();
        let mut data_product = plan.data_products.into_iter().next().unwrap();

        // Test: If we update a data product's state, do we see an update in memory?
        let old_state = data_product.state;
        let new_state = State::Running;
        let state_param = StateParam {
            id: data_product.id,
            state: new_state,
            run_id: Some(Uuid::new_v4()),
            link: Some("http://running.com".to_string()),
            passback: Some(json!({"status":"running"})),
        };

        data_product
            .state_update(
                &mut tx,
                plan.dataset.id,
                &state_param,
                username,
                modified_date,
            )
            .await
            .unwrap();

        assert_ne!(data_product.state, old_state);
        assert_eq!(data_product.state, new_state);
        assert_eq!(data_product.link, state_param.link);
        assert_eq!(data_product.passback, state_param.passback);
    }

    /// Test DataProduct::state_update - Do we see an update to the Data Product in the db?
    #[sqlx::test]
    async fn test_data_product_state_update_database(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Create test data
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        let plan = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();
        let mut data_product = plan.data_products.into_iter().next().unwrap();

        // Update state
        let new_state = State::Failed;
        let state_param = StateParam {
            id: data_product.id,
            state: new_state,
            run_id: Some(Uuid::new_v4()),
            link: Some("http://failed.com".to_string()),
            passback: Some(json!({"error":"test failure"})),
        };

        data_product
            .state_update(
                &mut tx,
                plan.dataset.id,
                &state_param,
                username,
                modified_date,
            )
            .await
            .unwrap();

        // Test: If we update a data product's state, do we see an update in the DB?
        let retrieved_plan = Plan::from_dataset_id(&mut tx, plan.dataset.id)
            .await
            .unwrap();

        assert!(retrieved_plan.data_products.contains(&data_product));
    }

    /// Test PlanParam::upsert - Can we insert a new dataset?
    #[sqlx::test]
    async fn test_plan_param_upsert_insert_dataset(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        // Test: Can we insert a new dataset?
        let result = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        assert_eq!(result.dataset.id, plan_param.dataset.id);
        assert_eq!(result.dataset.paused, plan_param.dataset.paused);
        assert_eq!(result.dataset.extra, plan_param.dataset.extra);
        assert_eq!(result.dataset.modified_by, username);

        assert_eq!(
            result.dataset,
            Dataset {
                id: plan_param.dataset.id,
                paused: plan_param.dataset.paused,
                extra: plan_param.dataset.extra,
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test PlanParam::upsert - Can we update an existing dataset?
    #[sqlx::test]
    async fn test_plan_param_upsert_update_dataset(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Insert initial dataset
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        // Test: Can we update an existing dataset?
        let updated_dataset_param = DatasetParam {
            id: plan_param.dataset.id,
            paused: true,
            extra: Some(json!({"updated":true})),
        };

        let updated_plan_param = PlanParam {
            dataset: updated_dataset_param,
            data_products: vec![],
            dependencies: vec![],
        };

        let result = updated_plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        assert_eq!(
            result.dataset,
            Dataset {
                id: updated_plan_param.dataset.id,
                paused: true,
                extra: Some(json!({"updated":true})),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }
        );
    }

    /// Test PlanParam::upsert - Can we insert a new data product?
    #[sqlx::test]
    async fn test_plan_param_upsert_insert_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        // Test: Can we insert a new data product?
        let result = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        let data_product_param = &plan_param.data_products[0];

        assert_eq!(result.data_products.len(), 2);
        assert!(result.data_products.contains(&DataProduct {
            id: data_product_param.id,
            compute: data_product_param.compute,
            name: data_product_param.name.clone(),
            version: data_product_param.version.clone(),
            eager: data_product_param.eager,
            passthrough: data_product_param.passthrough.clone(),
            state: State::Waiting,
            run_id: None,
            link: None,
            passback: None,
            extra: data_product_param.extra.clone(),
            modified_by: username.to_string(),
            modified_date: trim_to_microseconds(modified_date),
        }));
    }

    /// Test PlanParam::upsert - Can we update an existing data product?
    #[sqlx::test]
    async fn test_plan_param_upsert_update_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Insert initial data product
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        // Test: Can we update an existing data product?
        let updated_data_product_param = DataProductParam {
            id: plan_param.data_products[0].id,
            name: "updated_product".to_string(),
            version: "2.0.0".to_string(),
            eager: false,
            compute: Compute::Dbxaas,
            passthrough: Some(json!({"updated":"passthrough"})),
            extra: Some(json!({"updated":"extra"})),
        };

        let updated_plan_param = PlanParam {
            dataset: plan_param.dataset,
            data_products: vec![updated_data_product_param],
            dependencies: vec![],
        };

        let result = updated_plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        assert_eq!(result.data_products.len(), 2);
        assert!(result.data_products.contains(&DataProduct {
            id: plan_param.data_products[0].id,
            name: "updated_product".to_string(),
            version: "2.0.0".to_string(),
            eager: false,
            compute: Compute::Dbxaas,
            passthrough: Some(json!({"updated":"passthrough"})),
            extra: Some(json!({"updated":"extra"})),
            state: State::Waiting,
            run_id: None,
            link: None,
            passback: None,
            modified_by: username.to_string(),
            modified_date: trim_to_microseconds(modified_date),
        }));
    }

    /// Test PlanParam::upsert - Can we insert a new dependency?
    #[sqlx::test]
    async fn test_plan_param_upsert_insert_dependency(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        // Test: Can we insert a new dependency?
        let result = plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        assert_eq!(result.dependencies.len(), 1);
        assert_eq!(
            result.dependencies[0].parent_id,
            plan_param.dependencies[0].parent_id
        );
        assert_eq!(
            result.dependencies[0].child_id,
            plan_param.dependencies[0].child_id
        );
        assert_eq!(
            result.dependencies[0].extra,
            plan_param.dependencies[0].extra
        );

        assert_eq!(
            result.dependencies,
            vec![Dependency {
                parent_id: plan_param.dependencies[0].parent_id,
                child_id: plan_param.dependencies[0].child_id,
                extra: plan_param.dependencies[0].extra.clone(),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }]
        );
    }

    /// Test PlanParam::upsert - Can we update an existing dependency?
    #[sqlx::test]
    async fn test_plan_param_upsert_update_dependency(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Insert initial dependency
        let plan_param = PlanParam::default();
        let username = "test_user";
        let modified_date = Utc::now();

        plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        // Test: Can we update an existing dependency?
        let updated_dependency_param = DependencyParam {
            parent_id: plan_param.dependencies[0].parent_id,
            child_id: plan_param.dependencies[0].child_id,
            extra: Some(json!({"updated":"value"})),
        };

        let updated_plan_param = PlanParam {
            dataset: plan_param.dataset,
            data_products: plan_param.data_products,
            dependencies: vec![updated_dependency_param],
        };

        let result = updated_plan_param
            .upsert(&mut tx, username, modified_date)
            .await
            .unwrap();

        assert_eq!(result.dependencies.len(), 1);
        assert_eq!(
            result.dependencies[0].extra,
            Some(json!({"updated":"value"}))
        );
        assert_eq!(
            result.dependencies,
            vec![Dependency {
                parent_id: updated_plan_param.dependencies[0].parent_id,
                child_id: updated_plan_param.dependencies[0].child_id,
                extra: updated_plan_param.dependencies[0].extra.clone(),
                modified_by: username.to_string(),
                modified_date: trim_to_microseconds(modified_date),
            }]
        );
    }

    // ------------ Pure unit tests (#[test]) ------------

    /// Test Plan::data_product_ids - Do we get all the data product ids for all data products in a plan?
    #[test]
    fn test_plan_data_product_ids() {
        let plan = create_test_plan();
        let ids = plan.data_product_ids();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&plan.data_products[0].id));
        assert!(ids.contains(&plan.data_products[1].id));
    }

    /// Test Plan::edges - Do we get all the parent / child data product ids for all dependencies in a plan?
    #[test]
    fn test_plan_edges() {
        let plan = create_test_plan();
        let edges = plan.edges();

        assert_eq!(edges.len(), 1);
        assert_eq!(
            edges[0],
            (
                plan.dependencies[0].parent_id,
                plan.dependencies[0].child_id,
                1
            )
        );
    }

    /// Test Plan::data_product - Do we get a data product if we feed in its id?
    #[test]
    fn test_plan_data_product() {
        let plan = create_test_plan();
        let target_id = plan.data_products[0].id;

        // Test successful lookup
        let found_dp = plan.data_product(target_id);
        assert!(found_dp.is_some());
        assert_eq!(*found_dp.unwrap(), plan.data_products[0]);

        // Test with non-existent id
        let non_existent_id = Uuid::new_v4();
        let not_found_dp = plan.data_product(non_existent_id);
        assert!(not_found_dp.is_none());
    }

    /// Test Plan::data_product_mut - Do we get a data product in a mutable state if we feed in its id?
    #[test]
    fn test_plan_data_product_mut() {
        let mut plan = create_test_plan();
        let target_id = plan.data_products[0].id;

        // Test successful lookup
        let found_dp = plan.data_product_mut(target_id);
        assert!(found_dp.is_some());
        assert_eq!(found_dp.unwrap().id, target_id);

        // Test mutation
        let dp_mut = plan.data_product_mut(target_id).unwrap();
        dp_mut.name = "modified_name".to_string();
        dp_mut.state = State::Failed;

        assert_eq!(plan.data_products[0].name, "modified_name");
        assert_eq!(plan.data_products[0].state, State::Failed);

        // Test with non-existent id
        let non_existent_id = Uuid::new_v4();
        let not_found_dp = plan.data_product_mut(non_existent_id);
        assert!(not_found_dp.is_none());
    }

    /// Test Plan::to_dag - Can we convert a plan to a dag?
    #[test]
    fn test_plan_to_dag() {
        let plan = create_test_plan();
        let dag_result = plan.to_dag();

        assert!(dag_result.is_ok());
        let dag = dag_result.unwrap();

        // Check that all data products are in the DAG
        assert_eq!(dag.node_count(), 2);
        assert_eq!(dag.edge_count(), 1);
    }

    /// Test PlanParam::has_dup_data_products - If there are no duplicate data products, do we get None?
    #[test]
    fn test_plan_param_has_dup_data_products_none() {
        let plan_param = PlanParam::default();

        let result = plan_param.has_dup_data_products();
        assert!(result.is_none());
    }

    /// Test PlanParam::has_dup_data_products - If there is a duplicate data product, do we get Some()?
    #[test]
    fn test_plan_param_has_dup_data_products_some() {
        let dp1 = DataProductParam::default();
        let dp2 = DataProductParam {
            id: dp1.id,
            ..Default::default()
        };

        let plan_param = PlanParam {
            dataset: DatasetParam::default(),
            data_products: vec![dp1, dp2],
            dependencies: vec![],
        };

        let result = plan_param.has_dup_data_products();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), plan_param.data_products[0].id);
    }

    /// Test PlanParam::has_dup_dependencies - If there are no duplicate dependencies, do we get None?
    #[test]
    fn test_plan_param_has_dup_dependencies_none() {
        let plan_param = PlanParam::default();

        let result = plan_param.has_dup_dependencies();
        assert!(result.is_none());
    }

    /// Test PlanParam::has_dup_dependencies - If there is a duplicate dependency, do we get Some()?
    #[test]
    fn test_plan_param_has_dup_dependencies_some() {
        let dep1 = DependencyParam::default();
        let dep2 = DependencyParam {
            parent_id: dep1.parent_id,
            child_id: dep1.child_id,
            ..Default::default()
        };

        let plan_param = PlanParam {
            dataset: DatasetParam::default(),
            data_products: vec![],
            dependencies: vec![dep1, dep2],
        };

        let result = plan_param.has_dup_dependencies();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            (
                plan_param.dependencies[0].parent_id,
                plan_param.dependencies[0].child_id
            )
        );
    }

    /// Test PlanParam::data_product_ids - Do we get all the data product ids for all data products in a plan?
    #[test]
    fn test_plan_param_data_product_ids() {
        let plan_param = PlanParam::default();

        let ids = plan_param.data_product_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&plan_param.data_products[0].id));
        assert!(ids.contains(&plan_param.data_products[1].id));
    }

    /// Test PlanParam::edges - Do we get all the parent / child data product ids for all dependencies in a plan?
    #[test]
    fn test_plan_param_edges() {
        let plan_param = PlanParam::default();

        let edges = plan_param.edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(
            edges[0],
            (
                plan_param.dependencies[0].parent_id,
                plan_param.dependencies[0].child_id,
                1
            )
        );
    }

    /// Test StateParam::from - Can we convert a DataProduct to a StateParam?
    #[test]
    fn test_state_param_from_data_product() {
        let mut data_product = DataProduct {
            id: Uuid::new_v4(),
            compute: Compute::Cams,
            name: "test_product".to_string(),
            version: "1.0.0".to_string(),
            eager: false,
            passthrough: Some(json!({"test":"passthrough"})),
            state: State::Running,
            run_id: Some(Uuid::new_v4()),
            link: Some("http://test.com".to_string()),
            passback: Some(json!({"test":"passback"})),
            extra: Some(json!({"test":"extra"})),
            modified_by: "test_user".to_string(),
            modified_date: Utc::now(),
        };

        let state_param = StateParam::from(&mut data_product);

        assert_eq!(state_param.id, data_product.id);
        assert_eq!(state_param.state, data_product.state);
        assert_eq!(state_param.run_id, data_product.run_id);
        assert_eq!(state_param.link, data_product.link);
        assert_eq!(state_param.passback, data_product.passback);
    }
}
