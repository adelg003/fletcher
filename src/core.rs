use crate::{
    dag::Dag,
    db::search_plans_select,
    error::Error,
    model::{
        DataProduct, DataProductId, DatasetId, Edge, Plan, PlanParam, Search, State, StateParam,
    },
};
use chrono::{DateTime, Utc};
use petgraph::graph::DiGraph;
use poem::error::{BadRequest, Forbidden, InternalServerError, NotFound, UnprocessableEntity};
use sqlx::{Postgres, Transaction};
use std::collections::HashSet;
use tracing::warn;

/// How much do we want to paginate by
const PAGE_SIZE: u32 = 50;

/// Map Crate Error to Poem Error
fn to_poem_error(err: Error) -> poem::Error {
    match err {
        // Graph is cyclical
        Error::Cyclical => UnprocessableEntity(err),
        // Failed to build graph
        Error::Graph(error) => InternalServerError(error),
        // Already desired pause state
        Error::Pause(_, _) => BadRequest(err),
        // Row not found
        Error::Sqlx(sqlx::Error::RowNotFound) => NotFound(sqlx::Error::RowNotFound),
        // We hit a db constraint
        Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some() => BadRequest(err),
        // Unknown error
        error => InternalServerError(error),
    }
}

/// Validate a PlanParam from the user
fn validate_plan_param(param: &PlanParam, plan: &Option<Plan>) -> poem::Result<()> {
    // Does Plan have any dup data products?
    if let Some(data_product_id) = param.has_dup_data_products() {
        return Err(BadRequest(Error::Duplicate(data_product_id)));
    }

    // Does Plan have any dup dependencies?
    if let Some((parent_id, child_id)) = param.has_dup_dependencies() {
        return Err(BadRequest(Error::DuplicateDependencies(
            parent_id, child_id,
        )));
    }

    // Get a list of all Data Product IDs
    let data_product_ids: HashSet<DataProductId> = {
        let mut param_ids: Vec<DataProductId> = param.data_product_ids();

        // If we got a plan from the DB, add its data products
        if let Some(plan) = plan {
            let plan_ids: Vec<DataProductId> = plan.data_product_ids();
            param_ids.extend(plan_ids);
        }

        param_ids.into_iter().collect()
    };

    // Do all edge parents have a data product?
    param
        .parent_ids()
        .into_iter()
        .try_for_each(|parent_id: DataProductId| {
            if data_product_ids.contains(&parent_id) {
                Ok(())
            } else {
                Err(NotFound(Error::Missing(parent_id)))
            }
        })?;

    // Do all edge children have a data product?
    param
        .child_ids()
        .into_iter()
        .try_for_each(|child_id: DataProductId| {
            if data_product_ids.contains(&child_id) {
                Ok(())
            } else {
                Err(NotFound(Error::Missing(child_id)))
            }
        })?;

    // Collect all parent / child relationships
    let mut edges: HashSet<Edge> = param.edges().into_iter().collect();

    // If we got a plan from the DB, add its edges
    if let Some(plan) = plan {
        let plan_edges: Vec<Edge> = plan.edges();
        edges.extend(plan_edges);
    }

    // If you take all our data products and map them to a graph are they a valid dag?
    DiGraph::<DataProductId, u32>::build_dag(data_product_ids, edges).map_err(to_poem_error)?;

    Ok(())
}

/// Add a Plan Dag to the DB
pub async fn plan_add(
    tx: &mut Transaction<'_, Postgres>,
    param: &PlanParam,
    username: &str,
) -> poem::Result<Plan> {
    // Pull any prior details
    let wip_plan = Plan::from_db(tx, param.dataset.id).await;

    // So what did we get from the DB?
    let current_plan: Option<Plan> = match wip_plan {
        Ok(plan) => Some(plan),
        Err(Error::Sqlx(sqlx::Error::RowNotFound)) => None,
        Err(err) => return Err(InternalServerError(err)),
    };

    // Validate to make sure the user submitted valid parameters
    validate_plan_param(param, &current_plan)?;

    let modified_date: DateTime<Utc> = Utc::now();

    // Write our Plan to the DB
    let mut plan: Plan = param
        .upsert(tx, username, modified_date)
        .await
        .map_err(to_poem_error)?;

    // Triger the next batch of data products
    trigger_next_batch(tx, &mut plan, username, modified_date).await?;

    Ok(plan)
}

/// Read a Plan Dag from the DB
pub async fn plan_read(tx: &mut Transaction<'_, Postgres>, id: DatasetId) -> poem::Result<Plan> {
    Plan::from_db(tx, id).await.map_err(to_poem_error)
}

/// Set the pause state of a plan
pub async fn plan_pause_edit(
    tx: &mut Transaction<'_, Postgres>,
    id: DatasetId,
    paused: bool,
    username: &str,
) -> poem::Result<Plan> {
    // Pull the current plan from the DB
    let mut plan = Plan::from_db(tx, id).await.map_err(to_poem_error)?;

    // Timestamp of the transaction
    let modified_date: DateTime<Utc> = Utc::now();

    // Set the new pause state
    plan.paused(tx, paused, username, modified_date)
        .await
        .map_err(to_poem_error)?;

    // If unpaused, lets run the next batch
    if !paused {
        trigger_next_batch(tx, &mut plan, username, modified_date).await?;
    }

    Ok(plan)
}

/// Read a Data Product from the DB
pub async fn data_product_read(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    data_product_id: DataProductId,
) -> poem::Result<DataProduct> {
    DataProduct::from_db(tx, dataset_id, data_product_id)
        .await
        .map_err(to_poem_error)
}

/// Update the state of our data product
async fn state_update(
    tx: &mut Transaction<'_, Postgres>,
    plan: &mut Plan,
    state: &StateParam,
    username: &str,
    modified_date: DateTime<Utc>,
) -> poem::Result<()> {
    // Pull before we mutably borrow via plan.data_product()
    let dataset_id: DatasetId = plan.dataset.id;

    // Pull our Data Product
    let data_product: &mut DataProduct = plan
        .data_product_mut(state.id)
        .ok_or(NotFound(Error::Missing(state.id)))?;

    // Is the data product locked?
    if data_product.state == State::Disabled {
        return Err(Forbidden(Error::Disabled(data_product.id)));
    }

    // Update our Data Product State
    data_product
        .state_update(tx, dataset_id, state, username, modified_date)
        .await
        .map_err(to_poem_error)?;

    Ok(())
}

/// Clear all downstream data products
async fn clear_downstream_nodes(
    tx: &mut Transaction<'_, Postgres>,
    plan: &mut Plan,
    nodes: &[DataProductId],
    username: &str,
    modified_date: DateTime<Utc>,
) -> poem::Result<()> {
    // Pull before we mutably borrow via plan.data_product()
    let dataset_id: DatasetId = plan.dataset.id;

    // Generate Dag representation of the plan
    let dag: DiGraph<DataProductId, u32> = plan.to_dag().map_err(to_poem_error)?;

    // What are all the downstream nodes of our updated states?
    let mut downstream_ids = HashSet::<DataProductId>::new();
    for node in nodes {
        let new_ids: HashSet<DataProductId> = dag.downstream_nodes(*node);
        downstream_ids.extend(new_ids);
    }

    // Remove all nodes that are already in a Waiting or Disabled state
    downstream_ids.retain(|id: &DataProductId| {
        !matches!(
            plan.data_product(*id),
            Some(dp) if dp.state == State::Waiting,
        )
    });

    // Get ready to update the state of the data products
    for id in downstream_ids {
        let data_product: &mut DataProduct = plan
            .data_product_mut(id)
            .ok_or(InternalServerError(Error::Missing(id)))?;

        // Params for the current and new state. Make our change and dump the current state to fill the rest.
        let current_state: StateParam = data_product.into();
        let new_state = StateParam {
            state: State::Waiting,
            ..current_state
        };

        // Set non-waiting downstream_nodes to waiting
        data_product
            .state_update(tx, dataset_id, &new_state, username, modified_date)
            .await
            .map_err(to_poem_error)?;
    }

    Ok(())
}

/// Triger the next batch of data products
async fn trigger_next_batch(
    tx: &mut Transaction<'_, Postgres>,
    plan: &mut Plan,
    username: &str,
    modified_date: DateTime<Utc>,
) -> poem::Result<()> {
    // Is the dataset paused? If so, no need to trigger the next data product.
    if plan.dataset.paused {
        return Ok(());
    }

    // Pull before we mutably borrow via plan.data_product()
    let dataset_id: DatasetId = plan.dataset.id;

    // Find all nodes that can be ready to run
    let waiting_ids: HashSet<DataProductId> = plan
        .data_products
        .iter()
        .filter_map(|dp: &DataProduct| {
            if dp.state == State::Waiting && dp.eager {
                Some(dp.id)
            } else {
                None
            }
        })
        .collect();

    // Generate Dag representation of the plan (Disabled nodes are not part of the dag.)
    let dag: DiGraph<DataProductId, u32> = plan.to_dag().map_err(to_poem_error)?;

    // Check each data product's parents to see if any of them are blocking
    for waiting_id in waiting_ids {
        let parent_ids: HashSet<DataProductId> = dag.parent_nodes(waiting_id).into_iter().collect();

        // Check if parents are good to go. (all() returns true if the parent_ids is empty)
        let ready: bool = parent_ids.into_iter().all(|parent_id: DataProductId| {
            matches!(
                plan.data_product(parent_id),
                Some(dp) if dp.state == State::Success,
            )
        });

        // If the waiting node is ready to run, trigger it
        if ready {
            warn!(
                "This is where I would trigger the data product with the OaaS Wrapper... IF I HAD ONE!!!"
            );

            // Representation of the current data product
            let data_product: &mut DataProduct = plan
                .data_product_mut(waiting_id)
                .ok_or(InternalServerError(Error::Missing(waiting_id)))?;

            // Params for the current and new state. Make our change and dump the current state to fill the rest.
            let new_state = StateParam {
                state: State::Queued,
                ..data_product.into()
            };

            // Now record what we triggered the data product in the OaaS Wrapper
            data_product
                .state_update(tx, dataset_id, &new_state, username, modified_date)
                .await
                .map_err(to_poem_error)?;
        }
    }

    Ok(())
}

/// Edit the state of a Data Product
pub async fn states_edit(
    tx: &mut Transaction<'_, Postgres>,
    id: DatasetId,
    states: &[StateParam],
    username: &str,
) -> poem::Result<Plan> {
    // Timestamp of the transaction
    let modified_date: DateTime<Utc> = Utc::now();

    // Check the new state to update to, and make sure is valid.
    for state in states {
        if matches!(
            state.state,
            State::Disabled | State::Queued | State::Waiting
        ) {
            return Err(BadRequest(Error::BadState(state.id, state.state)));
        }
    }

    // Pull the Plan so we know what we are working with
    let mut plan = Plan::from_db(tx, id).await.map_err(to_poem_error)?;

    // Apply our updates to the data products
    for state in states {
        state_update(tx, &mut plan, state, username, modified_date).await?;
    }

    // Clear all nodes that are downstream of the ones we just updated.
    let nodes: Vec<DataProductId> = states.iter().map(|state: &StateParam| state.id).collect();
    clear_downstream_nodes(tx, &mut plan, &nodes, username, modified_date).await?;

    // Trigger the next batch of data products
    trigger_next_batch(tx, &mut plan, username, modified_date).await?;

    Ok(plan)
}

/// Clear Data Products so then can be rerun
pub async fn clear_edit(
    tx: &mut Transaction<'_, Postgres>,
    id: DatasetId,
    data_product_ids: &[DataProductId],
    username: &str,
) -> poem::Result<Plan> {
    // Timestamp of the transaction
    let modified_date: DateTime<Utc> = Utc::now();

    // Pull the Plan so we know what we are working with
    let mut plan = Plan::from_db(tx, id).await.map_err(to_poem_error)?;

    // Clear the data products
    for id in data_product_ids {
        // Pull Data Product of interest
        let data_product: &DataProduct = plan
            .data_product(*id)
            .ok_or(NotFound(Error::Missing(*id)))?;

        // Build new cleared state
        let state = StateParam {
            state: State::Waiting,
            ..data_product.into()
        };

        // Update the state to waiting
        state_update(tx, &mut plan, &state, username, modified_date).await?;
    }

    // Clear all nodes that are downstream of the ones we just updated.
    clear_downstream_nodes(tx, &mut plan, data_product_ids, username, modified_date).await?;

    // Trigger the next batch of data products
    trigger_next_batch(tx, &mut plan, username, modified_date).await?;

    Ok(plan)
}

/// Mark a Data Product as Disabled
pub async fn disable_drop(
    tx: &mut Transaction<'_, Postgres>,
    id: DatasetId,
    data_product_ids: &[DataProductId],
    username: &str,
) -> poem::Result<Plan> {
    // Timestamp of the transaction
    let modified_date: DateTime<Utc> = Utc::now();

    // Pull the Plan so we know what we are working with
    let mut plan = Plan::from_db(tx, id).await.map_err(to_poem_error)?;

    for id in data_product_ids {
        // Pull the Data Product we want
        let data_product: &DataProduct = plan
            .data_product(*id)
            .ok_or(NotFound(Error::Missing(*id)))?;

        // Get the new state params for the selected data product by using the old state params.
        let state = StateParam {
            state: State::Disabled,
            ..data_product.into()
        };

        // Apply our updates to the data products
        state_update(tx, &mut plan, &state, username, modified_date).await?;
    }

    // Triger the next batch of data products
    trigger_next_batch(tx, &mut plan, username, modified_date).await?;

    Ok(plan)
}

/// Search for a Plan
pub async fn plan_search_read(
    tx: &mut Transaction<'_, Postgres>,
    search_by: &str,
    page: u32,
) -> poem::Result<Vec<Search>> {
    // Compute offset
    let offset: u32 = page.saturating_mul(PAGE_SIZE);

    // Pull the Systems
    search_plans_select(tx, search_by, PAGE_SIZE, offset)
        .await
        .map_err(to_poem_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Compute, DataProduct, DataProductParam, Dataset, DatasetParam, Dependency, DependencyParam,
        Plan, PlanParam, State, StateParam,
    };
    use chrono::Utc;
    use poem::http::StatusCode;
    use serde_json::json;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Helper function to create test data
    fn create_test_plan_param() -> PlanParam {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: Some(json!({"test": "data"})),
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "test-product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: Some(json!({"pass": "through"})),
                    extra: Some(json!({"extra": "data"})),
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Dbxaas,
                    name: "test-product-2".to_string(),
                    version: "2.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "test-product-3".to_string(),
                    version: "1.5.0".to_string(),
                    eager: true,
                    passthrough: Some(json!({"test": true})),
                    extra: Some(json!({"type": "test"})),
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: Some(json!({"dep": "1->2"})),
                },
                DependencyParam {
                    parent_id: dp2_id,
                    child_id: dp3_id,
                    extra: Some(json!({"dep": "2->3"})),
                },
            ],
        }
    }

    // Tests for validate_plan_param function

    /// Test validate_plan_param accepts valid plan with no existing plan
    #[test]
    fn test_validate_plan_param_valid_new_plan() {
        let param = create_test_plan_param();
        let result = validate_plan_param(&param, &None);
        assert!(result.is_ok());
    }

    /// Test validate_plan_param rejects duplicate data product ids
    #[test]
    fn test_validate_plan_param_rejects_duplicate_data_products() {
        let duplicate_id = Uuid::new_v4();
        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: duplicate_id,
                    compute: Compute::Cams,
                    name: "product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: duplicate_id, // Duplicate ID
                    compute: Compute::Cams,
                    name: "product-2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            format!("{err}"),
            format!("Duplicate data-product id in parameter: {duplicate_id}"),
        );
    }

    /// Test validate_plan_param rejects duplicate dependencies in payload
    #[test]
    fn test_validate_plan_param_rejects_duplicate_dependencies() {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "product-2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: Some(json!({"first": "dependency"})),
                },
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id, // Duplicate dependency
                    extra: Some(json!({"second": "dependency"})),
                },
            ],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            format!("{err}"),
            format!("Duplicate dependency in parameter: {dp1_id} -> {dp2_id}")
        );
    }

    /// Test validate_plan_param rejects self-referencing dependencies
    #[test]
    fn test_validate_plan_param_rejects_self_reference() {
        let dp_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![DataProductParam {
                id: dp_id,
                compute: Compute::Cams,
                name: "product-1".to_string(),
                version: "1.0.0".to_string(),
                eager: true,
                passthrough: None,
                extra: None,
            }],
            dependencies: vec![DependencyParam {
                parent_id: dp_id,
                child_id: dp_id, // Self-reference
                extra: None,
            }],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(format!("{err}"), "Graph is cyclical");
    }

    /// Test validate_plan_param rejects parent dependencies with no data product
    #[test]
    fn test_validate_plan_param_rejects_missing_parent() {
        let dp1_id = Uuid::new_v4();
        let missing_parent_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![DataProductParam {
                id: dp1_id,
                compute: Compute::Cams,
                name: "product-1".to_string(),
                version: "1.0.0".to_string(),
                eager: true,
                passthrough: None,
                extra: None,
            }],
            dependencies: vec![DependencyParam {
                parent_id: missing_parent_id, // Parent doesn't exist
                child_id: dp1_id,
                extra: None,
            }],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            format!("Data product not found for: {missing_parent_id}")
        );
    }

    /// Test validate_plan_param rejects child dependencies with no data product
    #[test]
    fn test_validate_plan_param_rejects_missing_child() {
        let dp1_id = Uuid::new_v4();
        let missing_child_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![DataProductParam {
                id: dp1_id,
                compute: Compute::Cams,
                name: "product-1".to_string(),
                version: "1.0.0".to_string(),
                eager: true,
                passthrough: None,
                extra: None,
            }],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: missing_child_id, // Child doesn't exist
                extra: None,
            }],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            format!("Data product not found for: {missing_child_id}")
        );
    }

    /// Test validate_plan_param rejects cyclical dependencies in new plan
    #[test]
    fn test_validate_plan_param_rejects_cyclical_new_plan() {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "cycle-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "cycle-2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "cycle-3".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp2_id,
                    child_id: dp3_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp3_id,
                    child_id: dp1_id, // Creates cycle
                    extra: None,
                },
            ],
        };

        let result = validate_plan_param(&param, &None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(format!("{err}"), "Graph is cyclical");
    }

    /// Test validate_plan_param rejects cyclical dependencies when combined with existing plan
    #[test]
    fn test_validate_plan_param_rejects_cyclical_combined_plan() {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let existing_plan = Plan {
            dataset: Dataset {
                id: Uuid::new_v4(),
                paused: false,
                extra: None,
                modified_by: "test".to_string(),
                modified_date: Utc::now(),
            },
            data_products: vec![
                DataProduct {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "data product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting,
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                DataProduct {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "data product-2".to_string(),
                    version: "2.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting,
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
            ],
            dependencies: vec![Dependency {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: None,
                modified_by: "test".to_string(),
                modified_date: Utc::now(),
            }],
        };

        let param = PlanParam {
            dataset: DatasetParam {
                id: existing_plan.dataset.id,
                extra: None,
            },
            data_products: vec![],
            dependencies: vec![DependencyParam {
                parent_id: dp2_id,
                child_id: dp1_id, // Creates cycle with existing
                extra: None,
            }],
        };

        let result = validate_plan_param(&param, &Some(existing_plan));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(format!("{err}"), "Graph is cyclical");
    }

    // Tests for validate_plan_param with disabled nodes
    #[test]
    fn test_validate_plan_param_cyclical_dag_with_disabled_node() {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        // Create an existing plan with a disabled node that would create a cycle if enabled
        let existing_plan = Plan {
            dataset: Dataset {
                id: Uuid::new_v4(),
                paused: false,
                extra: None,
                modified_by: "test".to_string(),
                modified_date: Utc::now(),
            },
            data_products: vec![
                DataProduct {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "data-product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting,
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                DataProduct {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "data-product-2".to_string(),
                    version: "2.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting,
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                DataProduct {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "data-product-3".to_string(),
                    version: "3.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Disabled, // This node is disabled
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
            ],
            dependencies: vec![
                Dependency {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                Dependency {
                    parent_id: dp2_id,
                    child_id: dp3_id, // dp2 -> dp3
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                Dependency {
                    parent_id: dp3_id,
                    child_id: dp1_id, // dp3 -> dp1 (would create cycle)
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
            ],
        };

        // Create a new plan param that adds no new dependencies
        let param = PlanParam {
            dataset: DatasetParam {
                id: existing_plan.dataset.id,
                extra: None,
            },
            data_products: vec![],
            dependencies: vec![],
        };

        // This should NOT be considered cyclical because dp3 is disabled
        // and disabled nodes are excluded from the DAG validation
        let result = validate_plan_param(&param, &Some(existing_plan));
        assert!(
            result.is_ok(),
            "DAG should not be cyclical when the cycle includes a disabled node"
        );
    }

    /// Test validate_plan_param - Cyclical DAG without Disabled Node Should Be Cyclical
    #[test]
    fn test_validate_plan_param_cyclical_dag_without_disabled_node() {
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        // Create an existing plan with all nodes enabled (would create a cycle)
        let existing_plan = Plan {
            dataset: Dataset {
                id: Uuid::new_v4(),
                paused: false,
                extra: None,
                modified_by: "test".to_string(),
                modified_date: Utc::now(),
            },
            data_products: vec![
                DataProduct {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "data-product-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting, // All nodes are enabled
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                DataProduct {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "data-product-2".to_string(),
                    version: "2.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting, // All nodes are enabled
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                DataProduct {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "data-product-3".to_string(),
                    version: "3.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    state: State::Waiting, // This node is NOT disabled
                    run_id: None,
                    link: None,
                    passback: None,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
            ],
            dependencies: vec![
                Dependency {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                Dependency {
                    parent_id: dp2_id,
                    child_id: dp3_id, // dp2 -> dp3
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
                Dependency {
                    parent_id: dp3_id,
                    child_id: dp1_id, // dp3 -> dp1 (creates cycle)
                    extra: None,
                    modified_by: "test".to_string(),
                    modified_date: Utc::now(),
                },
            ],
        };

        // Create a new plan param that adds no new dependencies
        let param = PlanParam {
            dataset: DatasetParam {
                id: existing_plan.dataset.id,
                extra: None,
            },
            data_products: vec![],
            dependencies: vec![],
        };

        // This SHOULD be considered cyclical because no nodes are disabled
        let result = validate_plan_param(&param, &Some(existing_plan));
        assert!(
            result.is_err(),
            "DAG should be cyclical when no nodes are disabled"
        );
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(format!("{err}"), "Graph is cyclical");
    }

    // Tests for plan_add function

    /// Test plan_add can write a new plan to the database
    #[sqlx::test]
    async fn test_plan_add_new_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let result = plan_add(&mut tx, &param, username).await;
        assert!(result.is_ok());

        let plan = result.unwrap();
        assert_eq!(
            plan.dataset.id, param.dataset.id,
            "Plan dataset ID should match the parameter"
        );
        assert_eq!(
            plan.data_products.len(),
            3,
            "Plan should have 3 data products"
        );
        assert_eq!(
            plan.dependencies.len(),
            2,
            "Plan should have 2 dependencies"
        );
        assert_eq!(
            plan.dataset.modified_by, username,
            "Plan should be modified by the test user"
        );
    }

    /// Test plan_add can add new data products to existing plan
    #[sqlx::test]
    async fn test_plan_add_new_data_products_to_existing(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create initial plan
        let initial_param = create_test_plan_param();
        plan_add(&mut tx, &initial_param, username).await.unwrap();

        // Add new data product to existing plan
        let new_dp_id = Uuid::new_v4();
        let update_param = PlanParam {
            dataset: initial_param.dataset.clone(),
            data_products: vec![DataProductParam {
                id: new_dp_id,
                compute: Compute::Dbxaas,
                name: "new-product".to_string(),
                version: "3.0.0".to_string(),
                eager: true,
                passthrough: Some(json!({"new": "product"})),
                extra: Some(json!({"added": "later"})),
            }],
            dependencies: vec![],
        };

        let result = plan_add(&mut tx, &update_param, username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        assert_eq!(
            updated_plan.data_products.len(),
            4,
            "Updated plan should have 4 data products after adding new one"
        );
        let new_dp = updated_plan.data_product(new_dp_id);
        assert!(
            new_dp.is_some(),
            "Updated plan should contain the new data product"
        );
        assert_eq!(
            new_dp.unwrap().name,
            "new-product",
            "New data product should have correct name"
        );
    }

    /// Test plan_add can add new dependencies to existing plan
    #[sqlx::test]
    async fn test_plan_add_new_dependencies_to_existing(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let initial_param = create_test_plan_param();
        plan_add(&mut tx, &initial_param, username).await.unwrap();

        let dp1_id = initial_param.data_products[0].id;
        let dp3_id = initial_param.data_products[2].id;

        let update_param = PlanParam {
            dataset: initial_param.dataset.clone(),
            data_products: vec![],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp3_id,
                extra: Some(json!({"shortcut": "dependency"})),
            }],
        };

        let result = plan_add(&mut tx, &update_param, username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        assert_eq!(updated_plan.dependencies.len(), 3);
        let new_dep = updated_plan
            .dependencies
            .iter()
            .find(|dep| dep.parent_id == dp1_id && dep.child_id == dp3_id);
        assert!(
            new_dep.is_some(),
            "Updated plan should contain the new dependency"
        );
    }

    /// Test plan_add can update existing data products
    #[sqlx::test]
    async fn test_plan_add_update_existing_data_products(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let initial_param = create_test_plan_param();
        plan_add(&mut tx, &initial_param, username).await.unwrap();

        let dp1_id = initial_param.data_products[0].id;
        let update_param = PlanParam {
            dataset: initial_param.dataset.clone(),
            data_products: vec![DataProductParam {
                id: dp1_id,
                compute: Compute::Dbxaas,
                name: "updated-product-name".to_string(),
                version: "2.5.0".to_string(),
                eager: false,
                passthrough: Some(json!({"updated": "passthrough"})),
                extra: Some(json!({"updated": "extra"})),
            }],
            dependencies: vec![],
        };

        let result = plan_add(&mut tx, &update_param, username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        let updated_dp = updated_plan.data_product(dp1_id).unwrap();
        assert_eq!(
            updated_dp.name, "updated-product-name",
            "Data product name should be updated"
        );
        assert_eq!(
            updated_dp.compute,
            Compute::Dbxaas,
            "Data product compute should be updated to Dbxaas"
        );
        assert_eq!(
            updated_dp.version, "2.5.0",
            "Data product version should be updated"
        );
        assert!(
            !updated_dp.eager,
            "Data product eager should be updated to false"
        );
    }

    /// Test plan_add can update existing dependencies
    #[sqlx::test]
    async fn test_plan_add_update_existing_dependencies(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let initial_param = create_test_plan_param();
        plan_add(&mut tx, &initial_param, username).await.unwrap();

        let dp1_id = initial_param.data_products[0].id;
        let dp2_id = initial_param.data_products[1].id;

        let update_param = PlanParam {
            dataset: initial_param.dataset.clone(),
            data_products: vec![],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: Some(json!({"updated": "dependency_extra"})),
            }],
        };

        let result = plan_add(&mut tx, &update_param, username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        let updated_dep = updated_plan
            .dependencies
            .iter()
            .find(|dep| dep.parent_id == dp1_id && dep.child_id == dp2_id)
            .unwrap();
        assert_eq!(
            updated_dep.extra,
            Some(json!({"updated": "dependency_extra"}))
        );
    }

    // Tests for plan_read function

    /// Test plan_read can return a plan from the database
    #[sqlx::test]
    async fn test_plan_read_existing_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let added_plan = plan_add(&mut tx, &param, username).await.unwrap();
        let result = plan_read(&mut tx, added_plan.dataset.id).await;
        assert!(result.is_ok());

        let read_plan = result.unwrap();
        assert_eq!(
            read_plan.dataset.id, added_plan.dataset.id,
            "Read plan dataset ID should match added plan"
        );
        assert_eq!(
            read_plan.data_products.len(),
            added_plan.data_products.len(),
            "Read plan should have same number of data products as added plan"
        );
        assert_eq!(
            read_plan.dependencies.len(),
            added_plan.dependencies.len(),
            "Read plan should have same number of dependencies as added plan"
        );
    }

    /// Test plan_read returns error for non-existent plan
    #[sqlx::test]
    async fn test_plan_read_nonexistent_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let nonexistent_id = Uuid::new_v4();

        let result = plan_read(&mut tx, nonexistent_id).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            "no rows returned by a query that expected to return at least one row"
        );
    }

    // Tests for state_update function

    /// Test state_update
    #[sqlx::test]
    async fn test_state_update_accepts_valid_states(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let modified_date = Utc::now();

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dp_id = plan.data_products[0].id;

        let state_param = StateParam {
            id: dp_id,
            state: State::Running,
            run_id: Some(Uuid::new_v4()),
            link: Some(format!("http://{:?}.com", State::Running).to_string()),
            passback: Some(json!({"status": format!("{:?}", State::Running)})),
        };
        let result = state_update(&mut tx, &mut plan, &state_param, username, modified_date).await;
        assert!(result.is_ok());
    }

    /// Test state_update rejects updates to non-existent data products
    #[sqlx::test]
    async fn test_state_update_rejects_nonexistent_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let modified_date = Utc::now();

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        let nonexistent_id = Uuid::new_v4();
        let state_param = StateParam {
            id: nonexistent_id,
            state: State::Failed,
            run_id: None,
            link: None,
            passback: None,
        };

        let result = state_update(&mut tx, &mut plan, &state_param, username, modified_date).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            format!("Data product not found for: {nonexistent_id}")
        );
    }

    /// Test state_update rejects updates to disabled data products
    #[sqlx::test]
    async fn test_state_update_rejects_disabled_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let modified_date = Utc::now();

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dp_id = plan.data_products[0].id;
        plan.data_products[0].state = State::Disabled;

        let state_param = StateParam {
            id: dp_id,
            state: State::Running,
            run_id: Some(Uuid::new_v4()),
            link: None,
            passback: None,
        };

        let result = state_update(&mut tx, &mut plan, &state_param, username, modified_date).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(format!("{err}"), format!("Data product is locked: {dp_id}"));
    }

    // Tests for clear_downstream_nodes function

    /// Test clear_downstream_nodes sets downstream nodes to Waiting state
    #[sqlx::test]
    async fn test_clear_downstream_nodes_sets_waiting_state(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = param.data_products[0].id;
        let dp2_id = param.data_products[1].id;
        let dp3_id = param.data_products[2].id;

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // Update the state of our nodes
        let states = vec![
            StateParam {
                id: dp1_id,
                state: State::Failed,
                ..Default::default()
            },
            StateParam {
                id: dp2_id,
                state: State::Success,
                ..Default::default()
            },
            StateParam {
                id: dp3_id,
                state: State::Running,
                ..Default::default()
            },
        ];

        for state in states {
            state_update(&mut tx, &mut plan, &state, username, modified_date)
                .await
                .unwrap();
        }

        // Clear and check resuts.
        let result =
            clear_downstream_nodes(&mut tx, &mut plan, &[dp1_id], username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(plan.data_product(dp2_id).unwrap().state, State::Waiting);
        assert_eq!(plan.data_product(dp3_id).unwrap().state, State::Waiting);
    }

    /// Test clear_downstream_nodes skips nodes already in Waiting or Disabled state
    #[sqlx::test]
    async fn test_clear_downstream_nodes_skips_waiting_disabled(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let modified_date = Utc::now();

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        plan.data_products[1].state = State::Waiting;
        plan.data_products[2].state = State::Disabled;

        let original_dp1_modified = plan.data_products[1].modified_date;
        let original_dp2_modified = plan.data_products[2].modified_date;

        let states = vec![StateParam {
            id: plan.data_products[0].id,
            state: State::Failed,
            run_id: None,
            link: None,
            passback: None,
        }];
        let nodes: Vec<DataProductId> = states.into_iter().map(|state| state.id).collect();

        let result =
            clear_downstream_nodes(&mut tx, &mut plan, &nodes, username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(
            plan.data_products[1].state,
            State::Waiting,
            "Data product 1 should be set to Waiting state"
        );
        assert_eq!(
            plan.data_products[2].state,
            State::Disabled,
            "Data product 2 should remain Disabled"
        );
        assert_eq!(
            plan.data_products[1].modified_date, original_dp1_modified,
            "Data product 1 modified date should remain unchanged"
        );
        assert_eq!(
            plan.data_products[2].modified_date, original_dp2_modified,
            "Data product 2 modified date should remain unchanged"
        );
    }

    // Tests for trigger_next_batch function

    /// Test trigger_next_batch updates data products to Queued when conditions are met
    #[sqlx::test]
    async fn test_trigger_next_batch_updates_to_queued(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: None,
            }],
        };

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        plan.data_products[0].state = State::Success;
        plan.data_products[1].state = State::Waiting;

        let result = trigger_next_batch(&mut tx, &mut plan, username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(
            plan.data_products[1].state,
            State::Queued,
            "Data product should be set to Queued state after trigger"
        );
    }

    /// Test trigger_next_batch skips when dataset is paused
    #[sqlx::test]
    async fn test_trigger_next_batch_skips_when_paused(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: None,
            }],
        };

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        plan.data_products[0].state = State::Success;
        plan.data_products[1].state = State::Waiting;

        plan.paused(&mut tx, true, username, modified_date)
            .await
            .unwrap();

        let result = trigger_next_batch(&mut tx, &mut plan, username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(
            plan.data_products[1].state,
            State::Waiting,
            "Data product should remain in Waiting state when parent has multiple dependencies"
        );
    }

    /// Test trigger_next_batch skips non-eager data products
    #[sqlx::test]
    async fn test_trigger_next_batch_skips_non_eager(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: false,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: None,
            }],
        };

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // Update the state of our nodes
        state_update(
            &mut tx,
            &mut plan,
            &StateParam {
                id: dp1_id,
                state: State::Success,
                ..Default::default()
            },
            username,
            modified_date,
        )
        .await
        .unwrap();

        let result = trigger_next_batch(&mut tx, &mut plan, username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(plan.data_product(dp2_id).unwrap().state, State::Waiting);
    }

    /// Test trigger_next_batch with multiple successful parents
    #[sqlx::test]
    async fn test_trigger_next_batch_multiple_successful_parents(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "parent2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp3_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp2_id,
                    child_id: dp3_id,
                    extra: None,
                },
            ],
        };

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        plan.data_products[0].state = State::Success;
        plan.data_products[1].state = State::Success;
        plan.data_products[2].state = State::Waiting;

        let result = trigger_next_batch(&mut tx, &mut plan, username, modified_date).await;
        assert!(result.is_ok());
        assert_eq!(
            plan.data_products[2].state,
            State::Queued,
            "Data product should be set to Queued when all parent dependencies are successful"
        );
    }

    // Integration tests for states_edit function

    /// Test states_edit integrates all functionality correctly
    #[sqlx::test]
    async fn test_states_edit_integration(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let param = create_test_plan_param();
        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;

        let states = vec![StateParam {
            id: param.data_products[0].id,
            state: State::Success,
            run_id: Some(Uuid::new_v4()),
            link: Some("http://success.com".to_string()),
            passback: Some(json!({"completed": true})),
        }];

        let result = states_edit(&mut tx, dataset_id, &states, username).await;
        assert!(result.is_ok());
        let updated_plan = result.unwrap();

        assert_eq!(
            updated_plan
                .data_product(param.data_products[0].id)
                .unwrap()
                .state,
            State::Success
        );
        // Second DP is eager in test data, change to Queued
        let second_state = updated_plan
            .data_product(param.data_products[1].id)
            .unwrap()
            .state;
        assert!(matches!(second_state, State::Queued));
        // Third DP remains Waiting
        assert_eq!(
            updated_plan
                .data_product(param.data_products[2].id)
                .unwrap()
                .state,
            State::Waiting
        );
    }

    /// Test states_edit rejects invalid state transitions
    #[sqlx::test]
    async fn test_state_edit_rejects_invalid_states(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let param = create_test_plan_param();
        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;
        let dp_id = plan.data_products[0].id;

        for invalid_state in [State::Disabled, State::Queued, State::Waiting] {
            let states = vec![StateParam {
                id: dp_id,
                state: invalid_state,
                run_id: Some(Uuid::new_v4()),
                link: Some("http://success.com".to_string()),
                passback: Some(json!({"completed": true})),
            }];

            let result = states_edit(&mut tx, dataset_id, &states, username).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                format!("{err}"),
                format!("The requested state for {dp_id} is invalid: {invalid_state}")
            );
        }
    }

    /// Test states_edit accepts valid state transitions
    #[sqlx::test]
    async fn test_state_edit_accept_valid_states(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let param = create_test_plan_param();
        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;
        let dp_id = plan.data_products[0].id;

        for invalid_state in [State::Failed, State::Running, State::Success] {
            let states = vec![StateParam {
                id: dp_id,
                state: invalid_state,
                run_id: Some(Uuid::new_v4()),
                link: Some("http://success.com".to_string()),
                passback: Some(json!({"completed": true})),
            }];

            let result = states_edit(&mut tx, dataset_id, &states, username).await;

            assert!(result.is_ok());
        }
    }

    // Tests for clear_edit function

    /// Test clear_edit can clear a data product to queued state when conditions are met
    #[sqlx::test]
    async fn test_clear_edit_clears_to_queued_when_ready(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true, // Child is eager
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![DependencyParam {
                parent_id: dp1_id,
                child_id: dp2_id,
                extra: None,
            }],
        };

        let dataset_id = param.dataset.id;
        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // Set parent to Success so child can be triggered when cleared
        let modified_date = Utc::now();
        state_update(
            &mut tx,
            &mut plan,
            &StateParam {
                id: dp1_id,
                state: State::Success,
                ..Default::default()
            },
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Set child to some other state (like Failed)
        state_update(
            &mut tx,
            &mut plan,
            &StateParam {
                id: dp2_id,
                state: State::Failed,
                ..Default::default()
            },
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Clear the child data product
        let result = clear_edit(&mut tx, dataset_id, &[dp2_id], username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        // Child should be queued since parent is successful, dataset is unpaused, and child is eager
        assert_eq!(
            updated_plan.data_product(dp2_id).unwrap().state,
            State::Queued
        );
    }

    /// Test clear_edit sets downstream children to waiting state
    #[sqlx::test]
    async fn test_clear_edit_sets_downstream_children_to_waiting(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "child2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp2_id,
                    child_id: dp3_id,
                    extra: None,
                },
            ],
        };

        let dataset_id = param.dataset.id;
        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // Set downstream children to different states
        let modified_date = Utc::now();
        let states = vec![
            StateParam {
                id: dp2_id,
                state: State::Success,
                ..Default::default()
            },
            StateParam {
                id: dp3_id,
                state: State::Running,
                ..Default::default()
            },
        ];

        for state in states {
            state_update(&mut tx, &mut plan, &state, username, modified_date)
                .await
                .unwrap();
        }

        // Clear the parent data product
        let result = clear_edit(&mut tx, dataset_id, &[dp1_id], username).await;
        assert!(result.is_ok());

        let updated_plan = result.unwrap();
        // Both downstream children should be set to waiting
        assert_eq!(
            updated_plan.data_product(dp2_id).unwrap().state,
            State::Waiting
        );
        assert_eq!(
            updated_plan.data_product(dp3_id).unwrap().state,
            State::Waiting
        );
    }

    /// Test clear_edit returns error for non-existent data product
    #[sqlx::test]
    async fn test_clear_edit_rejects_nonexistent_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let dataset_id = param.dataset.id;
        let nonexistent_dp_id = Uuid::new_v4();

        // Create initial plan
        plan_add(&mut tx, &param, username).await.unwrap();

        // Try to clear non-existent data product
        let result = clear_edit(&mut tx, dataset_id, &[nonexistent_dp_id], username).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            format!("Data product not found for: {nonexistent_dp_id}")
        );
    }

    /// Test clear_edit returns error for disabled data product
    #[sqlx::test]
    async fn test_clear_edit_rejects_disabled_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let dataset_id = param.dataset.id;
        let dp_id = param.data_products[0].id;

        // Create initial plan
        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // First disable the data product
        let modified_date = Utc::now();
        state_update(
            &mut tx,
            &mut plan,
            &StateParam {
                id: dp_id,
                state: State::Disabled,
                ..Default::default()
            },
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Try to clear the disabled data product
        let result = clear_edit(&mut tx, dataset_id, &[dp_id], username).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(format!("{err}"), format!("Data product is locked: {dp_id}"));
    }

    // Tests for disable_drop function

    /// Test disable_drop - Success Case
    #[sqlx::test]
    async fn test_disable_drop_success(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let dataset_id = param.dataset.id;
        let dp_id = param.data_products[0].id;

        // Create initial plan
        plan_add(&mut tx, &param, username).await.unwrap();

        // Test disable_drop
        let result = disable_drop(&mut tx, dataset_id, &[dp_id], username).await;
        assert!(result.is_ok());

        let plan = result.unwrap();
        let data_product = plan.data_product(dp_id).unwrap();
        assert_eq!(
            data_product.state,
            State::Disabled,
            "Data product should be disabled after disable_drop operation"
        );
    }

    /// Test disable_drop - Non-existent Data Product
    #[sqlx::test]
    async fn test_disable_drop_nonexistent_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let dataset_id = param.dataset.id;
        let nonexistent_dp_id = Uuid::new_v4();

        // Create initial plan
        plan_add(&mut tx, &param, username).await.unwrap();

        // Try to disable non-existent data product
        let result = disable_drop(&mut tx, dataset_id, &[nonexistent_dp_id], username).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            format!("Data product not found for: {nonexistent_dp_id}")
        );
    }

    /// Test disable_drop - Already Disabled Data Product
    #[sqlx::test]
    async fn test_disable_drop_already_disabled(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";
        let dataset_id = param.dataset.id;
        let dp_id = param.data_products[0].id;

        // Create initial plan
        plan_add(&mut tx, &param, username).await.unwrap();

        // First disable
        let result = disable_drop(&mut tx, dataset_id, &[dp_id], username).await;

        assert!(result.is_ok());
        let plan = result.unwrap();

        assert_eq!(plan.data_product(dp_id).unwrap().state, State::Disabled);

        // Try to disable again
        let result = disable_drop(&mut tx, dataset_id, &[dp_id], username).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert_eq!(format!("{err}"), format!("Data product is locked: {dp_id}"));
    }

    /// Test disable_drop - Complex Parent-Child Triggering Scenario
    #[sqlx::test]
    async fn test_disable_drop_triggers_child_with_multiple_parents(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4(); // Parent 1 - Success
        let dp2_id = Uuid::new_v4(); // Parent 2 - Waiting (will be disabled)
        let dp3_id = Uuid::new_v4(); // Child - should trigger to Queued

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "parent2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "child".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp3_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp2_id,
                    child_id: dp3_id,
                    extra: None,
                },
            ],
        };

        let dataset_id = param.dataset.id;
        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // Set dp1 to Success, dp2 stays Waiting, dp3 stays Waiting
        state_update(
            &mut tx,
            &mut plan,
            &StateParam {
                id: dp1_id,
                state: State::Success,
                ..Default::default()
            },
            username,
            modified_date,
        )
        .await
        .unwrap();

        // Child should still be waiting because dp2 is not successful
        assert_eq!(plan.data_product(dp3_id).unwrap().state, State::Waiting);

        // Now disable dp2 (the waiting parent)
        let result = disable_drop(&mut tx, dataset_id, &[dp2_id], username).await;
        assert!(result.is_ok());
        plan = result.unwrap();

        // Check final states
        assert_eq!(plan.data_product(dp1_id).unwrap().state, State::Success);
        assert_eq!(plan.data_product(dp2_id).unwrap().state, State::Disabled);
        // dp3 should now be queued since its only remaining parent (dp1) is successful
        // and disabled nodes are not considered in the DAG
        assert_eq!(plan.data_product(dp3_id).unwrap().state, State::Queued);
    }

    // Tests for plan_pause_edit function

    /// Test plan_pause_edit can set pause state to true
    #[sqlx::test]
    async fn test_plan_pause_edit_set_pause_true(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;

        // Initially paused should be false
        assert!(!plan.dataset.paused, "Plan should initially be unpaused");

        // Set pause state to true
        let result = plan_pause_edit(&mut tx, dataset_id, true, username).await;
        assert!(result.is_ok());

        let paused_plan = result.unwrap();
        assert!(
            paused_plan.dataset.paused,
            "Plan should be paused after setting pause to true"
        );
        assert_eq!(
            paused_plan.dataset.modified_by, username,
            "Modified by should be updated to the user"
        );
    }

    /// Test plan_pause_edit can set pause state to false
    #[sqlx::test]
    async fn test_plan_pause_edit_set_pause_false(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;
        let modified_date = Utc::now();

        // First pause the plan
        plan.paused(&mut tx, true, username, modified_date)
            .await
            .unwrap();
        assert!(
            plan.dataset.paused,
            "Plan should be paused after calling paused method"
        );

        // Now unpause it
        let result = plan_pause_edit(&mut tx, dataset_id, false, username).await;
        assert!(result.is_ok());

        let unpaused_plan = result.unwrap();
        assert!(
            !unpaused_plan.dataset.paused,
            "Plan should be unpaused after setting pause to false"
        );
        assert_eq!(
            unpaused_plan.dataset.modified_by, username,
            "Modified by should be updated to the user"
        );
    }

    /// Test plan_pause_edit errors when trying to set pause state to current state
    #[sqlx::test]
    async fn test_plan_pause_edit_error_same_state(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = plan.dataset.id;

        // Initially paused is false, try to set it to false again
        let result = plan_pause_edit(&mut tx, dataset_id, false, username).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            format!("{err}"),
            format!("Dataset '{dataset_id}' pause state is already set to: false")
        );

        // First pause the plan
        let paused_plan = plan_pause_edit(&mut tx, dataset_id, true, username)
            .await
            .unwrap();
        assert!(
            paused_plan.dataset.paused,
            "Plan should be paused after first pause operation"
        );

        // Try to pause it again
        let result = plan_pause_edit(&mut tx, dataset_id, true, username).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            format!("{err}"),
            format!("Dataset '{dataset_id}' pause state is already set to: true")
        );
    }

    /// Test plan_pause_edit - when paused, downstream tasks remain in waiting state
    #[sqlx::test]
    async fn test_plan_pause_edit_paused_downstream_waiting(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "child2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp3_id,
                    extra: None,
                },
            ],
        };

        let dataset_id = param.dataset.id;
        let plan = plan_add(&mut tx, &param, username).await.unwrap();
        assert!(!plan.dataset.paused, "Plan should initially be unpaused");

        // Pause the plan
        let paused_plan = plan_pause_edit(&mut tx, dataset_id, true, username)
            .await
            .unwrap();
        assert!(
            paused_plan.dataset.paused,
            "Plan should be paused after pause operation"
        );

        // Set parent to Success so children would normally be triggered
        let plan = states_edit(
            &mut tx,
            dataset_id,
            &[StateParam {
                id: dp1_id,
                state: State::Success,
                ..Default::default()
            }],
            username,
        )
        .await
        .unwrap();

        // Children should remain in waiting state even though parent is successful
        assert_eq!(plan.data_product(dp2_id).unwrap().state, State::Waiting);
        assert_eq!(plan.data_product(dp3_id).unwrap().state, State::Waiting);
    }

    /// Test plan_pause_edit - when unpaused, downstream tasks enter queued state
    #[sqlx::test]
    async fn test_plan_pause_edit_unpaused_downstream_queued(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";
        let modified_date = Utc::now();

        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let dp3_id = Uuid::new_v4();

        let param = PlanParam {
            dataset: DatasetParam {
                id: Uuid::new_v4(),
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "parent".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Cams,
                    name: "child1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp3_id,
                    compute: Compute::Cams,
                    name: "child2".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
            ],
            dependencies: vec![
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp2_id,
                    extra: None,
                },
                DependencyParam {
                    parent_id: dp1_id,
                    child_id: dp3_id,
                    extra: None,
                },
            ],
        };

        let dataset_id = param.dataset.id;
        let mut plan = plan_add(&mut tx, &param, username).await.unwrap();

        // First pause the plan
        plan.paused(&mut tx, true, username, modified_date)
            .await
            .unwrap();

        // Set parent to Success while paused
        states_edit(
            &mut tx,
            dataset_id,
            &[StateParam {
                id: dp1_id,
                state: State::Success,
                ..Default::default()
            }],
            username,
        )
        .await
        .unwrap();

        // Children should still be waiting while paused
        assert_eq!(plan.data_product(dp2_id).unwrap().state, State::Waiting);
        assert_eq!(plan.data_product(dp3_id).unwrap().state, State::Waiting);

        // Now unpause the plan
        let unpaused_plan = plan_pause_edit(&mut tx, dataset_id, false, username)
            .await
            .unwrap();
        assert!(
            !unpaused_plan.dataset.paused,
            "Plan should be unpaused after unpause operation"
        );

        // Children should now be queued since parent is successful and plan is unpaused
        assert_eq!(
            unpaused_plan.data_product(dp2_id).unwrap().state,
            State::Queued
        );
        assert_eq!(
            unpaused_plan.data_product(dp3_id).unwrap().state,
            State::Queued
        );
    }

    /// Test plan_pause_edit returns error for non-existent plan
    #[sqlx::test]
    async fn test_plan_pause_edit_nonexistent_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let nonexistent_id = Uuid::new_v4();
        let username = "test_user";

        let result = plan_pause_edit(&mut tx, nonexistent_id, true, username).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            "no rows returned by a query that expected to return at least one row"
        );
    }

    // Tests for plan_search_read function

    /// Test plan_search_read can return search results
    #[sqlx::test]
    async fn test_plan_search_read_returns_results(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create test plans with different dataset names
        let mut param1 = create_test_plan_param();
        param1.dataset.extra = Some(json!({"name": "test-dataset-alpha"}));
        let mut param2 = create_test_plan_param();
        param2.dataset.id = Uuid::new_v4();
        param2.dataset.extra = Some(json!({"name": "test-dataset-beta"}));
        let mut param3 = create_test_plan_param();
        param3.dataset.id = Uuid::new_v4();
        param3.dataset.extra = Some(json!({"name": "another-dataset"}));

        // Add plans to database
        plan_add(&mut tx, &param1, username).await.unwrap();
        plan_add(&mut tx, &param2, username).await.unwrap();
        plan_add(&mut tx, &param3, username).await.unwrap();

        // Search for datasets containing "test-dataset"
        let search_term = "test-dataset";
        let page = 0;
        let result = plan_search_read(&mut tx, search_term, page).await;
        assert!(result.is_ok());

        let search_results = result.unwrap();
        // Should return at least 2 results (test-dataset-alpha and test-dataset-beta)
        assert!(search_results.len() >= 2);

        // Verify that the datasets we created are in the results
        let result_ids: Vec<_> = search_results.iter().map(|s| s.dataset_id).collect();
        assert!(result_ids.contains(&param1.dataset.id));
        assert!(result_ids.contains(&param2.dataset.id));
    }

    /// Test plan_search_read with empty search results
    #[sqlx::test]
    async fn test_plan_search_read_empty_results(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create a test plan
        let param = create_test_plan_param();
        plan_add(&mut tx, &param, username).await.unwrap();

        // Search for something that doesn't exist
        let search_term = "nonexistent-dataset-xyz";
        let page = 0;
        let result = plan_search_read(&mut tx, search_term, page).await;
        assert!(result.is_ok());

        let search_results = result.unwrap();
        assert!(search_results.is_empty());
    }

    /// Test plan_search_read pagination logic
    #[sqlx::test]
    async fn test_plan_search_read_pagination(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create multiple test plans with similar names to ensure pagination
        for i in 0..60 {
            let mut param = create_test_plan_param();
            param.dataset.id = Uuid::new_v4();
            param.dataset.extra = Some(json!({"name": format!("paginated-dataset-{:03}", i)}));
            plan_add(&mut tx, &param, username).await.unwrap();
        }

        let search_term = "paginated-dataset";

        // Test first page (page 0)
        let result = plan_search_read(&mut tx, search_term, 0).await;
        assert!(result.is_ok());
        let page_0_results = result.unwrap();
        assert_eq!(page_0_results.len(), 50); // PAGE_SIZE is 50

        // Test second page (page 1)
        let result = plan_search_read(&mut tx, search_term, 1).await;
        assert!(result.is_ok());
        let page_1_results = result.unwrap();
        assert_eq!(page_1_results.len(), 10); // Remaining 10 results

        // Test third page (page 2) - should be empty
        let result = plan_search_read(&mut tx, search_term, 2).await;
        assert!(result.is_ok());
        let page_2_results = result.unwrap();
        assert!(page_2_results.is_empty());

        // Verify results are different between pages
        let page_0_ids: Vec<_> = page_0_results.iter().map(|s| s.dataset_id).collect();
        let page_1_ids: Vec<_> = page_1_results.iter().map(|s| s.dataset_id).collect();
        assert!(page_0_ids.iter().all(|id| !page_1_ids.contains(id)));
    }

    /// Test plan_search_read with case insensitive search
    #[sqlx::test]
    async fn test_plan_search_read_case_insensitive(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create test plan with mixed case name
        let mut param = create_test_plan_param();
        param.dataset.extra = Some(json!({"name": "MixedCase-Dataset"}));
        plan_add(&mut tx, &param, username).await.unwrap();

        // Search with lowercase
        let result = plan_search_read(&mut tx, "mixedcase", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|s| s.dataset_id == param.dataset.id));

        // Search with uppercase
        let result = plan_search_read(&mut tx, "MIXEDCASE", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|s| s.dataset_id == param.dataset.id));
    }

    /// Test plan_search_read with special characters
    #[sqlx::test]
    async fn test_plan_search_read_special_characters(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create test plan with special characters
        let mut param = create_test_plan_param();
        param.dataset.extra = Some(json!({"name": "dataset-with-special_chars@123"}));
        plan_add(&mut tx, &param, username).await.unwrap();

        // Search for the dataset with special characters
        let result = plan_search_read(&mut tx, "special_chars", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|s| s.dataset_id == param.dataset.id));

        // Test with empty search string
        let result = plan_search_read(&mut tx, "", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        // Should return results (likely all datasets)
        assert!(!results.is_empty());
    }

    /// Test plan_search_read with very long search term
    #[sqlx::test]
    async fn test_plan_search_read_long_search_term(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create test plan
        let param = create_test_plan_param();
        plan_add(&mut tx, &param, username).await.unwrap();

        // Search with very long term
        let long_search_term = "a".repeat(1000);
        let result = plan_search_read(&mut tx, &long_search_term, 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(results.is_empty());
    }

    /// Test plan_search_read offset calculation
    #[sqlx::test]
    async fn test_plan_search_read_offset_calculation(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create enough test plans to test offset calculation
        for i in 0..75 {
            let mut param = create_test_plan_param();
            param.dataset.id = Uuid::new_v4();
            param.dataset.extra = Some(json!({"name": format!("offset-test-{:03}", i)}));
            plan_add(&mut tx, &param, username).await.unwrap();
        }

        let search_term = "offset-test";

        // Test different pages to verify offset calculation
        let page_0_result = plan_search_read(&mut tx, search_term, 0).await.unwrap();
        let page_1_result = plan_search_read(&mut tx, search_term, 1).await.unwrap();

        // Page 0 should have 50 results (PAGE_SIZE)
        assert_eq!(page_0_result.len(), 50);
        // Page 1 should have 25 results (75 - 50)
        assert_eq!(page_1_result.len(), 25);

        // Verify no overlap between pages
        let page_0_ids: Vec<_> = page_0_result.iter().map(|s| s.dataset_id).collect();
        let page_1_ids: Vec<_> = page_1_result.iter().map(|s| s.dataset_id).collect();
        assert!(page_0_ids.iter().all(|id| !page_1_ids.contains(id)));
    }

    /// Test plan_search_read with high page number
    #[sqlx::test]
    async fn test_plan_search_read_high_page_number(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create a single test plan
        let param = create_test_plan_param();
        plan_add(&mut tx, &param, username).await.unwrap();

        // Search with a very high page number
        let high_page = 999;
        let result = plan_search_read(&mut tx, "test", high_page).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert!(results.is_empty());
    }

    /// Test plan_search_read function does not error on database constraints
    #[sqlx::test]
    async fn test_plan_search_read_function_reliability(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        // Create test plan
        let param = create_test_plan_param();
        plan_add(&mut tx, &param, username).await.unwrap();

        // Test with various edge case search terms
        let edge_cases = vec![
            "%",  // SQL wildcard
            "_",  // SQL single character wildcard
            "'",  // Single quote
            "\"", // Double quote
            "\\", // Backslash
            "\n", // Newline
            "\t", // Tab
        ];

        for search_term in edge_cases {
            let result = plan_search_read(&mut tx, search_term, 0).await;
            assert!(
                result.is_ok(),
                "Search should not fail with term: {search_term}",
            );
        }
    }

    /// Test plan_search_read with multiple matching datasets
    #[sqlx::test]
    async fn test_plan_search_read_multiple_matches(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let username = "test_user";

        let dataset_names = vec![
            "production-data-pipeline",
            "staging-data-pipeline",
            "development-data-pipeline",
            "test-data-processing",
            "analytics-dashboard",
        ];

        let mut created_datasets = Vec::new();
        for name in dataset_names {
            let mut param = create_test_plan_param();
            param.dataset.id = Uuid::new_v4();
            param.dataset.extra = Some(json!({"name": name}));
            plan_add(&mut tx, &param, username).await.unwrap();
            created_datasets.push(param.dataset.id);
        }

        // Search for "data" should match first 4 datasets
        let result = plan_search_read(&mut tx, "-data-", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 4);

        // Search for "pipeline" should match first 3 datasets
        let result = plan_search_read(&mut tx, "pipeline", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 3);

        // Search for "analytics" should match only 1 dataset
        let result = plan_search_read(&mut tx, "analytics", 0).await;
        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].dataset_id, created_datasets[4],
            "Search should return the dataset with the highest modified date"
        );
    }

    // Tests for data_product_read function

    /// Test data_product_read can return a data product from the database
    #[sqlx::test]
    async fn test_data_product_read_existing_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let added_plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = added_plan.dataset.id;
        let data_product_id = added_plan.data_products[0].id;

        let result = data_product_read(&mut tx, dataset_id, data_product_id).await;
        assert!(result.is_ok());

        let read_data_product = result.unwrap();
        assert_eq!(
            read_data_product.id, data_product_id,
            "Read data product ID should match the requested ID"
        );
        assert_eq!(
            read_data_product.name, added_plan.data_products[0].name,
            "Read data product name should match added plan data product name"
        );
        assert_eq!(
            read_data_product.compute, added_plan.data_products[0].compute,
            "Read data product compute should match added plan data product compute"
        );
        assert_eq!(
            read_data_product.version, added_plan.data_products[0].version,
            "Read data product version should match added plan data product version"
        );
        assert_eq!(
            read_data_product.eager, added_plan.data_products[0].eager,
            "Read data product eager should match added plan data product eager"
        );
        assert_eq!(
            read_data_product.state, added_plan.data_products[0].state,
            "Read data product state should match added plan data product state"
        );
    }

    /// Test data_product_read returns error for non-existent data product
    #[sqlx::test]
    async fn test_data_product_read_nonexistent_data_product(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let param = create_test_plan_param();
        let username = "test_user";

        let added_plan = plan_add(&mut tx, &param, username).await.unwrap();
        let dataset_id = added_plan.dataset.id;
        let nonexistent_data_product_id = Uuid::new_v4();

        let result = data_product_read(&mut tx, dataset_id, nonexistent_data_product_id).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            format!("{err}"),
            "no rows returned by a query that expected to return at least one row"
        );
    }
}
