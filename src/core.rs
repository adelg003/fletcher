use crate::{
    dag::Dag,
    error::Error,
    model::{DataProduct, DataProductId, DatasetId, Edge, Plan, PlanParam, State, StateParam},
};
use chrono::{DateTime, Utc};
use petgraph::graph::DiGraph;
use poem::error::{BadRequest, Forbidden, InternalServerError, NotFound, UnprocessableEntity};
use sqlx::{Postgres, Transaction};
use std::collections::HashSet;

/// Map Crate Error to Poem Error
fn to_poem_error(err: Error) -> poem::Error {
    match err {
        // Graph is cyclical
        Error::Cyclical => UnprocessableEntity(Error::Cyclical),
        // Failed to build graph
        Error::Graph(err) => InternalServerError(err),
        // Row not found
        Error::Sqlx(sqlx::Error::RowNotFound) => NotFound(sqlx::Error::RowNotFound),
        // We hit a db constraint
        Error::Sqlx(sqlx::Error::Database(err)) if err.constraint().is_some() => BadRequest(err),
        // Unknown error
        err => InternalServerError(err),
    }
}

/// Validate a PlanParam from the user
fn validate_plan_param(param: &PlanParam, plan: &Option<Plan>) -> poem::Result<()> {
    // Does Plan have any dup data products?
    if let Some(data_product_id) = param.has_dup_data_products() {
        return Err(BadRequest(Error::Duplicate(data_product_id.to_owned())));
    }

    // Does Plan have any dup dependencies?
    if let Some((parent_id, child_id)) = param.has_dup_dependencies() {
        return Err(BadRequest(Error::DuplicateDependencies(
            parent_id.to_owned(),
            child_id.to_owned(),
        )));
    }

    // Get a list of all Data Product IDs
    let data_product_ids: HashSet<&DataProductId> = {
        let mut param_ids: Vec<&DataProductId> = param.data_product_ids();

        // If we got a plan from the DB, add its data products
        if let Some(plan) = plan {
            let plan_ids: Vec<&DataProductId> = plan.data_product_ids();
            param_ids.extend(plan_ids);
        }

        param_ids.into_iter().collect()
    };

    // Do all parents have a data product?
    let data_productless_parent: Option<&DataProductId> = param
        .parent_ids()
        .into_iter()
        .find(|id: &&DataProductId| !data_product_ids.contains(id));

    if let Some(parent_id) = data_productless_parent {
        return Err(BadRequest(Error::Missing(parent_id.to_owned())));
    }

    // Do all children have a data product?
    let data_productless_child: Option<&DataProductId> = param
        .child_ids()
        .into_iter()
        .find(|id: &&DataProductId| !data_product_ids.contains(id));

    if let Some(child_id) = data_productless_child {
        return Err(BadRequest(Error::Missing(child_id.to_owned())));
    }

    // Collect all parent / child relationships
    let edges: HashSet<Edge> = {
        let mut param_edges: Vec<Edge> = param.dependency_edges();

        // If we got a plan from the DB, add its edges
        if let Some(plan) = plan {
            let plan_edges: Vec<(&DataProductId, &DataProductId, u32)> = plan.edges();
            param_edges.extend(plan_edges);
        }

        param_edges.into_iter().collect()
    };

    // If you take all our data products and map them to a graph are they a valid dag?
    DiGraph::<&DataProductId, u32>::build_dag(data_product_ids, edges).map_err(to_poem_error)?;

    Ok(())
}

/// Add a Plan Dag to the DB
pub async fn plan_add(
    tx: &mut Transaction<'_, Postgres>,
    param: &PlanParam,
    username: &str,
) -> poem::Result<Plan> {
    // Pull any prior details
    let wip_plan = Plan::from_dataset_id(tx, &param.dataset.id).await;

    // So what did we get from the DB?
    let plan: Option<Plan> = match wip_plan {
        Ok(plan) => Some(plan),
        Err(Error::Sqlx(sqlx::Error::RowNotFound)) => None,
        Err(err) => return Err(InternalServerError(err)),
    };

    // Validate to make sure the user submitted valid parameters
    validate_plan_param(param, &plan)?;

    // Write our Plan to the DB
    param
        .upsert(tx, username, &Utc::now())
        .await
        .map_err(to_poem_error)
}

/// Read a Plan Dag from the DB
pub async fn plan_read(tx: &mut Transaction<'_, Postgres>, id: &DatasetId) -> poem::Result<Plan> {
    Plan::from_dataset_id(tx, id).await.map_err(to_poem_error)
}

/// Edit the state of a Data Product
pub async fn states_edit(
    tx: &mut Transaction<'_, Postgres>,
    id: &DatasetId,
    states: &Vec<StateParam>,
    username: &str,
) -> poem::Result<Plan> {
    // Timestamp of the transaction
    let modified_date: DateTime<Utc> = Utc::now();

    // Pull the Plan so we know what we are working with
    let mut plan = Plan::from_dataset_id(tx, id).await.map_err(to_poem_error)?;

    // Apply our updates to the data products
    for state in states {
        state_update(tx, &mut plan, state, username, &modified_date).await?;
    }

    // Clear all nodes that are downstream of the ones we just updated.
    clear_downstream_nodes(tx, &mut plan, states, username, &modified_date).await?;

    Ok(plan)
}

/// Update the state of our data product
async fn state_update(
    tx: &mut Transaction<'_, Postgres>,
    plan: &mut Plan,
    state: &StateParam,
    username: &str,
    modified_date: &DateTime<Utc>,
) -> poem::Result<()> {
    // Pull before we mutably borrow via plan.data_product()
    let dataset_id: DatasetId = plan.dataset.id;

    // Check the new state to update to, and make sure is valid.
    match state.state {
        State::Failed | State::Running | State::Success => Ok(()),
        State::Disabled | State::Queued | State::Waiting => {
            Err(BadRequest(Error::BadState(state.id.clone(), state.state)))
        }
    }?;

    // Pull our Data Product
    let data_product: &mut DataProduct = plan
        .data_product_mut(&state.id)
        .ok_or(NotFound(Error::Missing(state.id.clone())))?;

    // Is the data product locked?
    if data_product.state == State::Disabled {
        return Err(Forbidden(Error::Disabled(data_product.id.clone())));
    }

    // Update our Data Product State
    data_product
        .state_update(tx, &dataset_id, state, username, modified_date)
        .await
        .map_err(to_poem_error)?;

    Ok(())
}

/// Clear all downstream data products
async fn clear_downstream_nodes(
    tx: &mut Transaction<'_, Postgres>,
    plan: &mut Plan,
    states: &Vec<StateParam>,
    username: &str,
    modified_date: &DateTime<Utc>,
) -> poem::Result<()> {
    // Pull before we mutably borrow via plan.data_product()
    let dataset_id: DatasetId = plan.dataset.id;

    // What are all the downstream nodes of our updated states?
    let downstream_ids: HashSet<DataProductId> = {
        // Generate Dag representation of the plan
        let dag: DiGraph<&DataProductId, u32> = plan.to_dag().map_err(to_poem_error)?;

        // What are all the downstream nodes of our updated states?
        let mut downstream_ids = HashSet::<&DataProductId>::new();
        for state in states {
            let new_ids: HashSet<&DataProductId> = dag.downstream_nodes(&state.id);
            downstream_ids.extend(new_ids);
        }

        // Remove all nodes that are already in a Waiting or Disabled state
        downstream_ids.retain(|id: &&DataProductId| {
            !matches!(
                plan.data_product(id),
                Some(dp) if matches!(dp.state, State::Waiting | State::Disabled),
            )
        });

        // Clone the values so we can free up the pointers in the dag and allow more mutable
        // borrows of plan.
        downstream_ids.into_iter().cloned().collect()
    };

    // Get ready to update the state of the data products
    for id in downstream_ids {
        let data_product: &mut DataProduct = plan
            .data_product_mut(&id)
            .ok_or(InternalServerError(Error::Missing(id.clone())))?;

        // Params for the current and new state. Make our change and dump the current state to fill the rest.
        let current_state: StateParam = data_product.into();
        let new_state = StateParam {
            state: State::Waiting,
            ..current_state
        };

        // Set non-waiting downstream_nodes to waiting
        data_product
            .state_update(tx, &dataset_id, &new_state, username, modified_date)
            .await
            .map_err(to_poem_error)?;
    }

    Ok(())
}
