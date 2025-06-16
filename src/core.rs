use crate::{
    dag::Dag,
    error::Error,
    model::{DataProductId, DatasetId, Plan, PlanParam},
};
use chrono::Utc;
use petgraph::graph::DiGraph;
use poem::error::{BadRequest, InternalServerError, NotFound, Result, UnprocessableEntity};
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
fn validate_plan_param(param: &PlanParam, plan: &Option<Plan>) -> Result<()> {
    // Does Plan have any dup data products?
    if let Some(data_product_id) = param.has_dup_data_products() {
        return Err(BadRequest(Error::DuplicateDataProduct(data_product_id)));
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

    // Do all parents have a data product?
    let data_productless_parent: Option<DataProductId> = param
        .parent_ids()
        .into_iter()
        .find(|id: &DataProductId| !data_product_ids.contains(id));

    if let Some(parent_id) = data_productless_parent {
        return Err(BadRequest(Error::MissingDataProduct(parent_id)));
    }

    // Do all children have a data product?
    let data_productless_child: Option<DataProductId> = param
        .child_ids()
        .into_iter()
        .find(|id: &DataProductId| !data_product_ids.contains(id));

    if let Some(child_id) = data_productless_child {
        return Err(BadRequest(Error::MissingDataProduct(child_id)));
    }

    // Collect all parent / child relationships
    let dependency_edges: HashSet<(DataProductId, DataProductId, u32)> = {
        let mut param_edges: Vec<(DataProductId, DataProductId, u32)> = param.dependency_edges();

        // If we got a plan from the DB, add its edges
        if let Some(plan) = plan {
            let plan_edges: Vec<(DataProductId, DataProductId, u32)> = plan.dependency_edges();
            param_edges.extend(plan_edges);
        }

        param_edges.into_iter().collect()
    };

    // If you take all our data products and map them to a graph are they a valid dag?
    DiGraph::<DataProductId, u32>::build_dag(data_product_ids, dependency_edges)
        .map_err(to_poem_error)?;

    Ok(())
}

/// Add a Plan Dag to the DB
pub async fn plan_add(
    tx: &mut Transaction<'_, Postgres>,
    param: &PlanParam,
    username: &str,
) -> Result<Plan> {
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
pub async fn plan_read(tx: &mut Transaction<'_, Postgres>, id: &DatasetId) -> Result<Plan> {
    Plan::from_dataset_id(tx, id).await.map_err(to_poem_error)
}
