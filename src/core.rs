use crate::{
    graph::{build_graph, check_if_dag},
    model::{Plan, PlanParam},
};
use petgraph::graph::DiGraph;
use poem::{
    error::{BadRequest, InternalServerError, NotFound},
    http::StatusCode,
};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Map SQLx to Poem Errors
pub fn sqlx_to_poem_error(err: sqlx::Error) -> poem::Error {
    match err {
        sqlx::Error::RowNotFound => NotFound(err),
        sqlx::Error::Database(err) if err.constraint().is_some() => BadRequest(err),
        err => InternalServerError(err),
    }
}

/// Validate a PlanParam from the user
fn validate_plan_param(param: &PlanParam, plan: &Option<Plan>) -> Result<(), poem::Error> {
    // Does Plan have any dup data products?
    if param.has_dup_data_products() {
        return Err(poem::Error::from_string(
            "Duplicate data product ids found",
            StatusCode::BAD_REQUEST,
        ));
    }

    // Does Plan have any dup dependencies?
    if param.has_dup_dependencies() {
        return Err(poem::Error::from_string(
            "Duplicate dependencies found",
            StatusCode::BAD_REQUEST,
        ));
    }

    // Get a list of all Data Product IDs
    let data_product_ids: Vec<String> = {
        let mut param_ids: Vec<String> = param.data_product_ids();

        // If we got a plan from the DB, add its data products
        if let Some(plan) = plan {
            let plan_ids: Vec<String> = plan.data_product_ids();
            param_ids.extend(plan_ids);
        }

        param_ids
    };

    // Do all parents have a data product?
    let data_productless_parent: Option<String> = param
        .parent_ids()
        .into_iter()
        .find(|id: &String| !data_product_ids.contains(id));

    if let Some(parent_id) = data_productless_parent {
        return Err(poem::Error::from_string(
            format!("No data product for parent dependency: {parent_id}"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }

    // Do all children have a data product?
    let data_productless_child: Option<String> = param
        .child_ids()
        .into_iter()
        .find(|id: &String| !data_product_ids.contains(id));

    if let Some(child_id) = data_productless_child {
        return Err(poem::Error::from_string(
            format!("No data product for child dependency: {child_id}"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }

    // Collect all parent / child relationships
    let dependency_edges: Vec<(String, String, u32)> = {
        let mut param_edges: Vec<(String, String, u32)> =
            param.dependency_edges().into_iter().collect();

        // If we got a plan from the DB, add its edges
        if let Some(plan) = plan {
            let plan_edges: Vec<(String, String, u32)> =
                plan.dependency_edges().into_iter().collect();
            param_edges.extend(plan_edges);
        }

        param_edges
    };

    // Convert node and edges to a graph
    let graph: DiGraph<String, u32> =
        build_graph(data_product_ids, dependency_edges).map_err(InternalServerError)?;

    // Is the graph for the plan cyclical?
    check_if_dag(&graph)?;

    Ok(())
}

/// Add a Plan Dag to the DB
pub async fn plan_dag_add(
    tx: &mut Transaction<'_, Postgres>,
    param: PlanParam,
    username: &str,
) -> Result<Plan, poem::Error> {
    // Pull any prior details
    let wip_plan = Plan::from_dataset_id(param.dataset.id, tx).await;

    // So what did we get form the DB?
    let plan: Option<Plan> = match wip_plan {
        Ok(plan) => Some(plan),
        Err(sqlx::Error::RowNotFound) => None,
        Err(err) => return Err(InternalServerError(err)),
    };

    // Validate to make sure the user submitted valid parameters
    validate_plan_param(&param, &plan)?;

    // Write our Plan Dag to the DB
    param.upsert(tx, username).await.map_err(sqlx_to_poem_error)
}

/// Read a Plan Dag from the DB
pub async fn plan_dag_read(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<Plan, poem::Error> {
    Plan::from_dataset_id(id, tx)
        .await
        .map_err(sqlx_to_poem_error)
}
