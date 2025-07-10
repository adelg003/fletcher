use crate::{
    core::plan_read,
    model::{DataProduct, DataProductId, DatasetId, Plan, State},
    ui::{component::plan_search_component, layout::base_layout},
};
use maud::{Markup, html};
use petgraph::{
    dot::{Config, Dot, RankDir},
    graph::DiGraph,
};
use poem::{
    Result,
    error::{InternalServerError, NotFoundError},
    handler,
    http::StatusCode,
    web::{Data, Path},
};
use sqlx::{PgPool, Postgres, Transaction};

/// Index Page
#[handler]
pub async fn index(Data(pool): Data<&PgPool>) -> Result<Markup> {
    // Start Transaction
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;

    // Pull the top of the list to pre-render the page.
    let search_component: Markup = plan_search_component(&mut tx, "", 0).await?;

    Ok(base_layout(
        "Search",
        &None,
        html! {
            // Search for a Plan
            fieldset {
                legend { "Search by:" }
                input
                    id="search_by_input"
                    name="search_by"
                    class="input"
                    type="search"
                    placeholder="Begin typing to search Plans..."
                    hx-get="/component/plan_search?page=0"
                    hx-trigger="input changed delay:500ms, keyup[key=='Enter']"
                    hx-target="#search_results"
                    hx-swap="innerHTML";
            }
            // Search Results
            table {
                thead {
                    tr {
                        th { "Dataset ID" }
                        th { "Modified Date" }
                    }
                }
                tbody id="search_results" {
                    (search_component)
                }
            }
        },
    ))
}

/// Plan Page
#[handler]
pub async fn plan(Data(pool): Data<&PgPool>, Path(dataset_id): Path<String>) -> Result<Markup> {
    // Convert path dataset_id to UUID
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;

    // Start Transaction
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;

    // Pull the plan from the DB
    let plan: Plan = plan_read(&mut tx, dataset_id)
        .await
        .map_err(|err| match err.status() {
            // Map poem's "NotFound" states to "NotFoundError" so the 404 page comes up
            StatusCode::NOT_FOUND => NotFoundError.into(),
            _ => err,
        })?;

    let dag: DiGraph<DataProductId, u32> = plan.to_dag().map_err(InternalServerError)?;

    // Render plan in GraphViz DOT format
    let dag_viz: String = Dot::with_attr_getters(
        &dag,
        &[
            Config::EdgeNoLabel,
            Config::NodeNoLabel,
            Config::RankDir(RankDir::LR),
        ],
        &|_graph, _edge| String::new(),
        &|_graph, node| {
            let id: DataProductId = *node.1;
            let dp: &DataProduct = match plan.data_product(id) {
                Some(dp) => dp,
                None => return String::new(),
            };

            // Map state to the respective color and style
            let (color, style): (&str, &str) = match dp.state {
                State::Disabled => ("grey", "filed"),
                State::Failed => ("red", "bold"),
                State::Queued => ("grey", "bold"),
                State::Running => ("lightgreen", "bold"),
                State::Success => ("green", "bold"),
                State::Waiting => ("lightgrey", "bold"),
            };

            // Output GraphViz style we want
            format!(
                "label=\"{}:{}\", shape=\"box\", color=\"{}\", style=\"{}\"",
                dp.name, dp.version, color, style,
            )
        },
    )
    .to_string();

    Ok(html! {
        pre { (dag_viz) }
    })
}
