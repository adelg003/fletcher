use crate::{
    core::plan_read,
    model::{DataProduct, DataProductId, DatasetId, Plan, State},
    ui::{component::plan_search_component, layout::base_layout},
};
use maud::{Markup, PreEscaped, html};
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
pub async fn index_page(Data(pool): Data<&PgPool>) -> Result<Markup> {
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

/// Format nodes in the GraphViz format we want for the graph we will show in the UI
fn format_node(id: DataProductId, plan: &Plan) -> String {
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
}

/// Render a GraphViz DOT notation to HTML format
fn render_dot(dot: &str) -> Markup {
    let viz_js_script: PreEscaped<String> = PreEscaped(format!(
        r#"Viz.instance().then(function(viz) {{
            var svg = viz.renderSVGElement(` {dot} `);

            document.getElementById("graph").appendChild(svg);
        }});"#,
    ));

    html! {
        div id="graph" {}
        script src="/assets/viz-js/viz-standalone.js" {}
        script { (viz_js_script) }
    }
}

/// Plan Page
#[handler]
pub async fn plan_page(
    Data(pool): Data<&PgPool>,
    Path(dataset_id): Path<String>,
) -> Result<Markup> {
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
        &|_graph, (_node_idx, data_product_id)| format_node(*data_product_id, &plan),
    )
    .to_string();

    Ok(base_layout(
        "Plan",
        &Some(dataset_id),
        html! {
            // GraphViz representation of the plan's state
            h2 { "Plan's Current State:" }
            (render_dot(&dag_viz))

            // Data Products state details
            table {
                thead {
                    tr {
                        th { "Data Product ID" }
                        th { "Compute" }
                        th { "Name" }
                        th { "Version" }
                        th { "Eager" }
                        th { "State" }
                        th { "Run ID" }
                        th { "Link" }
                        th { "Last Update" }
                    }
                }
                tbody {
                    @for dp in plan.data_products {
                        tr
                            id={ "row_" (dp.id) } {
                            td { (dp.id) }
                            td { (dp.compute) }
                            td { (dp.name) }
                            td { (dp.version) }
                            td { (dp.eager) }
                            td { (dp.state) }
                            @if let Some(run_id) = dp.run_id {
                                td { (run_id) }
                            } @else {
                                td {}
                            }
                            @if let Some(link) = dp.link {
                                td { (link) }
                            } @else {
                                td {}
                            }
                            td { (dp.modified_date) }
                        }
                    }
                }
            }
        },
    ))
}
