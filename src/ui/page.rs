use crate::{
    Config,
    core::{
        clear_edit_with_config, plan_read, plan_run_read, plan_run_step_logs_read, plan_runs_read,
    },
    model::{DataProduct, DataProductId, DatasetId, ExecutionLog, Plan, PlanRun, RunPlan, State},
    ui::{component::plan_search_component, layout::base_layout},
};
use maud::{Markup, PreEscaped, html};
use poem::{
    Result,
    error::{InternalServerError, NotFoundError},
    handler,
    http::StatusCode,
    web::{Data, Json, Path, Query, Redirect},
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

struct PlanPageData {
    plan: Plan,
    runs: Vec<PlanRun>,
    dag_nodes_json: String,
}

struct RunPageData {
    plan: Plan,
    runs: Vec<PlanRun>,
    run_plan: RunPlan,
    dag_nodes_json: String,
    selected_step_id: Option<DataProductId>,
    selected_step_logs: Vec<ExecutionLog>,
}

#[derive(Serialize)]
struct RunDagUpdatePayload {
    nodes: Vec<NiceDagNode>,
    active: bool,
}

#[derive(Default, Deserialize)]
struct RunStepQuery {
    step: Option<String>,
}

#[derive(Serialize)]
struct NiceDagNode {
    id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    dependencies: Vec<String>,
    data: NiceDagNodeData,
}

#[derive(Serialize)]
struct NiceDagNodeData {
    name: String,
    version: String,
    state: String,
    compute: String,
    run_id: Option<String>,
}

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
            fieldset class="fieldset m-8 animate-fade" {
                legend class="fieldset-legend" { "Search by:" }
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
            table class="table table-zebra table-md animate-fade" {
                thead {
                    tr {
                        th { "Dataset ID" }
                        th { "Modified Date" }
                        th { "Extra" }
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
#[cfg_attr(not(test), allow(dead_code))]
fn format_node(id: DataProductId, plan: &Plan) -> String {
    let dp: &DataProduct = match plan.data_product(id) {
        Some(dp) => dp,
        None => return String::new(),
    };

    // Map state to the respective color and style
    let (color, style): (&str, &str) = match dp.state {
        State::Disabled => ("grey", "filled"),
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
#[cfg_attr(not(test), allow(dead_code))]
fn render_dot(dot: &str) -> Markup {
    // Escape backticks to prevent breaking the JavaScript template literal
    let escaped_dot = dot.replace('`', "\\`");
    let viz_js_script: PreEscaped<String> = PreEscaped(format!(
        r#"Viz.instance().then(function(viz) {{
            var svg = viz.renderSVGElement(` {escaped_dot} `);
            svg.classList.add('w-[800px]', 'h-auto');

            document.getElementById("graph").appendChild(svg);
        }});"#,
    ));

    html! {
        div id="graph" {}
        script src="/assets/viz/viz-global.js" {}
        script { (viz_js_script) }
    }
}

async fn load_plan_page_data(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
) -> Result<PlanPageData> {
    let plan: Plan = plan_read(tx, dataset_id).await.map_err(|err| {
        let error: poem::Error = err.into_poem_error();
        match error.status() {
            StatusCode::NOT_FOUND => NotFoundError.into(),
            _ => error,
        }
    })?;

    let runs = plan_runs_read(tx, dataset_id)
        .await
        .map_err(InternalServerError)?;
    let dag_nodes_json = build_plan_dag_nodes_json(&plan, Some("defined".to_string()))
        .map_err(InternalServerError)?;

    Ok(PlanPageData {
        plan,
        runs,
        dag_nodes_json,
    })
}

async fn load_run_page_data(
    tx: &mut Transaction<'_, Postgres>,
    dataset_id: DatasetId,
    plan_run_id: Uuid,
    selected_step_id: Option<DataProductId>,
) -> Result<RunPageData> {
    let plan: Plan = plan_read(tx, dataset_id).await.map_err(|err| {
        let error: poem::Error = err.into_poem_error();
        match error.status() {
            StatusCode::NOT_FOUND => NotFoundError.into(),
            _ => error,
        }
    })?;
    let runs = plan_runs_read(tx, dataset_id)
        .await
        .map_err(InternalServerError)?;
    let run_plan = plan_run_read(tx, dataset_id, plan_run_id)
        .await
        .map_err(|err| {
            let error: poem::Error = err.into_poem_error();
            match error.status() {
                StatusCode::NOT_FOUND => NotFoundError.into(),
                _ => error,
            }
        })?;
    let dag_nodes_json = build_run_dag_nodes_json(&run_plan).map_err(InternalServerError)?;
    let selected_step_logs = if let Some(step_id) = selected_step_id {
        plan_run_step_logs_read(tx, dataset_id, plan_run_id, step_id)
            .await
            .map_err(InternalServerError)?
    } else {
        Vec::new()
    };

    Ok(RunPageData {
        plan,
        runs,
        run_plan,
        dag_nodes_json,
        selected_step_id,
        selected_step_logs,
    })
}

fn build_plan_dag_nodes_json(
    plan: &Plan,
    state_override: Option<String>,
) -> serde_json::Result<String> {
    let active_ids: HashSet<DataProductId> = plan
        .data_products
        .iter()
        .filter(|dp| dp.state != State::Disabled)
        .map(|dp| dp.id)
        .collect();
    let dependency_map: HashMap<DataProductId, Vec<String>> = plan
        .dependencies
        .iter()
        .filter(|dep| active_ids.contains(&dep.parent_id) && active_ids.contains(&dep.child_id))
        .fold(HashMap::new(), |mut acc, dep| {
            acc.entry(dep.child_id)
                .or_default()
                .push(dep.parent_id.to_string());
            acc
        });
    let dag_nodes: Vec<NiceDagNode> = plan
        .data_products
        .iter()
        .filter(|dp| dp.state != State::Disabled)
        .map(|dp| NiceDagNode {
            id: dp.id.to_string(),
            dependencies: dependency_map.get(&dp.id).cloned().unwrap_or_default(),
            data: NiceDagNodeData {
                name: dp.name.clone(),
                version: dp.version.clone(),
                state: state_override
                    .clone()
                    .unwrap_or_else(|| dp.state.to_string()),
                compute: dp.compute.to_string(),
                run_id: dp.run_id.map(|run_id| run_id.to_string()),
            },
        })
        .collect();
    serde_json::to_string(&dag_nodes)
}

fn build_run_dag_nodes(run_plan: &RunPlan) -> Vec<NiceDagNode> {
    let dependency_map: HashMap<DataProductId, Vec<String>> =
        run_plan
            .dependencies
            .iter()
            .fold(HashMap::new(), |mut acc, dep| {
                acc.entry(dep.child_id)
                    .or_default()
                    .push(dep.parent_id.to_string());
                acc
            });
    let dag_nodes: Vec<NiceDagNode> = run_plan
        .data_products
        .iter()
        .map(|dp| NiceDagNode {
            id: dp.data_product_id.to_string(),
            dependencies: dependency_map
                .get(&dp.data_product_id)
                .cloned()
                .unwrap_or_default(),
            data: NiceDagNodeData {
                name: dp.name.clone(),
                version: dp.version.clone(),
                state: dp.state.to_string(),
                compute: dp.compute.to_string(),
                run_id: dp.step_run_id.map(|run_id| run_id.to_string()),
            },
        })
        .collect();
    dag_nodes
}

fn build_run_dag_nodes_json(run_plan: &RunPlan) -> serde_json::Result<String> {
    let dag_nodes = build_run_dag_nodes(run_plan);
    serde_json::to_string(&dag_nodes)
}

fn render_plan_summary(plan: &Plan) -> Markup {
    html! {
        div class="stats shadow animate-fade" {
            div class="stat" {
                div class="stat-title" { "Dataset ID:" }
                div class="stat-value" { (plan.dataset.id) }
                div class="stat-desc" {
                    @if plan.dataset.paused {
                        span class={ "badge badge-sm badge-warning" } {
                            "Paused"
                        }
                    } @else {
                        span class={ "badge badge-sm badge-info" } {
                            "Active"
                        }
                    }
                }
            }
            // Modified
            div class="stat" {
                div class="stat-title" { "Modified By:" }
                div class="stat-value" { (&plan.dataset.modified_by) }
                div class="stat-desc" { "Date: " (&plan.dataset.modified_date) }
            }
        }
        div class="ml-12 mt-6 animate-fade" {
            form method="post" action={ "/plan/" (plan.dataset.id) "/trigger" } {
                button
                    type="submit"
                    class="btn btn-warning"
                    disabled[plan.dataset.paused] {
                    "Trigger Run Again"
                }
            }
            p class="mt-2 text-sm opacity-70" { "Open a run to inspect plan state and step logs." }
            @if plan.dataset.paused {
                p class="mt-2 text-sm opacity-70" {
                    "Unpause the dataset before triggering another run."
                }
            }
        }
    }
}

fn render_dag_widget(
    widget_id: &str,
    dag_nodes_json: &str,
    selection_context: Option<(DatasetId, Uuid, Option<DataProductId>)>,
) -> Markup {
    let (selection_dataset_id, selection_plan_run_id, selection_step_id) =
        if let Some((dataset_id, plan_run_id, step_id)) = selection_context {
            (
                Some(dataset_id.to_string()),
                Some(plan_run_id.to_string()),
                step_id.map(|id| id.to_string()),
            )
        } else {
            (None, None, None)
        };

    html! {
        div
            id=(widget_id)
            class="fletcher-dag-widget"
            data-dataset-id=[selection_dataset_id]
            data-plan-run-id=[selection_plan_run_id]
            data-selected-node-id=[selection_step_id] {
            div class="mb-3 mr-4 flex items-center justify-end gap-2 text-sm" {
                span class="opacity-70 mr-2" { "Plan view" }
                button type="button" class="btn btn-sm btn-outline" data-plan-action="zoom-out" { "-" }
                span class="min-w-14 text-center font-medium" data-role="zoom-label" { "100%" }
                button type="button" class="btn btn-sm btn-outline" data-plan-action="zoom-in" { "+" }
                button type="button" class="btn btn-sm btn-outline" data-plan-action="fit" { "Fit" }
            }
            div
                class="fletcher-dag-canvas overflow-auto rounded-xl bg-base-100 p-4"
                style="height: min(70vh, 720px);" {}
            script type="application/json" class="fletcher-dag-data" {
                (PreEscaped(dag_nodes_json.to_string()))
            }
        }
    }
}

fn render_plan_graph(dataset_id: DatasetId, dag_nodes_json: &str) -> Markup {
    html! {
        div class="ml-12 animate-fade" {
            h2 class="text-4xl mb-2" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Plan "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Structure:"
                }
            }
            div class="ml-4" {
                (render_dag_widget(
                    &format!("dag_widget_plan_{dataset_id}"),
                    dag_nodes_json,
                    None,
                ))
            }
        }
    }
}

fn render_run_graph(
    dataset_id: DatasetId,
    plan_run_id: Uuid,
    dag_nodes_json: &str,
    selected_step_id: Option<DataProductId>,
) -> Markup {
    html! {
        div class="ml-12 animate-fade" {
            h2 class="text-4xl mb-2" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Run "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Plan:"
                }
            }
            div class="ml-4" {
                (render_dag_widget(
                    &format!("dag_widget_run_{plan_run_id}"),
                    dag_nodes_json,
                    Some((dataset_id, plan_run_id, selected_step_id)),
                ))
            }
        }
    }
}

fn render_run_list(
    dataset_id: DatasetId,
    runs: &[PlanRun],
    selected_run_id: Option<Uuid>,
) -> Markup {
    html! {
        div class="ml-12 mt-10 mr-12 animate-fade" {
            h2 class="text-4xl mb-2" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Plan "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Runs:"
                }
            }
            div class="max-h-[24rem] overflow-y-auto pr-2" {
                @if runs.is_empty() {
                    p class="opacity-70" { "No runs recorded yet." }
                } @else {
                    div class="space-y-2" {
                    @for run in runs {
                        @let row_class = if Some(run.id) == selected_run_id {
                            "flex items-center justify-between rounded-xl border border-warning bg-base-200 px-4 py-3"
                        } else {
                            "flex items-center justify-between rounded-xl border border-base-300 bg-base-100 px-4 py-3"
                        };
                        div class=(row_class) {
                            div class="space-y-1" {
                                a class="font-semibold link link-hover" href={ "/plan/" (dataset_id) "/run/" (run.id) } {
                                    (run.id)
                                }
                                div class="text-sm opacity-70" {
                                    "Started " (run.created_at) " by " (&run.created_by)
                                }
                            }
                            span class={ "badge " (match run.status {
                                State::Success => "badge-success",
                                State::Failed => "badge-error",
                                State::Running => "badge-info",
                                State::Queued | State::Waiting => "badge-neutral",
                                State::Disabled => "badge-neutral",
                            }) } {
                                (run.status)
                            }
                        }
                    }
                    }
                }
            }
        }
    }
}

fn render_selected_step_logs(
    run_plan: &RunPlan,
    selected_step_id: Option<DataProductId>,
    logs: &[ExecutionLog],
) -> Markup {
    html! {
        div class="ml-12 mt-10 mr-12 animate-fade" {
            h2 class="text-4xl mb-2" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Step "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Logs:"
                }
            }
            @if let Some(step_id) = selected_step_id {
                @if let Some(step) = run_plan
                    .data_products
                    .iter()
                    .find(|dp| dp.data_product_id == step_id) {
                    div class="mt-3 rounded-xl border border-base-300 bg-base-100 p-4" {
                        div class="mb-3 text-sm opacity-80" {
                            (step.name) " (" (step.data_product_id) ")"
                        }
                        @if logs.is_empty() {
                            p class="opacity-70" { "No logs recorded for this step in the selected run." }
                        } @else {
                            pre class="text-sm whitespace-pre-wrap" {
                                @for log in logs {
                                    "[" (log.created_at) "] "
                                    (log.stream) ": "
                                    (log.message)
                                    "\n"
                                }
                            }
                        }
                    }
                } @else {
                    p class="mt-3 opacity-70" { "Selected step was not found in this run." }
                }
            } @else {
                p class="mt-3 opacity-70" { "Click a plan step to view its logs." }
            }
        }
    }
}

fn selected_step_from_query(params: &RunStepQuery) -> Result<Option<DataProductId>> {
    match params.step.as_deref() {
        Some(step) => Ok(Some(
            DataProductId::try_parse(step).map_err(|_| NotFoundError)?,
        )),
        None => Ok(None),
    }
}

fn run_page_script(dataset_id: DatasetId, plan_run_id: Uuid, poll_live: bool) -> Markup {
    let logs_url = format!("/plan/{dataset_id}/run/{plan_run_id}/logs");
    let nodes_url = format!("/plan/{dataset_id}/run/{plan_run_id}/nodes");
    let widget_id = format!("dag_widget_run_{plan_run_id}");
    let script = PreEscaped(format!(
        r##"
window.fletcherRunLogsUrl = {logs_url:?};
window.fletcherRunNodesUrl = {nodes_url:?};
window.fletcherRunWidgetId = {widget_id:?};
if (!window.__fletcherRunStepHandlerInstalled) {{
  window.__fletcherRunStepHandlerInstalled = true;
  window.addEventListener("fletcher:dag-step-selected", function (event) {{
    const detail = event.detail || {{}};
    if (!detail.dataProductId) return;
    const url = new URL(window.location.href);
    url.searchParams.set("step", detail.dataProductId);
    window.history.replaceState({{}}, "", url.toString());
    if (window.updatePlanDagSelection) {{
      window.updatePlanDagSelection(window.fletcherRunWidgetId, detail.dataProductId);
    }}
    if (window.htmx) {{
      window.htmx.ajax("GET", `${{window.fletcherRunLogsUrl}}?step=${{encodeURIComponent(detail.dataProductId)}}`, {{
        target: "#run_logs",
        swap: "innerHTML",
      }});
    }}
  }});
}}
if ({poll_live}) {{
  if (window.__fletcherRunPollTimer) {{
    window.clearInterval(window.__fletcherRunPollTimer);
  }}
  window.__fletcherRunPollTimer = window.setInterval(function () {{
    fetch(window.fletcherRunNodesUrl, {{ headers: {{ "Accept": "application/json" }} }})
      .then(function (response) {{ return response.ok ? response.json() : null; }})
      .then(function (payload) {{
        if (!payload || !window.updatePlanDagWidgetNodes) return;
        window.updatePlanDagWidgetNodes(window.fletcherRunWidgetId, payload.nodes || []);
        if (!payload.active && window.__fletcherRunPollTimer) {{
          window.clearInterval(window.__fletcherRunPollTimer);
          window.__fletcherRunPollTimer = null;
        }}
      }})
      .catch(function () {{}});
  }}, 2000);
}}
"##,
        poll_live = if poll_live { "true" } else { "false" },
    ));
    html! { script { (script) } }
}

/// Trigger a plan rerun from the UI by clearing all non-disabled data products.
#[handler]
pub async fn trigger_plan_post(
    Data(app_config): Data<&Config>,
    Data(pool): Data<&PgPool>,
    Path(dataset_id): Path<String>,
) -> Result<Redirect> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;

    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let plan = plan_read(&mut tx, dataset_id).await.map_err(|err| {
        let error: poem::Error = err.into_poem_error();
        match error.status() {
            StatusCode::NOT_FOUND => NotFoundError.into(),
            _ => error,
        }
    })?;

    let rerun_ids: Vec<DataProductId> = plan
        .data_products
        .iter()
        .filter_map(|dp| (dp.state != State::Disabled).then_some(dp.id))
        .collect();

    if !rerun_ids.is_empty() {
        clear_edit_with_config(
            &mut tx,
            app_config,
            dataset_id,
            &rerun_ids,
            &app_config.worker_service,
        )
        .await
        .map_err(InternalServerError)?;
    }

    let latest_run_id = plan_runs_read(&mut tx, dataset_id)
        .await
        .map_err(InternalServerError)?
        .into_iter()
        .next()
        .map(|run| run.id);

    tx.commit().await.map_err(InternalServerError)?;
    Ok(Redirect::see_other(
        latest_run_id
            .map(|plan_run_id| format!("/plan/{dataset_id}/run/{plan_run_id}"))
            .unwrap_or_else(|| format!("/plan/{dataset_id}")),
    ))
}

/// Plan Page
#[handler]
pub async fn plan_page(
    Data(pool): Data<&PgPool>,
    Path(dataset_id): Path<String>,
) -> Result<Markup> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let plan_data = load_plan_page_data(&mut tx, dataset_id).await?;

    Ok(base_layout(
        "Plan",
        &Some(dataset_id),
        html! {
            script src="/assets/nice-dag/nice-dag-core.js" {}
            script src="/assets/js/plan-dag.js" {}
            (render_plan_summary(&plan_data.plan))
            (render_plan_graph(dataset_id, &plan_data.dag_nodes_json))
            (render_run_list(dataset_id, &plan_data.runs, None))
        },
    ))
}

#[handler]
pub async fn plan_run_page(
    Data(pool): Data<&PgPool>,
    Path((dataset_id, plan_run_id)): Path<(String, String)>,
    Query(params): Query<RunStepQuery>,
) -> Result<Markup> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;
    let plan_run_id = Uuid::try_parse(&plan_run_id).map_err(|_| NotFoundError)?;
    let selected_step_id = selected_step_from_query(&params)?;
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let run_data = load_run_page_data(&mut tx, dataset_id, plan_run_id, selected_step_id).await?;
    let poll_live = run_data.run_plan.run.is_active();

    Ok(base_layout(
        "Run",
        &Some(dataset_id),
        html! {
            script src="/assets/nice-dag/nice-dag-core.js" {}
            script src="/assets/js/plan-dag.js" {}
            (run_page_script(dataset_id, plan_run_id, poll_live))
            (render_plan_summary(&run_data.plan))
            div id="run_graph" {
                (render_run_graph(
                    dataset_id,
                    plan_run_id,
                    &run_data.dag_nodes_json,
                    run_data.selected_step_id,
                ))
            }
            div id="run_logs" {
                (render_selected_step_logs(
                    &run_data.run_plan,
                    run_data.selected_step_id,
                    &run_data.selected_step_logs,
                ))
            }
            (render_run_list(dataset_id, &run_data.runs, Some(plan_run_id)))
        },
    ))
}

#[handler]
pub async fn plan_run_nodes_get(
    Data(pool): Data<&PgPool>,
    Path((dataset_id, plan_run_id)): Path<(String, String)>,
) -> Result<Json<RunDagUpdatePayload>> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;
    let plan_run_id = Uuid::try_parse(&plan_run_id).map_err(|_| NotFoundError)?;
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let run_data = plan_run_read(&mut tx, dataset_id, plan_run_id)
        .await
        .map_err(|err| {
            let error: poem::Error = err.into_poem_error();
            match error.status() {
                StatusCode::NOT_FOUND => NotFoundError.into(),
                _ => error,
            }
        })?;

    Ok(Json(RunDagUpdatePayload {
        nodes: build_run_dag_nodes(&run_data),
        active: run_data.run.is_active(),
    }))
}

#[handler]
pub async fn plan_run_graph_get(
    Data(pool): Data<&PgPool>,
    Path((dataset_id, plan_run_id)): Path<(String, String)>,
    Query(params): Query<RunStepQuery>,
) -> Result<Markup> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;
    let plan_run_id = Uuid::try_parse(&plan_run_id).map_err(|_| NotFoundError)?;
    let selected_step_id = selected_step_from_query(&params)?;
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let run_data = load_run_page_data(&mut tx, dataset_id, plan_run_id, selected_step_id).await?;

    Ok(html! {
        (render_run_graph(
            dataset_id,
            plan_run_id,
            &run_data.dag_nodes_json,
            run_data.selected_step_id,
        ))
    })
}

#[handler]
pub async fn plan_run_logs_get(
    Data(pool): Data<&PgPool>,
    Path((dataset_id, plan_run_id)): Path<(String, String)>,
    Query(params): Query<RunStepQuery>,
) -> Result<Markup> {
    let dataset_id = DatasetId::try_parse(&dataset_id).map_err(|_| NotFoundError)?;
    let plan_run_id = Uuid::try_parse(&plan_run_id).map_err(|_| NotFoundError)?;
    let selected_step_id = selected_step_from_query(&params)?;
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;
    let run_data = load_run_page_data(&mut tx, dataset_id, plan_run_id, selected_step_id).await?;

    Ok(html! {
        (render_selected_step_logs(
            &run_data.run_plan,
            run_data.selected_step_id,
            &run_data.selected_step_logs,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Compute, DataProduct, DataProductParam, Dataset, DatasetParam, DependencyParam, PlanParam,
        State,
    };
    use crate::{load_config, ui::user_service};
    use chrono::Utc;
    use poem::{
        EndpointExt,
        test::{TestClient, TestResponse},
    };
    use pretty_assertions::assert_eq;
    use scraper::{Html, Selector};
    use serde_json::json;
    use sqlx::{PgPool, Postgres, Transaction};
    use uuid::Uuid;

    /// Helper function to create a test plan with specified data products
    fn create_test_plan_with_data_products(data_products: Vec<DataProduct>) -> Plan {
        let now = Utc::now();
        let dataset_id = Uuid::new_v4();

        Plan {
            dataset: Dataset {
                id: dataset_id,
                paused: false,
                extra: Some(json!({"test":"dataset"})),
                modified_by: "test_user".to_string(),
                modified_date: now,
            },
            data_products,
            dependencies: vec![],
        }
    }

    /// Helper function to create a test data product with specified state
    fn create_test_data_product(
        id: DataProductId,
        name: &str,
        version: &str,
        state: State,
    ) -> DataProduct {
        let now = Utc::now();

        DataProduct {
            id,
            compute: Compute::Cams,
            name: name.to_string(),
            version: version.to_string(),
            eager: false,
            passthrough: Some(json!({"test":"passthrough"})),
            state,
            run_id: Some(Uuid::new_v4()),
            link: Some("http://test.com".to_string()),
            passback: Some(json!({"test":"passback"})),
            extra: Some(json!({"test":"extra"})),
            modified_by: "test_user".to_string(),
            modified_date: now,
        }
    }

    // =============== format_node Function Tests ===============

    /// Test format_node with Disabled state
    #[test]
    fn test_format_node_disabled_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "test-product", "1.0.0", State::Disabled);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result, "label=\"test-product:1.0.0\", shape=\"box\", color=\"grey\", style=\"filled\"",
            "Formatted node should match expected disabled state"
        );
    }

    /// Test format_node with Failed state
    #[test]
    fn test_format_node_failed_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "failed-product", "2.1.0", State::Failed);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result, "label=\"failed-product:2.1.0\", shape=\"box\", color=\"red\", style=\"bold\"",
            "Formatted node should match expected failed state"
        );
    }

    /// Test format_node with Queued state
    #[test]
    fn test_format_node_queued_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "queued-product", "3.0.0", State::Queued);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result, "label=\"queued-product:3.0.0\", shape=\"box\", color=\"grey\", style=\"bold\"",
            "Formatted node should match expected queued state"
        );
    }

    /// Test format_node with Running state
    #[test]
    fn test_format_node_running_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "running-product", "1.5.0", State::Running);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result,
            "label=\"running-product:1.5.0\", shape=\"box\", color=\"lightgreen\", style=\"bold\"",
            "Formatted node should match expected running state"
        );
    }

    /// Test format_node with Success state
    #[test]
    fn test_format_node_success_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "success-product", "4.2.1", State::Success);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result,
            "label=\"success-product:4.2.1\", shape=\"box\", color=\"green\", style=\"bold\"",
            "Formatted node should match expected success state"
        );
    }

    /// Test format_node with Waiting state
    #[test]
    fn test_format_node_waiting_state() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "waiting-product", "0.9.0", State::Waiting);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result,
            "label=\"waiting-product:0.9.0\", shape=\"box\", color=\"lightgrey\", style=\"bold\"",
            "Formatted node should match expected waiting state"
        );
    }

    /// Test format_node when data product ID is not found in plan
    #[test]
    fn test_format_node_data_product_not_found() {
        let existing_id = Uuid::new_v4();
        let non_existent_id = Uuid::new_v4();

        let data_product =
            create_test_data_product(existing_id, "existing-product", "1.0.0", State::Success);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(non_existent_id, &plan);

        assert_eq!(
            result,
            String::new(),
            "Formatted node should be empty if data product not found"
        );
    }

    /// Test format_node with special characters in name and version
    #[test]
    fn test_format_node_special_characters() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(
            id,
            "test-product_with.special-chars",
            "1.0.0-beta.1",
            State::Success,
        );
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        assert_eq!(
            result,
            "label=\"test-product_with.special-chars:1.0.0-beta.1\", shape=\"box\", color=\"green\", style=\"bold\"",
            "Formatted node should match expected special characters state"
        );
    }

    /// Test format_node with empty plan (no data products)
    #[test]
    fn test_format_node_empty_plan() {
        let id = Uuid::new_v4();
        let plan = create_test_plan_with_data_products(vec![]);

        let result = format_node(id, &plan);

        assert_eq!(
            result,
            String::new(),
            "Formatted node should be empty for empty plan"
        );
    }

    /// Test format_node output format structure
    #[test]
    fn test_format_node_output_format() {
        let id = Uuid::new_v4();
        let data_product = create_test_data_product(id, "format-test", "1.0", State::Running);
        let plan = create_test_plan_with_data_products(vec![data_product]);

        let result = format_node(id, &plan);

        // Verify the output follows GraphViz DOT notation format
        assert!(
            result.starts_with("label=\""),
            "Formatted node should start with label attribute"
        );
        assert!(
            result.contains("shape=\"box\""),
            "Formatted node should contain box shape"
        );
        assert!(
            result.contains("color=\""),
            "Formatted node should contain color attribute"
        );
        assert!(
            result.contains("style=\""),
            "Formatted node should contain style attribute"
        );
        assert!(
            result.ends_with("\""),
            "Formatted node should end with closing quote"
        );

        // Verify all components are present
        assert!(
            result.contains("format-test:1.0"),
            "Formatted node should contain data product name and version"
        );
        assert!(
            result.contains("lightgreen"),
            "Formatted node should contain lightgreen color for Success state"
        );
        assert!(
            result.contains("bold"),
            "Formatted node should contain bold style for Success state"
        );
    }

    // =============== Render Dot Tests ===============

    /// Test render_dot with simple DOT notation
    #[test]
    fn test_render_dot_simple() {
        let dot_string = "digraph G { A -> B; }";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Parse HTML and verify structure
        let document = Html::parse_fragment(&html_string);

        // Should contain div with id="graph"
        let div_selector = Selector::parse("div#graph").unwrap();
        let div = document.select(&div_selector).next();
        assert!(div.is_some(), "Should have div with id='graph'");

        // Should contain script tag for viz-js library
        let script_selector = Selector::parse("script[src]").unwrap();
        let script_tag = document.select(&script_selector).next();
        assert!(
            script_tag.is_some(),
            "Should have script tag with src attribute"
        );
        assert_eq!(
            script_tag.unwrap().attr("src"),
            Some("/assets/viz/viz-global.js"),
            "Script src should point to viz-global.js"
        );

        // Should contain inline script with DOT string
        assert!(
            html_string.contains("Viz.instance().then"),
            "Should contain Viz.instance() call"
        );
        assert!(
            html_string.contains("renderSVGElement"),
            "Should contain renderSVGElement call"
        );
        assert!(
            html_string.contains("document.getElementById(\"graph\")"),
            "Should target graph element"
        );
        assert!(
            html_string.contains("appendChild(svg)"),
            "Should append SVG to graph element"
        );
        assert!(
            html_string.contains(dot_string),
            "Should contain the DOT string"
        );
    }

    /// Test render_dot with complex DOT notation
    #[test]
    fn test_render_dot_complex() {
        let dot_string = r#"digraph test {
            node [shape=box];
            A [label="Node A", color="red"];
            B [label="Node B", color="blue"];
            A -> B;
        }"#;
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Should contain the complex DOT string
        assert!(
            html_string.contains("digraph test"),
            "Should contain digraph declaration"
        );
        assert!(
            html_string.contains("node [shape=box]"),
            "Should contain node attributes"
        );
        assert!(
            html_string.contains("Node A"),
            "Should contain Node A label"
        );
        assert!(
            html_string.contains("Node B"),
            "Should contain Node B label"
        );
        assert!(
            html_string.contains("color=\"red\""),
            "Should contain red color"
        );
        assert!(
            html_string.contains("color=\"blue\""),
            "Should contain blue color"
        );
    }

    /// Test render_dot with empty DOT string
    #[test]
    fn test_render_dot_empty() {
        let dot_string = "";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Parse HTML and verify structure still exists
        let document = Html::parse_fragment(&html_string);

        // Should still have the div and script structure
        let div_selector = Selector::parse("div#graph").unwrap();
        let div = document.select(&div_selector).next();
        assert!(
            div.is_some(),
            "Should have div with id='graph' even with empty DOT"
        );

        // Should still have script tags
        let script_selector = Selector::parse("script").unwrap();
        let scripts: Vec<_> = document.select(&script_selector).collect();
        assert_eq!(
            scripts.len(),
            2,
            "Should have 2 script tags (library + inline)"
        );

        // Should contain empty DOT string in JavaScript
        assert!(
            html_string.contains("renderSVGElement(`  `)"),
            "Should handle empty DOT string"
        );
    }

    /// Test render_dot with special characters that need escaping
    #[test]
    fn test_render_dot_special_characters() {
        let dot_string = r#"digraph test { A [label="Quote\"Test"]; }"#;
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Should contain the DOT string with special characters
        assert!(
            html_string.contains("Quote\\\"Test"),
            "Should contain escaped quotes"
        );
        assert!(
            html_string.contains("digraph test"),
            "Should contain basic structure"
        );
    }

    /// Test render_dot HTML structure validation
    #[test]
    fn test_render_dot_html_structure() {
        let dot_string = "digraph { A -> B; }";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Parse HTML and validate complete structure
        let document = Html::parse_fragment(&html_string);

        // Should have exactly one div with id="graph"
        let div_selector = Selector::parse("div#graph").unwrap();
        let divs: Vec<_> = document.select(&div_selector).collect();
        assert_eq!(divs.len(), 1, "Should have exactly one div with id='graph'");

        // Div should be empty (content will be added by JavaScript)
        let graph_div = &divs[0];
        assert_eq!(
            graph_div.inner_html().trim(),
            "",
            "Graph div should be empty initially"
        );

        // Should have exactly 2 script tags
        let script_selector = Selector::parse("script").unwrap();
        let scripts: Vec<_> = document.select(&script_selector).collect();
        assert_eq!(scripts.len(), 2, "Should have exactly 2 script tags");

        // First script should load the library
        let lib_script = &scripts[0];
        assert_eq!(
            lib_script.attr("src"),
            Some("/assets/viz/viz-global.js"),
            "First script should load viz-js library"
        );
        assert!(
            lib_script.inner_html().is_empty(),
            "Library script should have no inline content"
        );

        // Second script should contain the JavaScript code
        let inline_script = &scripts[1];
        assert!(
            inline_script.attr("src").is_none(),
            "Second script should not have src attribute"
        );
        assert!(
            !inline_script.inner_html().is_empty(),
            "Inline script should have content"
        );
        assert!(
            inline_script.inner_html().contains("Viz.instance()"),
            "Should contain Viz.instance() call"
        );
    }

    /// Test render_dot JavaScript code generation
    #[test]
    fn test_render_dot_javascript_content() {
        let dot_string = "digraph test { node1 -> node2; }";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Verify JavaScript code structure
        assert!(
            html_string.contains("Viz.instance().then(function(viz) {"),
            "Should start Viz promise"
        );
        assert!(
            html_string.contains("var svg = viz.renderSVGElement(`"),
            "Should declare svg variable"
        );
        assert!(
            html_string.contains("document.getElementById(\"graph\").appendChild(svg);"),
            "Should append to graph"
        );
        assert!(html_string.contains("});"), "Should close promise handler");

        // Verify DOT string is properly embedded
        assert!(
            html_string.contains("digraph test { node1 -> node2; }"),
            "Should embed DOT string"
        );
    }

    /// Test render_dot with whitespace in DOT string
    #[test]
    fn test_render_dot_whitespace() {
        let dot_string = "  digraph   test   {   A   ->   B   }  ";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Should preserve whitespace in DOT string
        assert!(
            html_string.contains(dot_string),
            "Should preserve whitespace in DOT string"
        );
        assert!(
            html_string.contains("Viz.instance()"),
            "Should still generate valid JavaScript"
        );
    }

    /// Test render_dot output format consistency
    #[test]
    fn test_render_dot_output_format() {
        let dot_string = "digraph { A; }";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Verify overall HTML structure pattern
        assert!(
            html_string.starts_with("<div"),
            "Should start with div element"
        );
        assert!(
            html_string.ends_with("</script>"),
            "Should end with script closing tag"
        );
        assert!(
            html_string.contains("id=\"graph\""),
            "Should contain graph id"
        );
        assert!(
            html_string.contains("/assets/viz/viz-global.js"),
            "Should reference viz library"
        );

        // Should not contain any obvious HTML encoding issues
        assert!(
            !html_string.contains("&lt;"),
            "Should not double-encode HTML"
        );
        assert!(
            !html_string.contains("&gt;"),
            "Should not double-encode HTML"
        );
    }

    /// Test render_dot with backtick character escaping
    #[test]
    fn test_render_dot_backtick_escaping() {
        let dot_string = "digraph test { A [label=`backtick`]; B [label=\"normal\"]; A -> B; }";
        let result = render_dot(dot_string);
        let html_string = result.into_string();

        // Parse HTML and verify structure
        let document = Html::parse_fragment(&html_string);

        // Should contain div with id="graph"
        let div_selector = Selector::parse("div#graph").unwrap();
        let div = document.select(&div_selector).next();
        assert!(div.is_some(), "Should have div with id='graph'");

        // Should contain script tag for viz-js library
        let script_selector = Selector::parse("script[src]").unwrap();
        let script_tag = document.select(&script_selector).next();
        assert!(
            script_tag.is_some(),
            "Should have script tag with src attribute"
        );

        // Should contain inline script with escaped backticks
        assert!(
            html_string.contains("Viz.instance().then"),
            "Should contain Viz.instance() call"
        );
        assert!(
            html_string.contains("renderSVGElement"),
            "Should contain renderSVGElement call"
        );

        // Verify backticks are properly escaped in the JavaScript template literal
        assert!(
            html_string.contains("label=\\`backtick\\`"),
            "Should escape backticks in DOT string to prevent breaking JavaScript template literal"
        );

        // Verify that the original unescaped backticks from the DOT string are not present
        assert!(
            !html_string.contains("label=`backtick`"),
            "Should not contain unescaped backticks from DOT string that could break JavaScript"
        );

        // Verify the escaped DOT string is within the template literal
        assert!(
            html_string.contains("renderSVGElement(` digraph test { A [label=\\`backtick\\`]; B [label=\"normal\"]; A -> B; } `)"),
            "Should contain the properly escaped DOT string within template literal"
        );

        // Verify the DOT string structure is preserved (just with escaped backticks)
        assert!(
            html_string.contains("digraph test"),
            "Should preserve DOT string structure"
        );
        assert!(
            html_string.contains("label=\"normal\""),
            "Should preserve normal quotes unchanged"
        );
        assert!(
            html_string.contains("A -> B"),
            "Should preserve graph relationships"
        );
    }

    // =============== Index Page Tests ===============

    /// Helper function to setup test datasets for index page testing
    async fn setup_test_datasets_for_index(
        tx: &mut Transaction<'_, Postgres>,
        count: usize,
    ) -> Vec<DatasetId> {
        let mut dataset_ids = Vec::new();

        for _i in 0..count {
            let dataset_id = Uuid::new_v4();

            let plan_param = PlanParam {
                dataset: DatasetParam {
                    id: dataset_id,
                    extra: None,
                },
                data_products: vec![],
                dependencies: vec![],
            };

            plan_param.upsert(tx, "testuser", Utc::now()).await.unwrap();

            dataset_ids.push(dataset_id);
        }

        dataset_ids
    }

    // =============== Index Page Tests ===============

    /// Test index_page endpoint using TestClient
    #[sqlx::test]
    async fn test_index_page_endpoint(pool: PgPool) {
        // Setup test data
        let mut tx = pool.begin().await.unwrap();
        setup_test_datasets_for_index(&mut tx, 2).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to index page endpoint
        let response: TestResponse = cli.get("/").send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Verify we get HTML response (should not be empty)
        assert!(
            !html_content.is_empty(),
            "Response should contain HTML content"
        );

        // Parse HTML and verify basic structure
        let document = Html::parse_fragment(&html_content);

        // Should contain the title "Fletcher: Search"
        let title_selector = Selector::parse("title").unwrap();
        let title = document.select(&title_selector).next();
        assert!(title.is_some(), "Should have title element");
        assert_eq!(title.unwrap().inner_html(), "Fletcher");

        // Should contain the search input
        let input_selector = Selector::parse("input#search_by_input").unwrap();
        let input = document.select(&input_selector).next();
        assert!(
            input.is_some(),
            "Should have search input element in full page"
        );

        // Should contain the results table
        let table_selector = Selector::parse("table").unwrap();
        let table = document.select(&table_selector).next();
        assert!(table.is_some(), "Should have results table in full page");

        // Should have navigation with only Search link (no dataset_id)
        let nav_selector = Selector::parse("nav ul li").unwrap();
        let nav_items: Vec<_> = document.select(&nav_selector).collect();
        assert_eq!(
            nav_items.len(),
            1,
            "Should have only 1 nav item (Search only, no Plan link)"
        );

        let search_link = nav_items[0].select(&Selector::parse("a").unwrap()).next();
        assert!(search_link.is_some());
        assert_eq!(search_link.unwrap().inner_html(), "Search");
    }

    /// Test index_page endpoint with detailed HTML structure validation
    #[sqlx::test]
    async fn test_index_page_html_structure_detailed(pool: PgPool) {
        // Setup test data
        let mut tx = pool.begin().await.unwrap();
        setup_test_datasets_for_index(&mut tx, 3).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to index page endpoint
        let response: TestResponse = cli.get("/").send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Parse HTML and verify structure
        let document = Html::parse_fragment(&html_content);

        // Should contain the search fieldset
        let fieldset_selector = Selector::parse("fieldset").unwrap();
        let fieldset = document.select(&fieldset_selector).next();
        assert!(fieldset.is_some(), "Should have fieldset element");

        // Should contain the legend
        let legend_selector = Selector::parse("legend").unwrap();
        let legend = document.select(&legend_selector).next();
        assert!(legend.is_some(), "Should have legend element");
        assert_eq!(legend.unwrap().inner_html(), "Search by:");

        // Should contain the search input with correct attributes
        let input_selector = Selector::parse("input#search_by_input").unwrap();
        let input = document.select(&input_selector).next();
        assert!(input.is_some(), "Should have search input element");

        let input_element = input.unwrap();
        assert_eq!(input_element.attr("name"), Some("search_by"));
        assert_eq!(input_element.attr("class"), Some("input"));
        assert_eq!(input_element.attr("type"), Some("search"));
        assert_eq!(
            input_element.attr("placeholder"),
            Some("Begin typing to search Plans...")
        );
        assert_eq!(
            input_element.attr("hx-get"),
            Some("/component/plan_search?page=0")
        );
        assert_eq!(
            input_element.attr("hx-trigger"),
            Some("input changed delay:500ms, keyup[key=='Enter']")
        );
        assert_eq!(input_element.attr("hx-target"), Some("#search_results"));
        assert_eq!(input_element.attr("hx-swap"), Some("innerHTML"));

        // Should contain the results table
        let table_selector = Selector::parse("table").unwrap();
        let table = document.select(&table_selector).next();
        assert!(table.is_some(), "Should have results table");

        // Should contain table headers
        let th_selector = Selector::parse("th").unwrap();
        let headers: Vec<_> = document.select(&th_selector).collect();
        assert_eq!(headers.len(), 3, "Should have 3 table headers");
        assert_eq!(headers[0].inner_html(), "Dataset ID");
        assert_eq!(headers[1].inner_html(), "Modified Date");
        assert_eq!(headers[2].inner_html(), "Extra");

        // Should contain search results tbody
        let tbody_selector = Selector::parse("tbody#search_results").unwrap();
        let tbody = document.select(&tbody_selector).next();
        assert!(tbody.is_some(), "Should have search_results tbody");

        // Should contain some table rows from the pre-rendered search component
        let tr_selector = Selector::parse("tbody#search_results tr").unwrap();
        let rows: Vec<_> = document.select(&tr_selector).collect();
        assert_eq!(rows.len(), 3, "Should have 3 rows from test datasets");
    }

    /// Test index_page with empty database
    #[sqlx::test]
    async fn test_index_page_empty_database(pool: PgPool) {
        // Don't create any test data - test with empty database

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to index page endpoint
        let response: TestResponse = cli.get("/").send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Parse HTML and verify structure still works with no data
        let document = Html::parse_fragment(&html_content);

        // Should still contain the search form
        let input_selector = Selector::parse("input#search_by_input").unwrap();
        let input = document.select(&input_selector).next();
        assert!(
            input.is_some(),
            "Should have search input even with empty database"
        );

        // Should still contain the table structure
        let tbody_selector = Selector::parse("tbody#search_results").unwrap();
        let tbody = document.select(&tbody_selector).next();
        assert!(
            tbody.is_some(),
            "Should have search_results tbody even with empty database"
        );

        // Should have no data rows
        let tr_selector = Selector::parse("tbody#search_results tr").unwrap();
        let rows: Vec<_> = document.select(&tr_selector).collect();
        assert_eq!(rows.len(), 0, "Should have 0 rows with empty database");
    }

    /// Test index_page HTML structure matches expected layout
    #[sqlx::test]
    async fn test_index_page_html_content_validation(pool: PgPool) {
        // Setup minimal test data
        let mut tx = pool.begin().await.unwrap();
        setup_test_datasets_for_index(&mut tx, 1).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to index page endpoint
        let response: TestResponse = cli.get("/").send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Verify specific HTML structure (page title with spans)
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Fletcher: </span>"),
            "Should contain Fletcher prefix span"
        );
        assert!(
            html_content.contains("<span class=\"animate-fade bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Search</span>"),
            "Should contain Search title span"
        );
        assert!(
            html_content.contains("Search by:"),
            "Should contain fieldset legend"
        );
        assert!(
            html_content.contains("Begin typing to search Plans..."),
            "Should contain input placeholder"
        );
        assert!(
            html_content.contains("hx-get=\"/component/plan_search?page=0\""),
            "Should contain HTMX get attribute"
        );
        assert!(
            html_content.contains("hx-target=\"#search_results\""),
            "Should contain HTMX target attribute"
        );
        assert!(
            html_content.contains("Dataset ID"),
            "Should contain Dataset ID header"
        );
        assert!(
            html_content.contains("Modified Date"),
            "Should contain Modified Date header"
        );
        assert!(
            html_content.contains("id=\"search_results\""),
            "Should contain search_results tbody id"
        );
    }

    // =============== Plan Page Tests ===============

    /// Helper function to create a test plan with data products in the database
    async fn setup_test_plan_with_data_products(
        tx: &mut Transaction<'_, Postgres>,
        dataset_id: DatasetId,
        data_product_count: usize,
    ) -> Plan {
        let mut data_products = Vec::new();
        let mut dependencies = Vec::new();

        // Create data products with different states
        for i in 0..data_product_count {
            let data_product_id = Uuid::new_v4();

            data_products.push(DataProductParam {
                id: data_product_id,
                compute: if i % 2 == 0 {
                    Compute::Cams
                } else {
                    Compute::Dbxaas
                },
                name: format!("test-product-{i}"),
                version: format!("1.{i}.0"),
                eager: i % 2 == 0,
                passthrough: Some(json!({"test": format!("passthrough-{i}")})),
                extra: Some(json!({"test": format!("extra-{i}")})),
            });

            // Create dependencies between consecutive data products
            if i > 0 {
                dependencies.push(DependencyParam {
                    parent_id: data_products[i - 1].id,
                    child_id: data_product_id,
                    extra: Some(json!({"dependency": i})),
                });
            }
        }

        let plan_param = PlanParam {
            dataset: DatasetParam {
                id: dataset_id,
                extra: Some(json!({"test": "plan"})),
            },
            data_products,
            dependencies,
        };

        plan_param.upsert(tx, "testuser", Utc::now()).await.unwrap()
    }

    // =============== Plan Page Tests ===============

    /// Test plan_page endpoint with valid dataset_id
    #[sqlx::test]
    async fn test_plan_page_success(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 3).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Verify we get HTML response (should not be empty)
        assert!(
            !html_content.is_empty(),
            "Response should contain HTML content"
        );

        // Parse HTML and verify basic structure
        let document = Html::parse_fragment(&html_content);

        // Should contain the title "Fletcher: Plan"
        let title_selector = Selector::parse("title").unwrap();
        let title = document.select(&title_selector).next();
        assert!(title.is_some(), "Should have title element");
        assert_eq!(title.unwrap().inner_html(), "Fletcher");

        assert!(
            html_content.contains("Dataset ID:"),
            "Should render the plan summary block"
        );
        assert!(
            html_content.contains("Plan ")
                && html_content.contains("Structure:")
                && html_content.contains("Plan ")
                && html_content.contains("Runs:"),
            "Should render the new plan structure and runs sections"
        );

        // Should have navigation with both Search and Plan links
        let nav_selector = Selector::parse("nav ul li").unwrap();
        let nav_items: Vec<_> = document.select(&nav_selector).collect();
        assert_eq!(
            nav_items.len(),
            2,
            "Should have 2 nav items (Search and Plan)"
        );

        // First nav item should be Search
        let search_link = nav_items[0].select(&Selector::parse("a").unwrap()).next();
        assert!(search_link.is_some());
        assert_eq!(search_link.unwrap().inner_html(), "Search");

        // Second nav item should be Plan
        let plan_link = nav_items[1].select(&Selector::parse("a").unwrap()).next();
        assert!(plan_link.is_some());
        assert_eq!(plan_link.unwrap().inner_html(), "Plan");
        assert_eq!(
            plan_link.unwrap().attr("href"),
            Some(format!("/plan/{dataset_id}").as_str())
        );

        let dag_selector = Selector::parse(".fletcher-dag-widget").unwrap();
        let dag_widgets: Vec<_> = document.select(&dag_selector).collect();
        assert_eq!(dag_widgets.len(), 1, "Plan page should have one DAG widget");
        assert!(
            html_content.contains("No runs recorded yet.") || html_content.contains("/run/"),
            "Should show run history content"
        );
        assert!(
            !html_content.contains("language-json"),
            "Plan JSON details block should be hidden"
        );
    }

    /// Test trigger button reruns a plan from the UI
    #[sqlx::test]
    async fn test_trigger_plan_post(pool: PgPool) {
        let dataset_id = Uuid::new_v4();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let config = load_config().unwrap();

        let mut tx = pool.begin().await.unwrap();
        let plan_param = PlanParam {
            dataset: DatasetParam {
                id: dataset_id,
                extra: Some(json!({"test":"trigger"})),
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "step-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Dbxaas,
                    name: "step-2".to_string(),
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
        plan_param
            .upsert(&mut tx, "test_user", Utc::now())
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let app = user_service().data(config).data(pool.clone());
        let cli = TestClient::new(app);
        let response = cli.post(format!("/plan/{dataset_id}/trigger")).send().await;
        response.assert_status(StatusCode::SEE_OTHER);
        let location = response
            .0
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            location.starts_with(&format!("/plan/{dataset_id}/run/")),
            "Trigger should redirect to the latest run page"
        );

        let mut verify_tx = pool.begin().await.unwrap();
        let plan = plan_read(&mut verify_tx, dataset_id).await.unwrap();
        let step_1 = plan.data_product(dp1_id).unwrap();
        let step_2 = plan.data_product(dp2_id).unwrap();
        assert_eq!(step_1.state, State::Queued);
        assert_eq!(step_2.state, State::Waiting);
        assert!(step_1.run_id.is_some());
    }

    /// Test the plan page shows the structure DAG and run history links.
    #[sqlx::test]
    async fn test_plan_page_shows_structure_and_runs(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 1).await;
        tx.commit().await.unwrap();

        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;
        response.assert_status_is_ok();

        let html_content = response.0.into_body().into_string().await.unwrap();
        assert!(
            html_content.contains("Plan ") && html_content.contains("Structure:"),
            "Plan page should show the plan structure section"
        );
        assert!(
            html_content.contains("Plan ") && html_content.contains("Runs:"),
            "Plan page should show the plan runs section"
        );
        assert!(
            html_content.contains("nice-dag-core.js"),
            "Plan page should load the DAG widget assets"
        );
        assert!(
            html_content.contains("max-h-[24rem]") && html_content.contains("overflow-y-auto"),
            "Plan page should make the run list scrollable"
        );
        assert!(
            html_content.contains("No runs recorded yet.") || html_content.contains("/run/"),
            "Plan page should show run history content"
        );
    }

    /// Test the run page renders the selected run DAG and step log pane.
    #[sqlx::test]
    async fn test_plan_run_page(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let dataset_id = Uuid::new_v4();
        let config = load_config().unwrap();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let plan_param = PlanParam {
            dataset: DatasetParam {
                id: dataset_id,
                extra: Some(json!({"test":"run-page"})),
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "step-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Dbxaas,
                    name: "step-2".to_string(),
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
        crate::core::plan_add_with_config(&mut tx, &config, &plan_param, "test_user")
            .await
            .unwrap();
        let runs = plan_runs_read(&mut tx, dataset_id).await.unwrap();
        tx.commit().await.unwrap();

        let app = user_service().data(config).data(pool.clone());
        let cli = TestClient::new(app);

        let response: TestResponse = cli
            .get(format!("/plan/{dataset_id}/run/{}", runs[0].id))
            .send()
            .await;
        response.assert_status_is_ok();

        let html_content = response.0.into_body().into_string().await.unwrap();
        let run_graph_index = html_content.find("id=\"run_graph\"").unwrap();
        let run_list_index = html_content.find("Plan </span><span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Runs:</span>").unwrap();
        assert!(
            html_content.contains("Run ") && html_content.contains("Plan:"),
            "Run page should include the run plan section"
        );
        assert!(
            html_content.contains("Step ") && html_content.contains("Logs:"),
            "Run page should include the step logs section"
        );
        assert!(
            html_content.contains("Click a plan step to view its logs."),
            "Run page should prompt for step selection"
        );
        assert!(
            run_graph_index < run_list_index,
            "Run page should keep the DAG above the run list"
        );
    }

    /// Test the run graph fragment route renders the nice-dag widget payload.
    #[sqlx::test]
    async fn test_plan_run_graph_get(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();
        let dataset_id = Uuid::new_v4();
        let config = load_config().unwrap();
        let dp1_id = Uuid::new_v4();
        let dp2_id = Uuid::new_v4();
        let plan_param = PlanParam {
            dataset: DatasetParam {
                id: dataset_id,
                extra: None,
            },
            data_products: vec![
                DataProductParam {
                    id: dp1_id,
                    compute: Compute::Cams,
                    name: "step-1".to_string(),
                    version: "1.0.0".to_string(),
                    eager: true,
                    passthrough: None,
                    extra: None,
                },
                DataProductParam {
                    id: dp2_id,
                    compute: Compute::Dbxaas,
                    name: "step-2".to_string(),
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
        crate::core::plan_add_with_config(&mut tx, &config, &plan_param, "test_user")
            .await
            .unwrap();
        let runs = plan_runs_read(&mut tx, dataset_id).await.unwrap();
        tx.commit().await.unwrap();

        let app = user_service().data(config).data(pool.clone());
        let cli = TestClient::new(app);

        let response: TestResponse = cli
            .get(format!("/plan/{dataset_id}/run/{}/graph", runs[0].id))
            .send()
            .await;
        response.assert_status_is_ok();

        let html_content = response.0.into_body().into_string().await.unwrap();
        assert!(
            html_content.contains("fletcher-dag-widget"),
            "Graph fragment should include the DAG widget wrapper"
        );
        assert!(
            html_content.contains("fletcher-dag-data"),
            "Graph fragment should include DAG JSON payload"
        );
        assert!(
            !html_content.contains("<title>Fletcher</title>"),
            "Graph fragment should not render the full page layout"
        );
    }

    /// Test plan_page endpoint with invalid UUID
    #[sqlx::test]
    async fn test_plan_page_invalid_uuid(pool: PgPool) {
        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request with invalid UUID
        let response: TestResponse = cli.get("/plan/invalid-uuid").send().await;

        // Should return 404 Not Found
        response.assert_status(poem::http::StatusCode::NOT_FOUND);
    }

    /// Test plan_page endpoint with non-existent dataset_id
    #[sqlx::test]
    async fn test_plan_page_not_found(pool: PgPool) {
        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request with valid UUID but non-existent dataset
        let non_existent_id = Uuid::new_v4();
        let response: TestResponse = cli.get(format!("/plan/{non_existent_id}")).send().await;

        // Should return 404 Not Found
        response.assert_status(poem::http::StatusCode::NOT_FOUND);
    }

    /// Test plan_page with plan containing no data products
    #[sqlx::test]
    async fn test_plan_page_empty_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data with no data products
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 0).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Parse HTML and verify structure handles empty plan
        let document = Html::parse_fragment(&html_content);

        // Should still have the plan heading
        let h2_selector = Selector::parse("h2").unwrap();
        let h2 = document.select(&h2_selector).next();
        assert!(h2.is_some(), "Should have h2 element even with empty plan");

        assert!(
            html_content.contains("Plan ")
                && html_content.contains("Structure:")
                && html_content.contains("Plan ")
                && html_content.contains("Runs:"),
            "Should render the plan structure and runs sections"
        );

        assert!(
            document
                .root_element()
                .html()
                .contains("No runs recorded yet."),
            "Should show an empty run history message"
        );

        // Should still have DAG widget container
        let graph_div_selector = Selector::parse("div[id^='dag_widget_']").unwrap();
        let graph_div = document.select(&graph_div_selector).next();
        assert!(
            graph_div.is_some(),
            "Should have DAG widget container even with empty plan"
        );
    }

    /// Test plan_page HTML structure with detailed validation
    #[sqlx::test]
    async fn test_plan_page_html_structure_detailed(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data with specific values for validation
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 2).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Parse HTML and verify detailed structure
        let document = Html::parse_fragment(&html_content);

        // Should contain DAG widget container for visualization
        let graph_div_selector = Selector::parse("div[id^='dag_widget_']").unwrap();
        let graph_div = document.select(&graph_div_selector).next();
        assert!(
            graph_div.is_some(),
            "Should have a DAG widget container for visualization"
        );

        // Should contain script tags for the widget assets
        let script_selector = Selector::parse("script").unwrap();
        let scripts: Vec<_> = document.select(&script_selector).collect();
        assert!(
            scripts.len() >= 2,
            "Should have at least 2 script tags for the DAG widget"
        );

        // Should have nice-dag library script
        let nice_dag_script = scripts.iter().find(|script| {
            script
                .attr("src")
                .is_some_and(|src| src.contains("nice-dag-core.js"))
        });
        assert!(
            nice_dag_script.is_some(),
            "Should have nice-dag library script"
        );

        assert!(
            html_content.contains("Plan ") && html_content.contains("Runs:"),
            "Should contain the run history section"
        );
    }

    /// Test plan_page content validation for specific elements
    #[sqlx::test]
    async fn test_plan_page_content_validation(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 1).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Verify specific content elements (page title with spans)
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Fletcher: </span>"),
            "Should contain Fletcher prefix span"
        );
        assert!(
            html_content.contains("<span class=\"animate-fade bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Plan</span>"),
            "Should contain Plan title span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Plan </span>"),
            "Should contain Plan span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Structure:</span>"),
            "Should contain Structure span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Runs:</span>"),
            "Should contain Runs span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Plan </span>"),
            "Should contain Plan span"
        );
        assert!(
            html_content.contains("No runs recorded yet.") || html_content.contains("/run/"),
            "Should contain run history content"
        );
        assert!(
            html_content.contains("test-product-0"),
            "Should contain test data product name"
        );
        assert!(
            html_content.contains("1.0.0"),
            "Should contain test data product version"
        );
        assert!(
            html_content.contains("fletcher-dag-widget"),
            "Should contain DAG widget markup"
        );
        assert!(
            html_content.contains("nice-dag-core.js"),
            "Should reference nice-dag library"
        );
        assert!(
            html_content.contains("id=\"dag_widget_"),
            "Should contain DAG widget container id"
        );

        assert!(
            !html_content.contains("class=\"language-json\""),
            "Plan details JSON block should be hidden"
        );
        assert!(
            html_content.contains("max-h-[24rem]") && html_content.contains("overflow-y-auto"),
            "Should contain a scrollable run history list"
        );
    }

    /// Test plan_page details are hidden.
    #[sqlx::test]
    async fn test_plan_page_details_hidden(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data with specific values for JSON validation
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 2).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        assert!(
            !html_content.contains("Details:"),
            "Plan details heading should be hidden"
        );
        assert!(
            !html_content.contains("language-json"),
            "Plan details JSON block should be hidden"
        );
    }

    /// Test hidden plan details with empty plan.
    #[sqlx::test]
    async fn test_plan_page_details_hidden_empty_plan(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data with no data products
        let dataset_id = Uuid::new_v4();
        setup_test_plan_with_data_products(&mut tx, dataset_id, 0).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan page endpoint
        let response: TestResponse = cli.get(format!("/plan/{dataset_id}")).send().await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        assert!(
            !html_content.contains("language-json"),
            "Hidden plan details should remain hidden for empty plans"
        );
        assert!(
            html_content.contains("No runs recorded yet."),
            "Empty plans should still show empty run history"
        );
    }
}
