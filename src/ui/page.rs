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
use poem_openapi::types::ToJSON;
use serde_json::to_string_pretty;
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
        script src="/assets/viz/viz-standalone.js" {}
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
    let plan: Plan = plan_read(&mut tx, dataset_id).await.map_err(|err| {
        let error: poem::Error = err.into_poem_error();
        match error.status() {
            // Map poem's "NotFound" states to "NotFoundError" so the 404 page comes up
            StatusCode::NOT_FOUND => NotFoundError.into(),
            _ => error,
        }
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

    // Json version of the payload as a pretty string
    let plan_json_pretty: String = match plan.to_json() {
        Some(value) => to_string_pretty(&value).unwrap_or(String::new()),
        None => String::new(),
    };

    Ok(base_layout(
        "Plan",
        &Some(dataset_id),
        html! {
            // Data Sets Stats
            div class="stats shadow animate-fade" {
                // Data Set
                div class="stat" {
                    div class="stat-title" { "Dataset ID:" }
                    div class="stat-value" { (&plan.dataset.id) }
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

            // GraphViz representation of the plan's state
            div class="ml-12 animate-fade" {
                h2 class="text-4xl mb-2" {
                    span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                        "Plan's Current "
                    }
                    span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                        "State:"
                    }
                }
                div class="ml-4" {
                    (render_dot(&dag_viz))
                }
            }

            // Data Products state details
            h2 class="ml-12 mt-10 text-4xl mb-2 animate-fade" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Data "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Products:"
                }
            }
            table class="table table-zebra table-sm animate-fade" {
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
                    @for dp in &plan.data_products {
                        // Badge style for the eager flag
                        @let eager_badge: &str = if dp.eager {
                            "badge-primary"
                        } else {
                            "badge-secondary"
                        };

                        // Badge style for the state flag
                        @let state_badge: &str = match dp.state {
                            State::Disabled => "badge-error",
                            State::Failed => "badge-error",
                            State::Queued => "badge-neutral",
                            State::Running => "badge-info",
                            State::Success => "badge-success",
                            State::Waiting => "badge-neutral",
                        };

                        tr
                            id={ "row_" (dp.id) }
                            class="hover:bg-base-300" {
                            td { (dp.id) }
                            td { (dp.compute) }
                            td { (dp.name) }
                            td { (dp.version) }
                            // Put badges into span since if the badge is at the td level, and two
                            // badge td meet, they merge into one for some reason
                            td { span class={ "badge " (eager_badge) } {
                                (dp.eager)
                            } }
                            td { span class={ "badge " (state_badge) } {
                                (dp.state)
                            } }
                            @if let Some(run_id) = &dp.run_id {
                                td { (run_id) }
                            } @else {
                                td {}
                            }
                            @if let Some(link) = &dp.link {
                                td {
                                    a href=(link) {
                                        (link)
                                    }
                                }
                            } @else {
                                td {}
                            }
                            td { (dp.modified_date) }
                        }
                    }
                }
            }
            // Json Payload for the Plan
            h2 class="ml-12 mt-10 text-4xl mb-2 animate-fade" {
                span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                    "Plan "
                }
                span class="bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                    "Details:"
                }
            }
            pre class="animate-fade" {
                code class="language-json" {
                    (plan_json_pretty)
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Compute, DataProduct, DataProductParam, Dataset, DatasetParam, DependencyParam, PlanParam,
        State,
    };
    use crate::ui::user_service;
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
            Some("/assets/viz/viz-standalone.js"),
            "Script src should point to viz-standalone.js"
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
            Some("/assets/viz/viz-standalone.js"),
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
            html_string.contains("/assets/viz/viz-standalone.js"),
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
        assert_eq!(headers.len(), 2, "Should have 2 table headers");
        assert_eq!(headers[0].inner_html(), "Dataset ID");
        assert_eq!(headers[1].inner_html(), "Modified Date");

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

        // Should contain plan heading
        let h2_selector = Selector::parse("h2").unwrap();
        let h2 = document.select(&h2_selector).next();
        assert!(h2.is_some(), "Should have h2 element");

        // Check for spans within h2
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h2.unwrap().select(&span_selector).collect();
        assert_eq!(spans.len(), 2, "h2 should have exactly 2 span elements");
        assert_eq!(spans[0].inner_html(), "Plan's Current ");
        assert_eq!(spans[1].inner_html(), "State:");

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

        // Should contain data products table
        let table_selector = Selector::parse("table").unwrap();
        let table = document.select(&table_selector).next();
        assert!(table.is_some(), "Should have data products table");

        // Should have correct table headers
        let th_selector = Selector::parse("th").unwrap();
        let headers: Vec<_> = document.select(&th_selector).collect();
        assert_eq!(headers.len(), 9, "Should have 9 table headers");
        assert_eq!(headers[0].inner_html(), "Data Product ID");
        assert_eq!(headers[1].inner_html(), "Compute");
        assert_eq!(headers[2].inner_html(), "Name");
        assert_eq!(headers[3].inner_html(), "Version");
        assert_eq!(headers[4].inner_html(), "Eager");
        assert_eq!(headers[5].inner_html(), "State");
        assert_eq!(headers[6].inner_html(), "Run ID");
        assert_eq!(headers[7].inner_html(), "Link");
        assert_eq!(headers[8].inner_html(), "Last Update");

        // Should have data product rows
        let tr_selector = Selector::parse("tbody tr").unwrap();
        let rows: Vec<_> = document.select(&tr_selector).collect();
        assert_eq!(rows.len(), 3, "Should have 3 data product rows");

        // Verify first row structure
        let first_row = &rows[0];
        let td_selector = Selector::parse("td").unwrap();
        let cells: Vec<_> = first_row.select(&td_selector).collect();
        assert_eq!(cells.len(), 9, "Each row should have 9 cells");

        // Check that cells contain expected content types
        assert!(
            !cells[0].inner_html().is_empty(),
            "Data Product ID cell should not be empty"
        );
        assert!(
            cells[2].inner_html().contains("test-product"),
            "Name cell should contain test-product"
        );
        assert!(
            cells[3].inner_html().contains("1."),
            "Version cell should contain version number"
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

        // Check for spans within h2
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h2.unwrap().select(&span_selector).collect();
        assert_eq!(spans.len(), 2, "h2 should have exactly 2 span elements");
        assert_eq!(spans[0].inner_html(), "Plan's Current ");
        assert_eq!(spans[1].inner_html(), "State:");

        // Should still have the table structure
        let table_selector = Selector::parse("table").unwrap();
        let table = document.select(&table_selector).next();
        assert!(table.is_some(), "Should have table even with empty plan");

        // Should have no data product rows
        let tr_selector = Selector::parse("tbody tr").unwrap();
        let rows: Vec<_> = document.select(&tr_selector).collect();
        assert_eq!(
            rows.len(),
            0,
            "Should have 0 data product rows for empty plan"
        );

        // Should still have visualization div
        let graph_div_selector = Selector::parse("div#graph").unwrap();
        let graph_div = document.select(&graph_div_selector).next();
        assert!(
            graph_div.is_some(),
            "Should have visualization div even with empty plan"
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

        // Should contain div with id="graph" for GraphViz visualization
        let graph_div_selector = Selector::parse("div#graph").unwrap();
        let graph_div = document.select(&graph_div_selector).next();
        assert!(
            graph_div.is_some(),
            "Should have div with id='graph' for visualization"
        );

        // Should contain script tags for viz-js
        let script_selector = Selector::parse("script").unwrap();
        let scripts: Vec<_> = document.select(&script_selector).collect();
        assert!(
            scripts.len() >= 2,
            "Should have at least 2 script tags for visualization"
        );

        // Should have viz-js library script
        let viz_script = scripts.iter().find(|script| {
            script
                .attr("src")
                .is_some_and(|src| src.contains("viz-standalone.js"))
        });
        assert!(viz_script.is_some(), "Should have viz-js library script");

        // Should contain table with tbody for data products
        let tbody_selector = Selector::parse("table tbody").unwrap();
        let tbody = document.select(&tbody_selector).next();
        assert!(tbody.is_some(), "Should have tbody in data products table");

        // Verify data product rows have correct structure
        let tr_selector = Selector::parse("tbody tr").unwrap();
        let rows: Vec<_> = document.select(&tr_selector).collect();

        for (i, row) in rows.iter().enumerate() {
            // Each row should have id attribute
            assert!(
                row.attr("id").is_some(),
                "Each row should have id attribute"
            );
            assert!(
                row.attr("id").unwrap().starts_with("row_"),
                "Row id should start with 'row_'"
            );

            // Each row should have 9 cells
            let td_selector = Selector::parse("td").unwrap();
            let cells: Vec<_> = row.select(&td_selector).collect();
            assert_eq!(cells.len(), 9, "Row {} should have 9 cells", i);
        }
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
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Plan's Current </span>"),
            "Should contain Plan's Current span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">State:</span>"),
            "Should contain State span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Data </span>"),
            "Should contain Data span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Products:</span>"),
            "Should contain Products span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent\">Plan </span>"),
            "Should contain Plan span"
        );
        assert!(
            html_content.contains("<span class=\"bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent\">Details:</span>"),
            "Should contain Details span"
        );
        assert!(
            html_content.contains("Data Product ID"),
            "Should contain table headers"
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
            html_content.contains("Viz.instance()"),
            "Should contain visualization JavaScript"
        );
        assert!(
            html_content.contains("viz-standalone.js"),
            "Should reference viz-js library"
        );
        assert!(
            html_content.contains("id=\"graph\""),
            "Should contain graph div id"
        );

        // Verify JSON rendering
        assert!(
            html_content.contains("class=\"language-json\""),
            "Should contain Prism.js JSON language class"
        );
        assert!(
            html_content.contains("<pre class=\"animate-fade\"><code class=\"language-json\">"),
            "Should contain properly formatted JSON code block with animation class"
        );
    }

    /// Test plan_page JSON rendering functionality
    #[sqlx::test]
    async fn test_plan_page_json_rendering(pool: PgPool) {
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

        // Parse HTML and verify JSON rendering structure
        let document = Html::parse_fragment(&html_content);

        // Should contain "Plan Details:" heading with spans
        let h2_selector = Selector::parse("h2").unwrap();
        let headings: Vec<_> = document.select(&h2_selector).collect();

        // Find the heading that contains "Plan " and "Details:" in spans
        let plan_details_heading = headings.iter().find(|h2| {
            let span_selector = Selector::parse("span").unwrap();
            let spans: Vec<_> = h2.select(&span_selector).collect();
            spans.len() == 2
                && spans[0].inner_html() == "Plan "
                && spans[1].inner_html() == "Details:"
        });
        assert!(
            plan_details_heading.is_some(),
            "Should have 'Plan Details:' heading with proper span structure"
        );

        // Should contain pre > code structure with language-json class
        let pre_code_selector = Selector::parse("pre code.language-json").unwrap();
        let json_code_block = document.select(&pre_code_selector).next();
        assert!(
            json_code_block.is_some(),
            "Should have pre > code.language-json structure"
        );

        // Get the JSON content from the code block
        let json_content = json_code_block.unwrap().inner_html();
        assert!(!json_content.is_empty(), "JSON content should not be empty");

        // Verify JSON structure contains expected plan elements
        assert!(
            json_content.contains("dataset"),
            "JSON should contain dataset object"
        );
        assert!(
            json_content.contains("data_products"),
            "JSON should contain data_products array"
        );
        assert!(
            json_content.contains("dependencies"),
            "JSON should contain dependencies array"
        );

        // Verify dataset fields are present
        assert!(
            json_content.contains("\"id\""),
            "JSON should contain dataset id field"
        );
        assert!(
            json_content.contains("\"paused\""),
            "JSON should contain paused field"
        );
        assert!(
            json_content.contains("\"modified_by\""),
            "JSON should contain modified_by field"
        );
        assert!(
            json_content.contains("\"modified_date\""),
            "JSON should contain modified_date field"
        );

        // Verify data product fields are present
        assert!(
            json_content.contains("test-product-0"),
            "JSON should contain first data product name"
        );
        assert!(
            json_content.contains("test-product-1"),
            "JSON should contain second data product name"
        );
        assert!(
            json_content.contains("\"compute\""),
            "JSON should contain compute field"
        );
        assert!(
            json_content.contains("\"version\""),
            "JSON should contain version field"
        );
        assert!(
            json_content.contains("\"eager\""),
            "JSON should contain eager field"
        );
        assert!(
            json_content.contains("\"state\""),
            "JSON should contain state field"
        );

        // Verify JSON is properly formatted (should contain indentation/whitespace)
        assert!(
            json_content.contains("\n"),
            "JSON should be pretty-printed with newlines"
        );
        assert!(json_content.contains("  "), "JSON should be indented");

        // Verify the JSON content is valid by attempting to parse it
        let parsed_json: Result<serde_json::Value, _> = serde_json::from_str(&json_content);
        assert!(parsed_json.is_ok(), "JSON content should be valid JSON");

        // Verify specific JSON structure
        let json_value = parsed_json.unwrap();
        assert!(json_value.is_object(), "Root JSON should be an object");
        assert!(
            json_value.get("dataset").is_some(),
            "Should have dataset field"
        );
        assert!(
            json_value.get("data_products").is_some(),
            "Should have data_products field"
        );
        assert!(
            json_value.get("dependencies").is_some(),
            "Should have dependencies field"
        );

        // Verify data_products is an array with correct count
        let data_products = json_value.get("data_products").unwrap();
        assert!(data_products.is_array(), "data_products should be an array");
        assert_eq!(
            data_products.as_array().unwrap().len(),
            2,
            "Should have 2 data products"
        );

        // Verify dependencies is an array
        let dependencies = json_value.get("dependencies").unwrap();
        assert!(dependencies.is_array(), "dependencies should be an array");
        assert_eq!(
            dependencies.as_array().unwrap().len(),
            1,
            "Should have 1 dependency"
        );
    }

    /// Test plan_page JSON rendering with empty plan
    #[sqlx::test]
    async fn test_plan_page_json_rendering_empty_plan(pool: PgPool) {
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

        // Parse HTML and verify JSON rendering structure for empty plan
        let document = Html::parse_fragment(&html_content);

        // Should still contain JSON structure even with empty plan
        let pre_code_selector = Selector::parse("pre code.language-json").unwrap();
        let json_code_block = document.select(&pre_code_selector).next();
        assert!(
            json_code_block.is_some(),
            "Should have JSON structure even with empty plan"
        );

        // Get the JSON content
        let json_content = json_code_block.unwrap().inner_html();
        assert!(
            !json_content.is_empty(),
            "JSON content should not be empty even with empty plan"
        );

        // Verify JSON is valid and contains expected structure
        let parsed_json: Result<serde_json::Value, _> = serde_json::from_str(&json_content);
        assert!(
            parsed_json.is_ok(),
            "JSON content should be valid JSON for empty plan"
        );

        let json_value = parsed_json.unwrap();
        assert!(
            json_value.get("dataset").is_some(),
            "Should have dataset field"
        );
        assert!(
            json_value.get("data_products").is_some(),
            "Should have data_products field"
        );

        // Verify empty arrays
        let data_products = json_value.get("data_products").unwrap();
        assert!(data_products.is_array(), "data_products should be an array");
        assert_eq!(
            data_products.as_array().unwrap().len(),
            0,
            "Should have 0 data products"
        );

        let dependencies = json_value.get("dependencies").unwrap();
        assert!(dependencies.is_array(), "dependencies should be an array");
        assert_eq!(
            dependencies.as_array().unwrap().len(),
            0,
            "Should have 0 dependencies"
        );
    }
}
