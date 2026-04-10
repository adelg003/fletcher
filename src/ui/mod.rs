mod component;
mod error;
mod layout;
mod page;
mod snippet;

use crate::ui::page::{
    index_page, plan_page, plan_run_graph_get, plan_run_logs_get, plan_run_nodes_get,
    plan_run_page, trigger_plan_post,
};
pub use crate::ui::{component::plan_search_get, error::not_found_404};
use poem::{Route, get, post};

/// Router for UI
pub fn user_service() -> Route {
    Route::new()
        .at("/", get(index_page))
        .at("/component/plan_search", get(plan_search_get))
        .at("/plan/:dataset_id", get(plan_page))
        .at("/plan/:dataset_id/run/:plan_run_id", get(plan_run_page))
        .at(
            "/plan/:dataset_id/run/:plan_run_id/nodes",
            get(plan_run_nodes_get),
        )
        .at(
            "/plan/:dataset_id/run/:plan_run_id/graph",
            get(plan_run_graph_get),
        )
        .at(
            "/plan/:dataset_id/run/:plan_run_id/logs",
            get(plan_run_logs_get),
        )
        .at("/plan/:dataset_id/trigger", post(trigger_plan_post))
}
