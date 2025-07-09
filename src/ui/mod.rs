mod error;
mod layout;
mod page;
mod snippet;

pub use crate::ui::error::not_found_404;
use crate::ui::page::index;
use poem::{get, Route};

/// Router for UI
pub fn route() -> Route {
    Route::new().at("/", get(index))
    //.at("/component/plan_search", get(plan_search_get))
    //.at("/component/plan_graph", get(plan_graph_get))
    //.at("/plan/:dataset_id", get(plan))
}
