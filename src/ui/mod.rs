mod component;
mod error;
mod layout;
mod page;
mod snippet;

use crate::ui::page::{index_page, plan_page};
pub use crate::ui::{component::plan_search_get, error::not_found_404};
use poem::{Route, get};

/// Router for UI
pub fn user_service() -> Route {
    Route::new()
        .at("/", get(index_page))
        .at("/component/plan_search", get(plan_search_get))
        .at("/plan/:dataset_id", get(plan_page))
}
