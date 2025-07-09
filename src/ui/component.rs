use crate::{core::plan_search_read, model::Search};
use maud::{Markup, html};
use poem::{
    Result,
    error::InternalServerError,
    handler,
    web::{Data, Query},
};
use serde::Deserialize;
use sqlx::{PgPool, Postgres, Transaction};

/// Data for Plan Search Web Component
pub async fn plan_search_component(
    tx: &mut Transaction<'_, Postgres>,
    search_by: &str,
    page: u32,
) -> Result<Markup> {
    // Search for anything that meets our criteria
    let plans: Vec<Search> = plan_search_read(tx, search_by, page).await?;

    // Page number for the next page if it is valid
    let maybe_next_page: u32 = page.saturating_add(1);

    // More Systems on next page?
    let more_plans: Vec<Search> = plan_search_read(tx, search_by, maybe_next_page).await?;

    let next_page: Option<u32> = if more_plans.is_empty() {
        None
    } else {
        Some(maybe_next_page)
    };

    Ok(html! {
        // One Row per Plan returned
        @for plan in plans {
            // Pre-compute / formate some values
            @let modified_date: String = match plan.modified_date {
                Some(modified_date) => modified_date.to_string(),
                None => "".to_string()
            };

            tr
                id={ "row_" (plan.dataset_id) }
                href={ "/plan/" (plan.dataset_id) }
                onclick={ "window.location='/plan/" (plan.dataset_id) "';" } {
                td { (plan.dataset_id) }
                td { (modified_date) }
            }
        }
        // Pagination Placeholder
        @if let Some(next_page) = next_page {
            tr
                id="search_pagination"
                hx-get={ "/component/plan_search?search_by=" (search_by) r"&page=" (next_page) }
                hx-trigger="revealed"
                hx-swap="outerHTML"
                hx-target="#search_pagination" {
            }
        }
    })
}

/// Parameters to search by
#[derive(Deserialize)]
struct SearchParams {
    search_by: String,
    page: u32,
}

/// Web Component to search for your plans
#[handler]
pub async fn plan_search_get(
    Data(pool): Data<&PgPool>,
    Query(params): Query<SearchParams>,
) -> Result<Markup> {
    // Start Transaction
    let mut tx: Transaction<'_, Postgres> = pool.begin().await.map_err(InternalServerError)?;

    // Render component
    plan_search_component(&mut tx, &params.search_by, params.page).await
}
