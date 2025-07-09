use crate::ui::{component::plan_search_component, layout::base_layout};
use maud::{Markup, html};
use poem::{Result, error::InternalServerError, handler, web::Data};
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
