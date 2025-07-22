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
            // Pre-compute / format some values
            @let modified_date: String = match plan.modified_date {
                Some(modified_date) => modified_date.to_string(),
                None => "".to_string()
            };

            tr
                id={ "row_" (plan.dataset_id) }
                class="hover:bg-base-300 cursor-pointer animate-fade-up"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DatasetId, DatasetParam, PlanParam};
    use crate::ui::user_service;
    use chrono::Utc;
    use maud::PreEscaped;
    use poem::{
        EndpointExt,
        test::{TestClient, TestResponse},
    };
    use pretty_assertions::assert_eq;
    use scraper::{Html, Selector};
    use sqlx::PgPool;
    use uuid::Uuid;

    /// Helper function to setup test datasets in database
    async fn setup_test_datasets(
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

    /// Test plan_search_component with less than 50 plans - no pagination
    #[sqlx::test]
    async fn test_plan_search_component_less_than_50_plans(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data - create 25 datasets within the same transaction
        setup_test_datasets(&mut tx, 25).await;

        // Call the component function within the same transaction
        let result = plan_search_component(&mut tx, "", 0).await;
        assert!(
            result.is_ok(),
            "Plan search component should execute successfully"
        );

        let markup = result.unwrap();

        // Wrap headerless HTML fragment to have a head so it plays nice with scrapper / html5ever
        let html_string = html! { table { (markup) } }.into_string();
        let document = Html::parse_fragment(&html_string);

        // Count total table rows
        let tr_selector = Selector::parse("tr").unwrap();
        let tr_elements: Vec<_> = document.select(&tr_selector).collect();

        // Should have 25 rows (one per plan), no pagination row
        assert_eq!(
            tr_elements.len(),
            25,
            "Should have exactly 25 table rows for 25 plans"
        );

        // Should NOT have search_pagination row
        let pagination_selector = Selector::parse("tr#search_pagination").unwrap();
        assert!(
            document.select(&pagination_selector).next().is_none(),
            "Should not have search_pagination row when less than 50 plans"
        );
    }

    /// Test plan_search_component with exactly 50 plans - no pagination
    #[sqlx::test]
    async fn test_plan_search_component_exactly_50_plans(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data - create exactly 50 datasets within the same transaction
        setup_test_datasets(&mut tx, 50).await;

        // Call the component function within the same transaction
        let result = plan_search_component(&mut tx, "", 0).await;
        assert!(
            result.is_ok(),
            "Plan search component should execute successfully with 50 plans"
        );

        let markup = result.unwrap();

        // Wrap headerless HTML fragment to have a head so it plays nice with scrapper / html5ever
        let html_string = html! { table { (markup) } }.into_string();
        let document = Html::parse_fragment(&html_string);

        // Count total table rows
        let tr_selector = Selector::parse("tr").unwrap();
        let tr_elements: Vec<_> = document.select(&tr_selector).collect();

        // Should have exactly 50 rows, no pagination row
        assert_eq!(
            tr_elements.len(),
            50,
            "Should have exactly 50 table rows for 50 plans"
        );

        // Should NOT have search_pagination row (since there are no more plans on next page)
        let pagination_selector = Selector::parse("tr#search_pagination").unwrap();
        assert!(
            document.select(&pagination_selector).next().is_none(),
            "Should not have search_pagination row when exactly 50 plans"
        );
    }

    /// Test plan_search_component with more than 50 plans - should have pagination
    #[sqlx::test]
    async fn test_plan_search_component_more_than_50_plans(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data - create 75 datasets (more than PAGE_SIZE) within the same transaction
        setup_test_datasets(&mut tx, 75).await;

        // Call the component function for page 0 within the same transaction
        let search_term = "test";
        let result = plan_search_component(&mut tx, search_term, 0).await;
        assert!(result.is_ok());

        let markup = result.unwrap();

        // Wrap headerless HTML fragment to have a head so it plays nice with scrapper / html5ever
        let html_string = html! { table { (markup) } }.into_string();
        let document = Html::parse_fragment(&html_string);

        // Count total table rows
        let tr_selector = Selector::parse("tr").unwrap();
        let tr_elements: Vec<_> = document.select(&tr_selector).collect();

        // Should have exactly 51 rows (50 plan rows + 1 pagination row)
        assert_eq!(
            tr_elements.len(),
            51,
            "Should have 50 plan rows + 1 pagination row"
        );

        // Should have search_pagination row
        let pagination_selector = Selector::parse("tr#search_pagination").unwrap();
        let pagination_row = document.select(&pagination_selector).next();
        assert!(
            pagination_row.is_some(),
            "Should have search_pagination row when more than 50 plans"
        );

        // Check pagination row attributes
        let pagination_element = pagination_row.unwrap();
        assert_eq!(pagination_element.attr("hx-trigger"), Some("revealed"));
        assert_eq!(pagination_element.attr("hx-swap"), Some("outerHTML"));
        assert_eq!(
            pagination_element.attr("hx-target"),
            Some("#search_pagination")
        );

        // The hx-get should point to page 1
        let hx_get = pagination_element.attr("hx-get");
        assert!(
            hx_get.is_some(),
            "Pagination element should have hx-get attribute"
        );
        assert!(
            hx_get.unwrap().contains("page=1"),
            "Pagination should link to page 1"
        );

        let expected_url = format!("/component/plan_search?search_by={search_term}&page=1");
        assert_eq!(
            hx_get,
            Some(expected_url.as_str()),
            "Pagination URL should include search term and next page number"
        );
    }

    /// Test plan_search_component table row structure
    #[sqlx::test]
    async fn test_plan_search_component_table_row_structure(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data - create 5 datasets for easy testing within the same transaction
        setup_test_datasets(&mut tx, 5).await;

        let result = plan_search_component(&mut tx, "", 0).await;
        assert!(
            result.is_ok(),
            "Plan search component should execute successfully for table row structure test"
        );

        let markup = result.unwrap();

        // Wrap headerless HTML fragment to have a head so it plays nice with scrapper / html5ever
        let html_string = html! { table { (markup) } }.into_string();
        let document = Html::parse_fragment(&html_string);

        // Check that each row has the correct structure
        let tr_selector = Selector::parse("tr").unwrap();
        let tr_elements: Vec<_> = document.select(&tr_selector).collect();

        // Should have exactly 5 rows
        assert_eq!(tr_elements.len(), 5, "Should have exactly 5 table rows");

        // Check first row structure
        let first_row = &tr_elements[0];

        // Should have id attribute with row_ prefix
        let row_id = first_row.attr("id");
        assert!(row_id.is_some(), "First row should have id attribute");
        assert!(
            row_id.unwrap().starts_with("row_"),
            "Row id should start with 'row_' prefix"
        );

        // Should have onclick attribute
        let onclick = first_row.attr("onclick");
        assert!(onclick.is_some(), "First row should have onclick attribute");
        assert!(
            onclick.unwrap().contains("window.location='/plan/"),
            "Row onclick should contain window.location navigation"
        );

        // Should have exactly 2 td elements (dataset_id and modified_date)
        let td_selector = Selector::parse("td").unwrap();
        let td_elements: Vec<_> = first_row.select(&td_selector).collect();
        assert_eq!(
            td_elements.len(),
            2,
            "Each row should have exactly 2 td elements"
        );
    }

    /// Test plan_search_get endpoint using TestClient
    #[sqlx::test]
    async fn test_plan_search_get_endpoint(pool: PgPool) {
        let mut tx = pool.begin().await.unwrap();

        // Setup test data - create 3 datasets for testing
        setup_test_datasets(&mut tx, 3).await;
        tx.commit().await.unwrap();

        // Create TestClient for UI service
        let app = user_service().data(pool.clone());
        let cli = TestClient::new(app);

        // Test GET request to plan_search endpoint
        let response: TestResponse = cli
            .get("/component/plan_search?search_by=&page=0")
            .send()
            .await;

        // Check status is OK
        response.assert_status_is_ok();

        // Get response text (HTML)
        let html_content = response.0.into_body().into_string().await.unwrap();

        // Wrap headerless HTML fragment to have a head so it plays nice with scrapper / html5ever
        let html_string = html! { table { (PreEscaped(html_content)) } }.into_string();
        let document = Html::parse_fragment(&html_string);

        // Should have table rows for our datasets
        let tr_selector = Selector::parse("tr").unwrap();
        let tr_elements: Vec<_> = document.select(&tr_selector).collect();

        // Verify we have exactly 3 tr elements (one for each dataset created)
        assert_eq!(
            tr_elements.len(),
            3,
            "Should have exactly 3 tr elements for the 3 test datasets"
        );
    }
}
