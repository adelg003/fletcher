use crate::model::DatasetId;
use maud::{Markup, html};

/// HTML Page Head
pub fn head() -> Markup {
    html! {
        head {
            title { "Fletcher" }
            meta name="description" content="The OaaS Conductor of Data Products";
            meta name="keywords" content="Conductor, Data Product, Dataset, Fletcher, OaaS, Search";
            meta name="viewport" content="width=device-width, initial-scale=1.0";
            link rel="icon" type="image/x-icon" href="/assets/images/favicon.ico";
            // HTMX
            script defer src="/assets/htmx/htmx.min.js" {}
        }
    }
}

/// NavBar
fn navbar(dataset_id: &Option<DatasetId>) -> Markup {
    html! {
        nav {
            ul {
              li { a href="/" { "Search" } }
                @if let Some(dataset_id) = dataset_id {
                    li { a href={ "/plan/" (dataset_id) } { "Plan" } }
                }
            }
        }
    }
}

// Page Title
fn page_title(title: &str) -> Markup {
    html! {
        h1 {
            "Fletcher: " (title)
        }
    }
}

/// Header at the top of every page
pub fn header(title: &str, dataset_id: &Option<DatasetId>) -> Markup {
    html! {
        header {
            (navbar(dataset_id))
            (page_title(title))
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use scraper::{ElementRef, Html, Selector};
    use uuid::Uuid;

    /// Test that HTMX is present in the head
    #[test]
    fn test_htmx_present() {
        let head = Html::parse_fragment(&head().into_string());

        let script_selector = Selector::parse("script").unwrap();
        dbg!(&head);
        let script: ElementRef = head.select(&script_selector).next().unwrap();

        assert!(script.attr("defer").is_some());
        assert_eq!(script.attr("src"), Some("/assets/htmx/htmx.min.js"));
    }

    /// Test navbar with dataset_id - should have second li element pointing to plan
    #[test]
    fn test_navbar_with_dataset_id() {
        let dataset_id = Uuid::new_v4();
        let navbar_markup = navbar(&Some(dataset_id));
        let navbar_html = navbar_markup.into_string();
        let document = Html::parse_fragment(&navbar_html);

        // Should have nav > ul > li structure
        let li_selector = Selector::parse("nav > ul > li").unwrap();
        let li_elements: Vec<_> = document.select(&li_selector).collect();

        // Should have exactly 2 li elements (Search and Plan)
        assert_eq!(li_elements.len(), 2);

        // Second li should contain Plan link pointing to the UUID
        let a_selector = Selector::parse("a").unwrap();
        let second_link = li_elements[1].select(&a_selector).next().unwrap();
        assert_eq!(second_link.inner_html(), "Plan");
        assert_eq!(
            second_link.attr("href"),
            Some(format!("/plan/{dataset_id}").as_str())
        );
    }

    /// Test navbar without dataset_id - should NOT have second li element
    #[test]
    fn test_navbar_without_dataset_id() {
        let navbar_markup = navbar(&None);
        let navbar_html = navbar_markup.into_string();
        let document = Html::parse_fragment(&navbar_html);

        // Should have nav > ul > li structure
        let li_selector = Selector::parse("nav > ul > li").unwrap();
        let li_elements: Vec<_> = document.select(&li_selector).collect();

        // Should have exactly 1 li element (only Search, no Plan)
        assert_eq!(li_elements.len(), 1);
    }
}
