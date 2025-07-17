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
            // Prism.js
            link rel="stylesheet" type="text/css" href="/assets/prism/prism.css";
            script defer src="/assets/prism/prism.js" {}
            script defer src="/assets/prism/prism-json.js" {}
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

    /// Test that Prism.js CSS is present in the head
    #[test]
    fn test_prism_css_present() {
        let head = Html::parse_fragment(&head().into_string());

        let link_selector = Selector::parse("link[rel=\"stylesheet\"]").unwrap();
        let prism_css_link = head
            .select(&link_selector)
            .find(|link| link.attr("href") == Some("/assets/prism/prism.css"));

        assert!(
            prism_css_link.is_some(),
            "Prism.js CSS should be present in head"
        );
        let link = prism_css_link.unwrap();
        assert_eq!(link.attr("type"), Some("text/css"));
        assert_eq!(link.attr("href"), Some("/assets/prism/prism.css"));
    }

    /// Test that Prism.js core script is present in the head
    #[test]
    fn test_prism_js_present() {
        let head = Html::parse_fragment(&head().into_string());

        let script_selector = Selector::parse("script[src]").unwrap();
        let prism_js_script = head
            .select(&script_selector)
            .find(|script| script.attr("src") == Some("/assets/prism/prism.js"));

        assert!(
            prism_js_script.is_some(),
            "Prism.js core script should be present in head"
        );
        let script = prism_js_script.unwrap();
        assert!(
            script.attr("defer").is_some(),
            "Prism.js script should have defer attribute"
        );
        assert_eq!(script.attr("src"), Some("/assets/prism/prism.js"));
    }

    /// Test that Prism.js JSON plugin script is present in the head
    #[test]
    fn test_prism_json_present() {
        let head = Html::parse_fragment(&head().into_string());

        let script_selector = Selector::parse("script[src]").unwrap();
        let prism_json_script = head
            .select(&script_selector)
            .find(|script| script.attr("src") == Some("/assets/prism/prism-json.js"));

        assert!(
            prism_json_script.is_some(),
            "Prism.js JSON plugin should be present in head"
        );
        let script = prism_json_script.unwrap();
        assert!(
            script.attr("defer").is_some(),
            "Prism.js JSON script should have defer attribute"
        );
        assert_eq!(script.attr("src"), Some("/assets/prism/prism-json.js"));
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
        assert_eq!(
            li_elements.len(),
            2,
            "Breadcrumb should contain 2 list items"
        );

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
        assert_eq!(
            li_elements.len(),
            1,
            "Single-item breadcrumb should contain 1 list item"
        );
    }

    /// Test page_title generates proper h1 element with title
    #[test]
    fn test_page_title_basic() {
        let title = "Dashboard";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        // Should have h1 element
        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        assert_eq!(
            h1_element.inner_html(),
            "Fletcher: Dashboard",
            "Page title should include 'Fletcher: ' prefix and the provided title"
        );
    }

    /// Test page_title with empty string
    #[test]
    fn test_page_title_empty_string() {
        let title = "";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        assert_eq!(
            h1_element.inner_html(),
            "Fletcher: ",
            "Page title should handle empty title gracefully"
        );
    }

    /// Test page_title with special characters
    #[test]
    fn test_page_title_special_characters() {
        let title = "Data & Analytics <Report>";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        assert_eq!(
            h1_element.inner_html(),
            "Fletcher: Data &amp; Analytics &lt;Report&gt;",
            "Page title should properly escape HTML special characters"
        );
    }

    /// Test page_title with long title
    #[test]
    fn test_page_title_long_title() {
        let title = "This is a very long page title that should still be handled correctly by the page_title function";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        let expected = format!("Fletcher: {title}");
        assert_eq!(
            h1_element.inner_html(),
            expected,
            "Page title should handle long titles correctly"
        );
    }

    /// Test page_title with numeric and mixed content
    #[test]
    fn test_page_title_mixed_content() {
        let title = "Plan 42 - Version 1.2.3";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        assert_eq!(
            h1_element.inner_html(),
            "Fletcher: Plan 42 - Version 1.2.3",
            "Page title should handle mixed alphanumeric content correctly"
        );
    }

    /// Test page_title always generates exactly one h1 element
    #[test]
    fn test_page_title_single_h1() {
        let title = "Test Title";
        let page_title_markup = page_title(title);
        let page_title_html = page_title_markup.into_string();
        let document = Html::parse_fragment(&page_title_html);

        let h1_selector = Selector::parse("h1").unwrap();
        let h1_count = document.select(&h1_selector).count();
        assert_eq!(
            h1_count, 1,
            "Page title should generate exactly one h1 element"
        );
    }
}
