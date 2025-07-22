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
            // TailwindCSS
            link rel="stylesheet" type="text/css" href="/assets/tailwindcss/tailwind.css";
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
        nav class="breadcrumbs ml-8" {
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
        h1 class="text-6xl ml-8 pb-2" {
            span class="bg-gradient-to-r from-orange-700 to-amber-600 bg-clip-text text-transparent" {
                "Fletcher: "
            }
            span class="animate-fade bg-gradient-to-r from-amber-600 to-amber-400 bg-clip-text text-transparent" {
                (title)
            }
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

    /// Test that TailwindCSS is present in the head
    #[test]
    fn test_tailwindcss_present() {
        let head = Html::parse_fragment(&head().into_string());

        let link_selector = Selector::parse("link[rel=\"stylesheet\"]").unwrap();
        let tailwind_css_link = head
            .select(&link_selector)
            .find(|link| link.attr("href") == Some("/assets/tailwindcss/tailwind.css"));

        assert!(
            tailwind_css_link.is_some(),
            "tailwind.css should be present in head"
        );
        let link = tailwind_css_link.unwrap();
        assert_eq!(link.attr("type"), Some("text/css"));
        assert_eq!(link.attr("href"), Some("/assets/tailwindcss/tailwind.css"));
    }

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
        // Check spans within h1
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h1_element.select(&span_selector).collect();
        assert_eq!(
            spans.len(),
            2,
            "Page title should have exactly 2 span elements"
        );

        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain 'Fletcher: ' prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            "Dashboard",
            "Second span should contain the provided title"
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
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h1_element.select(&span_selector).collect();

        assert_eq!(
            spans.len(),
            2,
            "Page title should have exactly 2 span elements"
        );
        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain 'Fletcher: ' prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            "",
            "Second span should handle empty title gracefully"
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
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h1_element.select(&span_selector).collect();

        assert_eq!(
            spans.len(),
            2,
            "Page title should have exactly 2 span elements"
        );
        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain 'Fletcher: ' prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            "Data &amp; Analytics &lt;Report&gt;",
            "Second span should properly escape HTML special characters"
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
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h1_element.select(&span_selector).collect();

        assert_eq!(
            spans.len(),
            2,
            "Page title should have exactly 2 span elements"
        );
        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain 'Fletcher: ' prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            title,
            "Second span should handle long titles correctly"
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
        let span_selector = Selector::parse("span").unwrap();
        let spans: Vec<_> = h1_element.select(&span_selector).collect();

        assert_eq!(
            spans.len(),
            2,
            "Page title should have exactly 2 span elements"
        );
        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain 'Fletcher: ' prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            "Plan 42 - Version 1.2.3",
            "Second span should handle mixed alphanumeric content correctly"
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

    /// Test header function with dataset_id
    #[test]
    fn test_header_with_dataset_id() {
        let title = "Test Page";
        let dataset_id = Some(Uuid::new_v4());
        let header_markup = header(title, &dataset_id);
        let header_html = header_markup.into_string();
        let document = Html::parse_fragment(&header_html);

        // Should contain header element
        let header_selector = Selector::parse("header").unwrap();
        let header_element = document.select(&header_selector).next();
        assert!(
            header_element.is_some(),
            "Header should contain header element"
        );

        // Should contain navigation
        let nav_selector = Selector::parse("nav").unwrap();
        let nav_element = document.select(&nav_selector).next();
        assert!(nav_element.is_some(), "Header should contain navigation");

        // Should have 2 nav items (Search and Plan)
        let li_selector = Selector::parse("nav ul li").unwrap();
        let nav_items = document.select(&li_selector).count();
        assert_eq!(
            nav_items, 2,
            "Header with dataset_id should have 2 nav items"
        );

        // Should contain page title
        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next();
        assert!(h1_element.is_some(), "Header should contain h1 title");

        // Should contain title spans
        let span_selector = Selector::parse("h1 span").unwrap();
        let spans = document.select(&span_selector).count();
        assert_eq!(spans, 2, "Header title should have 2 spans");
    }

    /// Test header function without dataset_id
    #[test]
    fn test_header_without_dataset_id() {
        let title = "Search Page";
        let dataset_id = None;
        let header_markup = header(title, &dataset_id);
        let header_html = header_markup.into_string();
        let document = Html::parse_fragment(&header_html);

        // Should contain header element
        let header_selector = Selector::parse("header").unwrap();
        let header_element = document.select(&header_selector).next();
        assert!(
            header_element.is_some(),
            "Header should contain header element"
        );

        // Should contain navigation
        let nav_selector = Selector::parse("nav").unwrap();
        let nav_element = document.select(&nav_selector).next();
        assert!(nav_element.is_some(), "Header should contain navigation");

        // Should have 1 nav item (Search only)
        let li_selector = Selector::parse("nav ul li").unwrap();
        let nav_items = document.select(&li_selector).count();
        assert_eq!(
            nav_items, 1,
            "Header without dataset_id should have 1 nav item"
        );

        // Should contain page title
        let h1_selector = Selector::parse("h1").unwrap();
        let h1_element = document.select(&h1_selector).next();
        assert!(h1_element.is_some(), "Header should contain h1 title");

        // Check title content
        let span_selector = Selector::parse("h1 span").unwrap();
        let spans: Vec<_> = h1_element.unwrap().select(&span_selector).collect();
        assert_eq!(spans.len(), 2, "Header title should have 2 spans");
        assert_eq!(
            spans[0].inner_html(),
            "Fletcher: ",
            "First span should contain Fletcher prefix"
        );
        assert_eq!(
            spans[1].inner_html(),
            "Search Page",
            "Second span should contain title"
        );
    }

    /// Test header function with empty title
    #[test]
    fn test_header_empty_title() {
        let title = "";
        let dataset_id = Some(Uuid::new_v4());
        let header_markup = header(title, &dataset_id);
        let header_html = header_markup.into_string();
        let document = Html::parse_fragment(&header_html);

        // Should still contain all structure elements
        let header_selector = Selector::parse("header").unwrap();
        assert!(
            document.select(&header_selector).next().is_some(),
            "Header should contain header element even with empty title"
        );

        let nav_selector = Selector::parse("nav").unwrap();
        assert!(
            document.select(&nav_selector).next().is_some(),
            "Header should contain navigation even with empty title"
        );

        let h1_selector = Selector::parse("h1").unwrap();
        assert!(
            document.select(&h1_selector).next().is_some(),
            "Header should contain h1 title even with empty title"
        );
    }
}
