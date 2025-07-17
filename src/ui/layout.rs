use crate::{
    model::DatasetId,
    ui::snippet::{head, header},
};
use maud::{DOCTYPE, Markup, html};

/// Base Page Layout
pub fn base_layout(title: &str, dataset_id: &Option<DatasetId>, main: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en-US" {
            (head())
            body {
                (header(title, dataset_id))
                main {
                    (main)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maud::html;
    use scraper::{Html, Selector};
    use uuid::Uuid;

    /// Test base_layout generates proper HTML structure with dataset_id
    #[test]
    fn test_base_layout_with_dataset_id() {
        let title = "Test Page";
        let dataset_id = Some(Uuid::new_v4());
        let main_content = html! {
            div {
                p { "Test main content" }
            }
        };

        let layout = base_layout(title, &dataset_id, main_content);
        let layout_html = layout.into_string();
        let document = Html::parse_document(&layout_html);

        // Should have DOCTYPE
        assert!(
            layout_html.starts_with("<!DOCTYPE html>"),
            "Layout should start with DOCTYPE declaration"
        );

        // Should have html element with lang attribute
        let html_selector = Selector::parse("html").unwrap();
        let html_element = document.select(&html_selector).next().unwrap();
        assert_eq!(
            html_element.attr("lang"),
            Some("en-US"),
            "HTML element should have lang='en-US' attribute"
        );

        // Should have head section
        let head_selector = Selector::parse("head").unwrap();
        let head_count = document.select(&head_selector).count();
        assert_eq!(head_count, 1, "Layout should have exactly one head element");

        // Should have body section
        let body_selector = Selector::parse("body").unwrap();
        let body_count = document.select(&body_selector).count();
        assert_eq!(body_count, 1, "Layout should have exactly one body element");

        // Should have header section inside body
        let header_selector = Selector::parse("body > header").unwrap();
        let header_count = document.select(&header_selector).count();
        assert_eq!(
            header_count, 1,
            "Layout should have exactly one header inside body"
        );

        // Should have main section inside body
        let main_selector = Selector::parse("body > main").unwrap();
        let main_count = document.select(&main_selector).count();
        assert_eq!(
            main_count, 1,
            "Layout should have exactly one main inside body"
        );

        // Main content should be present
        let main_p_selector = Selector::parse("main div p").unwrap();
        let main_p = document.select(&main_p_selector).next().unwrap();
        assert_eq!(
            main_p.inner_html(),
            "Test main content",
            "Main content should be rendered inside main element"
        );
    }

    /// Test base_layout generates proper HTML structure without dataset_id
    #[test]
    fn test_base_layout_without_dataset_id() {
        let title = "Another Test Page";
        let dataset_id = None;
        let main_content = html! {
            section {
                h2 { "Section Title" }
                p { "Section content goes here" }
            }
        };

        let layout = base_layout(title, &dataset_id, main_content);
        let layout_html = layout.into_string();
        let document = Html::parse_document(&layout_html);

        // Should have DOCTYPE
        assert!(
            layout_html.starts_with("<!DOCTYPE html>"),
            "Layout should start with DOCTYPE declaration"
        );

        // Should have proper structure even without dataset_id
        let html_selector = Selector::parse("html").unwrap();
        let html_element = document.select(&html_selector).next().unwrap();
        assert_eq!(
            html_element.attr("lang"),
            Some("en-US"),
            "HTML element should have lang='en-US' attribute"
        );

        // Main content should be rendered correctly
        let section_selector = Selector::parse("main section").unwrap();
        let section_count = document.select(&section_selector).count();
        assert_eq!(
            section_count, 1,
            "Layout should contain the section from main content"
        );

        let h2_selector = Selector::parse("main section h2").unwrap();
        let h2_element = document.select(&h2_selector).next().unwrap();
        assert_eq!(
            h2_element.inner_html(),
            "Section Title",
            "Section title should be rendered correctly"
        );

        let p_selector = Selector::parse("main section p").unwrap();
        let p_element = document.select(&p_selector).next().unwrap();
        assert_eq!(
            p_element.inner_html(),
            "Section content goes here",
            "Section content should be rendered correctly"
        );
    }

    /// Test base_layout with empty main content
    #[test]
    fn test_base_layout_with_empty_main() {
        let title = "Empty Main Test";
        let dataset_id = Some(Uuid::new_v4());
        let main_content = html! {};

        let layout = base_layout(title, &dataset_id, main_content);
        let layout_html = layout.into_string();
        let document = Html::parse_document(&layout_html);

        // Should still have proper structure
        let main_selector = Selector::parse("body > main").unwrap();
        let main_element = document.select(&main_selector).next().unwrap();

        // Main should exist but be empty
        let main_inner_html = main_element.inner_html();
        assert!(
            main_inner_html.trim().is_empty(),
            "Main element should be empty when no content provided"
        );

        // Other elements should still be present
        let header_selector = Selector::parse("body > header").unwrap();
        let header_count = document.select(&header_selector).count();
        assert_eq!(
            header_count, 1,
            "Header should still be present with empty main content"
        );
    }

    /// Test base_layout with complex nested main content
    #[test]
    fn test_base_layout_with_complex_main() {
        let title = "Complex Content Test";
        let dataset_id = None;
        let main_content = html! {
            article {
                header {
                    h1 { "Article Title" }
                    p class="meta" { "By Author" }
                }
                section {
                    p { "First paragraph" }
                    ul {
                        li { "List item 1" }
                        li { "List item 2" }
                    }
                }
                footer {
                    p { "Article footer" }
                }
            }
        };

        let layout = base_layout(title, &dataset_id, main_content);
        let layout_html = layout.into_string();
        let document = Html::parse_document(&layout_html);

        // Should handle complex nested content correctly
        let article_selector = Selector::parse("main article").unwrap();
        let article_count = document.select(&article_selector).count();
        assert_eq!(
            article_count, 1,
            "Complex main content should be preserved in layout"
        );

        // Check nested elements are preserved
        let h1_selector = Selector::parse("main article header h1").unwrap();
        let h1_element = document.select(&h1_selector).next().unwrap();
        assert_eq!(
            h1_element.inner_html(),
            "Article Title",
            "Nested h1 should be preserved"
        );

        let meta_selector = Selector::parse("main article header p.meta").unwrap();
        let meta_element = document.select(&meta_selector).next().unwrap();
        assert_eq!(
            meta_element.inner_html(),
            "By Author",
            "Class attributes should be preserved"
        );

        let li_selector = Selector::parse("main article section ul li").unwrap();
        let li_count = document.select(&li_selector).count();
        assert_eq!(li_count, 2, "Nested list items should be preserved");
    }
}
