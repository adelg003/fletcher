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
