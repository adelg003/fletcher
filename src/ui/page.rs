use maud::{Markup, html};
use poem::handler;

/// Index Page
#[handler]
pub async fn index() -> Markup {
    html! {
        p { "Hello World" }
    }
}
