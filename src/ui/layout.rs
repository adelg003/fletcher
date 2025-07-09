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
