mod api;
mod core;
mod dag;
mod db;
mod error;
mod model;
mod ui;

use crate::{
    api::Api,
    ui::{not_found_404, user_service},
};
use poem::{
    EndpointExt, Route, Server, endpoint::EmbeddedFilesEndpoint, listener::TcpListener,
    middleware::Tracing,
};
use poem_openapi::OpenApiService;
use rust_embed::Embed;
use sqlx::PgPool;

/// Static files hosted via webserver
#[derive(Embed)]
#[folder = "assets"]
struct Assets;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Lets get pretty error reports
    color_eyre::install()?;

    // Enable Poem's logging
    tracing_subscriber::fmt::init();

    // Setup our OpenAPI Service
    let api_service =
        OpenApiService::new(Api, "Fletcher", "0.1.0").server("http://0.0.0.0:3000/api");
    let spec = api_service.spec_endpoint();
    let swagger = api_service.swagger_ui();

    // Connect to PostgreSQL
    let pool = PgPool::connect(&dotenvy::var("DATABASE_URL")?).await?;

    // Route inbound traffic
    let app = Route::new()
        // Developer friendly locations
        .nest("/api", api_service)
        .nest("/assets", EmbeddedFilesEndpoint::<Assets>::new())
        .at("/spec", spec)
        .nest("/swagger", swagger)
        // User UI
        .nest("/", user_service())
        .catch_error(not_found_404)
        // Global context to be shared
        .data(pool)
        // Utilites being added to our services
        .with(Tracing);

    // Lets run our service
    Server::new(TcpListener::bind("0.0.0.0:3000"))
        .run(app)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that required asset files are embedded correctly
    #[test]
    fn test_required_assets_exist() {
        // Check that htmx.min.js exists in the embedded assets
        let htmx_file = Assets::get("htmx/htmx.min.js");
        assert!(
            htmx_file.is_some(),
            "htmx/htmx.min.js should be present in embedded assets",
        );

        // Check that viz-standalone.js exists in the embedded assets
        let viz_file = Assets::get("viz/viz-standalone.js");
        assert!(
            viz_file.is_some(),
            "viz/viz-standalone.js should be present in embedded assets",
        );

        // Check that prism.js exists in the embedded assets
        let prism_js_file = Assets::get("prism/prism.js");
        assert!(
            prism_js_file.is_some(),
            "prism/prism.js should be present in embedded assets",
        );

        // Check that prism-json.js exists in the embedded assets
        let prism_json_file = Assets::get("prism/prism-json.js");
        assert!(
            prism_json_file.is_some(),
            "prism/prism-json.js should be present in embedded assets",
        );

        // Check that prism.css exists in the embedded assets
        let prism_css_file = Assets::get("prism/prism.css");
        assert!(
            prism_css_file.is_some(),
            "prism/prism.css should be present in embedded assets",
        );
    }
}
