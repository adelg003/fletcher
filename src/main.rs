mod api;
mod auth;
mod core;
mod dag;
mod db;
mod error;
mod model;
mod ui;

use crate::{
    api::Api,
    auth::RemoteAuth,
    ui::{not_found_404, user_service},
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use poem::{
    EndpointExt, Route, Server, endpoint::EmbeddedFilesEndpoint, listener::TcpListener,
    middleware::Tracing,
};
use poem_openapi::OpenApiService;
use rust_embed::Embed;
use sqlx::{PgPool, migrate};

/// Struct we will put all our configs to run Fletcher into
#[derive(Clone)]
struct Config {
    database_url: String,
    decoding_key: DecodingKey,
    encoding_key: EncodingKey,
    remote_auths: Vec<RemoteAuth>,
}

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

    // Read in configs
    let config: Config = load_config()?;

    // Setup our OpenAPI Service
    let api_service =
        OpenApiService::new(Api, "Fletcher", "0.1.0").server("http://0.0.0.0:3000/api");
    let spec = api_service.spec_endpoint();
    let swagger = api_service.swagger_ui();

    // Connect to PostgreSQL
    let pool = PgPool::connect(&config.database_url).await?;
    migrate!().run(&pool).await?;

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
        .data(config)
        .data(pool)
        // Utilites being added to our services
        .with(Tracing);

    // Lets run our service
    Server::new(TcpListener::bind("0.0.0.0:3000"))
        .run(app)
        .await?;

    Ok(())
}

/// Load in Fletcher's configs
fn load_config() -> color_eyre::Result<Config> {
    let secret_key: String = dotenvy::var("SECRET_KEY")?;
    let secret_key: &[u8] = secret_key.as_bytes();

    Ok(Config {
        database_url: dotenvy::var("DATABASE_URL")?,
        encoding_key: EncodingKey::from_secret(secret_key),
        decoding_key: DecodingKey::from_secret(secret_key),
        remote_auths: serde_json::from_str(&dotenvy::var("REMOTE_APIS")?)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that required asset files are embedded correctly
    #[test]
    fn test_required_assets_exist() {
        // Check that tailwind.css exists in the embedded assets
        let tailwind_css_file = Assets::get("tailwindcss/tailwind.css");
        assert!(
            tailwind_css_file.is_some(),
            "tailwindcss/tailwind.css should be present in embedded assets",
        );

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
