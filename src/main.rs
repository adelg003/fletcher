mod api;
mod core;
mod dag;
mod db;
mod error;
mod model;
mod ui;

use crate::{
    api::Api,
    ui::{not_found_404, route},
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
#[folder = "tmp/assets"]
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
