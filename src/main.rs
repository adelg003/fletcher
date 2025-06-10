mod api;
mod core;
mod db;
mod model;

use api::Api;
use color_eyre::eyre;
use poem::{EndpointExt, Route, Server, listener::TcpListener, middleware::Tracing};
use poem_openapi::OpenApiService;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), eyre::Error> {
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
        .at("/spec", spec)
        .nest("/swagger", swagger)
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
