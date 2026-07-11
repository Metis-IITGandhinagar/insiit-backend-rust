// Comment out the following line when building for production
#![allow(unused)]


use axum::{ routing::{ Router, get, post } };
mod helpers;
mod routes;
mod schemas;
use sqlx::{ postgres::PgPoolOptions };
use std::{ env::var, fmt::format };

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let env_vars = match get_env_vars() {
        Ok(v) => v,
        Err(e) => panic!("Couldn't get all the environment variables: {e}")
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(env_vars.postgres_url.as_str()).await.expect("Couldn't connect to database");
    match helpers::initialize_database(&pool).await {
        Ok(_) => println!("Successfully initialized database"),
        Err(e) => panic!("Couldn't initialize database: {e}")
    };
    println!("Connected to database");
    let mess_routes = routes::mess::get_routes();
    let bus_routes = routes::bus::get_routes();
    let router = Router::new()
        .route("/", get(async || {"Go to /api-docs for API Documentation"}))
        .merge(mess_routes)
        .merge(bus_routes)
        .with_state(pool);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3700").await.unwrap();
    axum::serve(listener, router).await.unwrap();
}

fn get_env_vars() -> Result<EnvironmentVariables, String> {
    let mut env_vars = EnvironmentVariables { postgres_url: String::new() };
    match var("POSTGRES_URL") {
        Ok(v) => env_vars.postgres_url = v,
        Err(e) =>  return Err(format!("Couldn't get environment variable POSTGRES_URL: {e}"))
    }
    Ok(env_vars)

}

struct EnvironmentVariables {
    postgres_url: String
}

