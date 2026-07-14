// Comment out the following line when building for production
// #![allow(unused)]

use axum::{ extract::FromRef, routing::{ Router, get } };
use rs_firebase_admin_sdk::App;
use std::sync::Arc;
use rs_firebase_admin_sdk::{ auth::FirebaseAuth, client::ReqwestApiClient };
use sqlx::{ postgres::PgPoolOptions, PgPool };
use std::env::var;


mod auth;
mod helpers;
mod routes;
mod schemas;

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

    let firebase_app = match App::live().await {
        Ok(app) => app,
        Err(e) => panic!("Couldn't connect to firebase project: {e}. Check README.md for steps")
    };
    println!("Connected to firebase project");

    let firebase_token_validator = Arc::new(
        rs_firebase_admin_sdk::jwt::LiveValidator::new_jwt_validator(env_vars.firebase_project_id.clone())
            .expect("Couldn't create LiveValidator")
    );
    println!("Created firebase token validator");

    let firebase_auth_service = firebase_app.auth();
    let state = AppState {
        pool: pool.clone(),
        firebase_auth_service: Arc::from(firebase_auth_service),
        firebase_token_validator,
    };

    let admin_routes = routes::admin::get_routes();
    let bus_routes = routes::bus::get_routes();
    let events_routes = routes::events::get_routes();
    let mess_routes = routes::mess::get_routes();
    let outlets_routes = routes::outlets::get_routes();
    let router = Router::new()
        .route("/", get(async || {"Go to /api-docs for API Documentation"}))
        .merge(admin_routes)
        .merge(bus_routes)
        .merge(events_routes)
        .merge(mess_routes)
        .merge(outlets_routes)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3700").await.unwrap();
    axum::serve(listener, router).await.unwrap();
}

fn get_env_vars() -> Result<EnvironmentVariables, String> {
    let mut env_vars = EnvironmentVariables { postgres_url: String::new(), firebase_project_id: String::new() };
    match var("POSTGRES_URL") {
        Ok(v) => env_vars.postgres_url = v,
        Err(e) =>  return Err(format!("Couldn't get environment variable POSTGRES_URL: {e}"))
    }
    match var("GOOGLE_CLOUD_PROJECT") {
        Ok(v) => env_vars.firebase_project_id = v,
        Err(e) =>  return Err(format!("Couldn't get environment variable GOOGLE_CLOUD_PROJECT: {e}"))
    }
    Ok(env_vars)

}

struct EnvironmentVariables {
    postgres_url: String,
    firebase_project_id: String
}


#[derive(Clone, FromRef)]
pub struct AppState {
    pool: PgPool,
    firebase_auth_service: Arc<FirebaseAuth<ReqwestApiClient>>,
    pub firebase_token_validator: Arc<rs_firebase_admin_sdk::jwt::LiveValidator>
}

