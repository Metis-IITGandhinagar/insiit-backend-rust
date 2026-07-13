use axum::{ extract::{ Json, Path, State }, routing:: { Router, get, post }, http::StatusCode, response::Json as JsonResponse };
use sqlx::{ PgPool, query, query_as };

use crate::AppState;
use crate::schemas::outlets_schemas::Outlet;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/outlets/{id}", get(get_outlet))
        .route("/outlets", get(get_outlets))
        .route("/outlets", post(add_outlet))
}

async fn get_outlets(State(state): State<AppState>) -> Result<JsonResponse<Vec<Outlet>>, (StatusCode, String)> {
    match query_as::<_, Outlet>(
        "SELECT id, name, latitude, longitude, landmark, open_time, close_time, menu, base64_image FROM outlets;"
    )
    .fetch_all(&state.pool).await {
        Ok(outlets) => Ok(Json(outlets)),
        Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn' get outlets from database")))
    }
}

async fn get_outlet(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<Outlet>, (StatusCode, String)> {
    match query_as::<_, Outlet>(
        "SELECT id, name, latitude, longitude, description, landmark, base64_image, menu, open_time, close_time WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool)
        .await {
            Ok(outlet) => Ok(Json(outlet)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get outlet from the database")))
        }
}

async fn add_outlet(State(state): State<AppState>, Json(outlet): Json<Outlet>) -> Result<JsonResponse<Outlet>, (StatusCode, String)> {
    match query(
        "INSERT INTO outlets (name, latitude, longitude, landmark, open_time, close_time, menu, base64_image) VALUES($1, $2, $3, $3, $4, $5, $5::jsonb, $6)"
    )
        .bind(&outlet.name)
        .bind(&outlet.location.latitude)
        .bind(&outlet.location.longitude)
        .bind(&outlet.landmark)
        .bind(&outlet.open_time)
        .bind(&outlet.close_time)
        .bind(serde_json::to_value(&outlet.menu).expect("will only be invoked if payload is properly structured"))
        .bind(&outlet.base64_image)
        .execute(&state.pool).await {
            Ok(_) => Ok(Json(outlet)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add outlet to database")))
    }
}
