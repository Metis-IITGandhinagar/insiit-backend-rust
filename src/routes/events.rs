use axum::{ extract::{ Json, Path, State }, routing:: { Router, get, post } , http::StatusCode, response::Json as JsonResponse };
use sqlx::{ PgPool, query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::events_schemas::EventEntry;


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/events", get(get_events))
        .route("/events/{id}", get(get_event))
        .route("/events", post(add_event))
}


async fn get_events(State(state): State<AppState>) -> Result<JsonResponse<Vec<EventEntry>>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, poster_base64, added_by_email, address, start_datetime WHERE start_datetime > $1"
    )
        .bind(OffsetDateTime::now_utc())
        .fetch_all(&state.pool).await {
            Ok(events) => Ok(Json(events)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get events from the database")))
        }
}

async fn get_event(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_base64, added_by_email, address, start_datetime WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(event) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get event from the database")))
        }
}

async fn add_event(State(state): State<AppState>, Json(event): Json<EventEntry>) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    match query(
        "INSERT INTO events(name, description, ,poster_base64, added_by_email, address, start_datetime)"
    )
        .execute(&state.pool).await {
            Ok(_) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add event to database")))
        }
}
