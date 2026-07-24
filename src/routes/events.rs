use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::events_schemas::EventEntry;


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/events", get(get_events))
        .route("/events/{id}", get(get_event))
        .route("/events", post(verify_and_execute(AdminPermission::PostEvent, add_event)))
        .route("/events/{id}", delete(delete_event))
}


async fn get_events(State(state): State<AppState>) -> Result<JsonResponse<Vec<EventEntry>>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, poster_base64, added_by_email, address, start_datetime FROM events WHERE start_datetime > $1"
    )
        .bind(OffsetDateTime::now_utc())
        .fetch_all(&state.pool).await {
            Ok(events) => Ok(Json(events)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get events from the database")))
        }
}

async fn get_event(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_base64, added_by_email, address, start_datetime FROM events WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(event) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get event from the database")))
        }
}

async fn add_event(State(state): State<AppState>, request: Request) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let Json(event) = match Json::<EventEntry>::from_request(request, &state).await {
        Ok(event) => event,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    match query(
        "INSERT INTO events(name, description, poster_base64, added_by_email, address, start_datetime) VALUES($1, $2, $3, $4, $5, $6)"
    )
        .bind(&event.name)
        .bind(&event.description)
        .bind(&event.poster_base64)
        .bind(&event.added_by_email)
        .bind(&event.address)
        .bind(&event.start_datetime)
        .execute(&state.pool).await {
            Ok(_) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add event to database")))
        }
}

async fn delete_event(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>) -> Result<Json<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("Events: Found user for saving event_entry");
            user
        },
        Err(e) => {
            log::error!("Events: Couldn't find user for saving event_entry: {e}");
            return Err((StatusCode::FORBIDDEN, String::from("Couldn't authenticate user")))
        }
    };
    let email = match user.get("email") {
        Some(value) => match value.as_str() {
            Some(email) => email,
            None => return Err((StatusCode::FORBIDDEN, String::from("Invalid user"))),
        },
        None => return Err((StatusCode::FORBIDDEN, String::from("Invalid user"))),
    };
    match query(
        "DELETE FROM events WHERE id = $1 AND added_by_email = $2"
    )
        .bind(id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(result) if result.rows_affected() > 0 => {
                log::info!("Events: Delete event_entry");
                Ok(Json(()))
            },
            Ok(_) => {
                Err((StatusCode::NOT_FOUND, String::from("event not found")))
            },
            Err(e) => {
                log::error!("Events: Error deleting event_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't delete event entry from the database")))
            }
        }
}
