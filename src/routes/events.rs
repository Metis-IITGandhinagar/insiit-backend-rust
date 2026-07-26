use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post, put }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::events_schemas::{ EventEntry, EventRequest };
use crate::utils::save_image;


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/events", get(get_events))
        .route("/events/{id}", get(get_event))
        .route("/events", post(verify_and_execute(AdminPermission::PostEvent, add_event)))
        .route("/events/{id}", put(edit_event))
        .route("/events/{id}", delete(delete_event))
}


async fn get_events(State(state): State<AppState>) -> Result<JsonResponse<Vec<EventEntry>>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_url, added_by_email, address, start_datetime FROM events WHERE start_datetime > $1"
    )
        .bind(OffsetDateTime::now_utc())
        .fetch_all(&state.pool).await {
            Ok(events) => Ok(Json(events)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get events from the database")))
        }
}

async fn get_event(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_url, added_by_email, address, start_datetime FROM events WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(event) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get event from the database")))
        }
}

async fn add_event(State(state): State<AppState>, request: Request, email: String) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let Json(event_request) = match Json::<EventRequest>::from_request(request, &state).await {
        Ok(event_request) => event_request,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    let poster_url = if let Some(poster_base64) = &event_request.poster_base64 {
        match save_image(poster_base64, &state.image_directory).await {
            Ok(url) => Some(url),
            Err(_) => {
                log::error!("Events: Failed to save event poster image");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't save event poster image")));
            }
        }
    } else { None };
    match query_as::<_, EventEntry>(
        "INSERT INTO events(name, description, poster_url, added_by_email, address, start_datetime)
        VALUES($1, $2, $3, $4, $5, $6)
        RETURNING id, name, description, poster_url, added_by_email, address, start_datetime;
        "
    )
        .bind(&event_request.name)
        .bind(&event_request.description)
        .bind(&poster_url)
        .bind(&email)
        .bind(&event_request.address)
        .bind(&event_request.start_datetime)
        .fetch_one(&state.pool).await {
            Ok(event) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add event to database")))
        }
}

async fn edit_event(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>, Json(event_request): Json<EventRequest>) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("Events: Found user for editing event_entry");
            user
        },
        Err(e) => {
            log::error!("Events: Couldn't find user for editing event_entry: {e}");
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

    let poster_url = if let Some(poster_base64) = &event_request.poster_base64 {
        match save_image(poster_base64, &state.image_directory).await {
            Ok(url) => Some(url),
            Err(_) => {
                log::error!("Events: Failed to save event poster image");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't save event poster image")));
            }
        }
    } else { None };

    match query_as::<_, EventEntry>(
        "UPDATE events
        SET name = $1, description = $2, poster_url = COALESCE($3, poster_url), address = $4, start_datetime = $5
        WHERE id = $6 AND added_by_email = $7
        RETURNING id, name, description, poster_url, added_by_email, address, start_datetime;
        "
    )
        .bind(&event_request.name)
        .bind(&event_request.description)
        .bind(&poster_url)
        .bind(&event_request.address)
        .bind(&event_request.start_datetime)
        .bind(id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_event_entry) => {
                log::info!("Events: Edited event_entry");
                Ok(Json(updated_event_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("Events: didn't find any event_entry to edit for this user.");
                Err((StatusCode::NOT_FOUND, String::from("event not found")))
            },
            Err(e) => {
                log::error!("Events: Error editing event_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't edit event entry in the database")))
            }
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
