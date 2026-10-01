use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{Router, delete, get, post, put},
};
use axum_extra::{
    headers::Authorization, headers::authorization::Bearer, typed_header::TypedHeader,
};
use sqlx::{query, query_as};
use time::OffsetDateTime;

use crate::AppState;
use crate::auth::email_from_token;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::events_schemas::{EventEntry, EventRequest};
use crate::schemas::push_schemas;
use crate::utils::save_image;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/events", get(get_events))
        .route("/events/pending", get(get_pending_events))
        .route("/events/{id}", get(get_event))
        .route("/events", post(add_event))
        .route("/events/{id}", put(edit_event))
        .route("/events/{id}/approve", put(approve_event))
        .route("/events/{id}/reject", put(reject_event))
        .route("/events/{id}", delete(delete_event))
}

/// An event that ends before it starts is a typo, not a schedule.
fn check_datetimes(event_request: &EventRequest) -> Result<(), (StatusCode, String)> {
    match event_request.end_datetime {
        Some(end_datetime) if end_datetime <= event_request.start_datetime => Err((
            StatusCode::BAD_REQUEST,
            String::from("end_datetime must be later than start_datetime"),
        )),
        _ => Ok(()),
    }
}

async fn get_events(
    State(state): State<AppState>,
) -> Result<JsonResponse<Vec<EventEntry>>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved FROM events WHERE start_datetime > $1 AND approved = TRUE"
    )
        .bind(OffsetDateTime::now_utc())
        .fetch_all(&state.pool).await {
            Ok(events) => Ok(Json(events)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get events from the database")))
        }
}

async fn get_event(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved FROM events WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(event) => Ok(Json(event)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get event from the database")))
        }
}

async fn get_pending_events(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
) -> Result<JsonResponse<Vec<EventEntry>>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    match AdminPermission::ManageEvents
        .granted_to(token, state.clone())
        .await
    {
        Ok(Some(_)) => (),
        Ok(None) => return Err((StatusCode::FORBIDDEN, String::from("Forbidden"))),
        Err(e) => {
            log::error!("Events: Couldn't authenticate user: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't authenticate user"),
            ));
        }
    };
    match query_as::<_, EventEntry>(
        "SELECT id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved FROM events WHERE approved = FALSE ORDER BY start_datetime"
    )
        .fetch_all(&state.pool).await {
            Ok(events) => Ok(Json(events)),
            Err(e) => {
                log::error!("Events: Error fetching pending events: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get pending events from the database")))
            }
        }
}

async fn approve_event(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<i32>,
) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    match AdminPermission::ManageEvents
        .granted_to(token, state.clone())
        .await
    {
        Ok(Some(_)) => (),
        Ok(None) => return Err((StatusCode::FORBIDDEN, String::from("Forbidden"))),
        Err(e) => {
            log::error!("Events: Couldn't authenticate user: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't authenticate user"),
            ));
        }
    };
    match query_as::<_, EventEntry>(
        "UPDATE events
        SET approved = TRUE
        WHERE id = $1
        RETURNING id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved;
        "
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(event) => {
                if let Err(e) = push_schemas::enqueue_approval(&state.pool, event.id, event.start_datetime).await {
                    log::error!("Events: Couldn't schedule notifications for approved event_entry: {e}");
                }
                log::info!("Events: Approved event_entry");
                Ok(Json(event))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("Events: didn't find any event_entry to approve.");
                Err((StatusCode::NOT_FOUND, String::from("event not found")))
            },
            Err(e) => {
                log::error!("Events: Error approving event_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't approve event entry in the database")))
            }
        }
}

async fn reject_event(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<i32>,
) -> Result<JsonResponse<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    match AdminPermission::ManageEvents
        .granted_to(token, state.clone())
        .await
    {
        Ok(Some(_)) => (),
        Ok(None) => return Err((StatusCode::FORBIDDEN, String::from("Forbidden"))),
        Err(e) => {
            log::error!("Events: Couldn't authenticate user: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't authenticate user"),
            ));
        }
    };
    let rejection = query("UPDATE events SET approved = FALSE WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await;
    match rejection {
        Ok(result) if result.rows_affected() > 0 => {
            // Nothing queued for a rejected event should still go out.
            if let Err(e) = push_schemas::cancel_pending(&state.pool, id).await {
                log::error!("Events: Couldn't cancel notifications for rejected event_entry: {e}");
            }
            log::info!("Events: Rejected event_entry");
            Ok(Json(()))
        }
        Ok(_) => {
            log::info!("Events: didn't find any event_entry to reject.");
            Err((StatusCode::NOT_FOUND, String::from("event not found")))
        }
        Err(e) => {
            log::error!("Events: Error rejecting event_entry: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't reject event entry in the database"),
            ))
        }
    }
}

async fn add_event(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Json(event_request): Json<EventRequest>,
) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let email = email_from_token(&state, token).await?;
    check_datetimes(&event_request)?;
    let poster_url = if let Some(poster_base64) = &event_request.poster_base64 {
        match save_image(poster_base64, &state.image_directory).await {
            Ok(url) => Some(url),
            Err(_) => {
                log::error!("Events: Failed to save event poster image");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("Couldn't save event poster image"),
                ));
            }
        }
    } else {
        None
    };
    match query_as::<_, EventEntry>(
        "INSERT INTO events(name, description, poster_url, added_by_email, address, start_datetime, end_datetime)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved;
        ",
    )
    .bind(&event_request.name)
    .bind(&event_request.description)
    .bind(&poster_url)
    .bind(email)
    .bind(&event_request.address)
    .bind(&event_request.start_datetime)
    .bind(&event_request.end_datetime)
    .fetch_one(&state.pool)
    .await
    {
        Ok(event) => Ok(Json(event)),
        Err(_e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from("Couldn't add event to database"),
        )),
    }
}

async fn edit_event(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<i32>,
    Json(event_request): Json<EventRequest>,
) -> Result<JsonResponse<EventEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let email = email_from_token(&state, token).await?;
    check_datetimes(&event_request)?;

    let poster_url = if let Some(poster_base64) = &event_request.poster_base64 {
        match save_image(poster_base64, &state.image_directory).await {
            Ok(url) => Some(url),
            Err(_) => {
                log::error!("Events: Failed to save event poster image");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("Couldn't save event poster image"),
                ));
            }
        }
    } else {
        None
    };

    match query_as::<_, EventEntry>(
        "UPDATE events
        SET name = $1, description = $2, poster_url = COALESCE($3, poster_url), address = $4, start_datetime = $5, end_datetime = $6
        WHERE id = $7 AND added_by_email = $8
        RETURNING id, name, description, poster_url, added_by_email, address, start_datetime, end_datetime, approved;
        "
    )
        .bind(&event_request.name)
        .bind(&event_request.description)
        .bind(&poster_url)
        .bind(&event_request.address)
        .bind(&event_request.start_datetime)
        .bind(&event_request.end_datetime)
        .bind(id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_event_entry) => {
                if let Err(e) = push_schemas::reschedule_reminders(&state.pool, id, updated_event_entry.start_datetime).await {
                    log::error!("Events: Couldn't reschedule reminders for edited event_entry: {e}");
                }
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

async fn delete_event(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Path(id): Path<i32>,
) -> Result<Json<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let email = email_from_token(&state, token.clone()).await?;
    let can_approve = matches!(
        AdminPermission::ManageEvents
            .granted_to(token, state.clone())
            .await,
        Ok(Some(_))
    );
    let deletion = if can_approve {
        query("DELETE FROM events WHERE id = $1")
            .bind(id)
            .execute(&state.pool)
            .await
    } else {
        query("DELETE FROM events WHERE id = $1 AND added_by_email = $2")
            .bind(id)
            .bind(email)
            .execute(&state.pool)
            .await
    };
    match deletion {
        Ok(result) if result.rows_affected() > 0 => {
            log::info!("Events: Delete event_entry");
            Ok(Json(()))
        }
        Ok(_) => Err((StatusCode::NOT_FOUND, String::from("event not found"))),
        Err(e) => {
            log::error!("Events: Error deleting event_entry: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't delete event entry from the database"),
            ))
        }
    }
}
