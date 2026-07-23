use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post, put }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::{ AppState, utils::save_image };
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::announcements_schemas::{ AnnouncementEntry, AnnouncementRequest };


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/announcements", get(get_all_announcements))
        .route("/announcements/{id}", get(get_announcement_by_id))
        .route("/announcements/{id}", delete(delete_announcement))
        .route("/announcements", put(edit_announcement))
        .route("/announcements", post(verify_and_execute(AdminPermission::PostAnnouncement, add_announcement)))
}

async fn get_all_announcements(State(state): State<AppState>) -> Result<JsonResponse<Vec<AnnouncementEntry>>, (StatusCode, String)> {
    match query_as::<_, AnnouncementEntry>(
        //TODO filter by datetime > now
        "SELECT id, title, description, added_on_timestamp, added_by_email, img_url FROM announcements"
    )
        .fetch_all(&state.pool).await {
            Ok(announcements) => Ok(Json(announcements)),
            Err(sqlx::error::Error::RowNotFound) => Err((StatusCode::NOT_FOUND, String::from("No announcement found"))),
            Err(e) => {
                log::error!("Announcements: Error fetching announcements: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Error fetching announcement")))
            }
        }
}

async fn get_announcement_by_id(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<AnnouncementEntry>, (StatusCode, String)> {
    match query_as::<_, AnnouncementEntry>(
        "SELECT id, title, description, added_on_timestamp, added_by_email, img_url FROM announcements WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(announcement) => Ok(Json(announcement)),
            Err(sqlx::error::Error::RowNotFound) => Err((StatusCode::NOT_FOUND, String::from("Announcement not found"))),
            Err(e) => {
                log::error!("Announcements: Error fetching announcements: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Error fetching announcement")))
            }
        }
}

async fn delete_announcement(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>) -> Result<Json<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("Announcements: Found user for saving announcement_entry");
            user
        },
        Err(e) => {
            log::error!("Announcement: Couldn't find user for saving announcement_entry: {e}");
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
        "DELETE FROM announcements WHERE id = $1 AND added_by_email = $2"
    )
        .bind(id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(result) if result.rows_affected() > 0 => {
                log::info!("Announcements: Delete announcement_entry");
                Ok(Json(()))
            },
            Ok(_) => {
                Err((StatusCode::NOT_FOUND, String::from("announcement not found")))
            },
            Err(e) => {
                log::error!("Announcements: Error deleting announcement_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't delete announcement entry from the database")))
            }
        }
}


async fn edit_announcement(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>, Json(announcement_request): Json<AnnouncementRequest>) -> Result<JsonResponse<AnnouncementEntry>, (StatusCode, String)>{
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("Announcement: Found user for saving announcement_entry");
            user
        },
        Err(e) => {
            log::error!("Announcement: Couldn't find user for saving announcement_entry: {e}");
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

    let img_url = if let Some(img_base64) = &announcement_request.img_base64 {
        match save_image(img_base64).await {
            Ok(url) => Some(url),
            Err(e) => {
                // TODO log error properly
                log::error!("Failed to upload image:");
                None
            }
        }
    } else { None };
    match query_as::<_, AnnouncementEntry>(
        "UPDATE announcements
        SET title = $1, description = $2, img_url = COALESCE($3, img_url)
        WHERE id = $4 AND added_by_email = $5
        RETURNING id, title, description, added_on_timestamp, added_by_email, img_url
        "
    )
        .bind(&announcement_request.title)
        .bind(&announcement_request.description)
        .bind(img_url)
        .bind(id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_announcement_entry) => {
                log::info!("Announcement: Added announcement_entry");
                Ok(Json(updated_announcement_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("Announcement: didn't find any announcement_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get announcement_entry from db")))
            },
            Err(e) => {
                log::error!("Announcement: Error adding announcement_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't edit announcement entry in the database")))
            }
        }
}



async fn add_announcement(State(state): State<AppState>, request: Request) -> Result<JsonResponse<AnnouncementEntry>, (StatusCode, String)> {
    let Json(announcement_request) = match Json::<AnnouncementRequest>::from_request(request, &state).await {
        Ok(announcement_request) => announcement_request,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    let timestamp = OffsetDateTime::now_utc();
    // WARNING/FIX/TODO, implement the save_image function properly and don't use unwrap.
    let mut img_url = None;
    if let Some(img_base64) = &announcement_request.img_base64 {
        img_url = Some(crate::utils::save_image(img_base64).await.unwrap());
    }

    match query_as::<_, AnnouncementEntry>(
        "INSERT INTO announcements(title, description, added_on_timestamp, added_by_email, img_url)
        VALUES($1, $2, $3, $4, $5)
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_url;
        "
    )
        .bind(&announcement_request.title)
        .bind(&announcement_request.description)
        .bind(timestamp)
        // BUG/WARNING/TODO get email not from request header but from auth header token
        .bind(&announcement_request.added_by_email)
        .bind(img_url)
        .fetch_one(&state.pool).await {
            Ok(announcement) => {
                log::info!("Announcement: Added item announcement_entry");
                Ok(Json(announcement))
            },
            Err(e) => {
                log::error!("BuySell: Error adding buy_sell_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add buy sell entry in the database")))
            }
        }
}
