use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post, put }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::lost_found_schemas::{ LostFoundEntry, LostFoundRequest };


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/lost-found", get(get_all_lost_found))
        .route("/lost-found/{id}", get(get_lost_found_by_id))
        .route("/lost-found", delete(delete_lost_found))
        .route("/lost-found", put(edit_lost_found))
        .route("/lost-found", post(add_lost_found))
}

async fn get_all_lost_found(State(state): State<AppState>) -> Result<JsonResponse<Vec<LostFoundEntry>>, (StatusCode, String)> {
    match query_as::<_, LostFoundEntry>(
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, is_found, img_urls FROM lostfoundentries"
    )
        .fetch_all(&state.pool)
        .await {
            Ok(lost_found_entries) => {
                log::info!("Sending lost found entries");
                Ok(Json(lost_found_entries))
            },
            Err(e) => {
                log::error!("Error fetching lost found entries: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't fetch lost found entries from database")))
            }
        }
}

async fn get_lost_found_by_id(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<LostFoundEntry>, (StatusCode, String)> {
    match query_as::<_, LostFoundEntry>(
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, is_found, img_urls FROM lostfoundentries WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(lost_found_entry) => {
                log::info!("LostFound: Sending lost_found_entry from database");
                Ok(Json(lost_found_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("LostFound: didn't find any lost_found_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get lost_found_entry from db")))
            },
            Err(e) => {
                log::error!("LostFound: Error fetching lost_found_entry from database: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get lost_found_entry from db")))
            }
        }
}

async fn add_lost_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(lost_found_request): Json<LostFoundRequest>) -> Result<JsonResponse<LostFoundRequest>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("LostFound: Found user for saving lost_found_entry");
            user
        },
        Err(e) => {
            log::error!("LostFound: Couldn't find user for saving lost_found_entry: {e}");
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
    let timestamp = OffsetDateTime::now_utc();
    let mut img_urls = vec![];
    for img in &lost_found_request.base64_images {
        // WARNING: Fix this
        let url = crate::utils::save_image(img).await.unwrap();
        img_urls.push(url);
    }

    match query(
        "INSERT INTO lostfoundentries(item_name, description, added_on_timestamp, added_by_email, img_urls) values($1, $2, $3, $4, $5);"
    )
        .bind(&lost_found_request.item_name)
        .bind(&lost_found_request.description)
        .bind(timestamp)
        .bind(email)
        .bind(img_urls)
        .execute(&state.pool).await {
            Ok(_) => {
                log::info!("LostFound: Added item lost_found_entry");
                Ok(Json(lost_found_request))
            },
            Err(e) => {
                log::error!("LostFound: Error adding lost_found_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add lost found entry in the database")))
            }
        }
}

async fn edit_lost_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(lost_found_entry): Json<LostFoundEntry>) -> Result<JsonResponse<LostFoundEntry>, (StatusCode, String)>{
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("LostFound: Found user for saving lost_found_entry");
            user
        },
        Err(e) => {
            log::error!("LostFound: Couldn't find user for saving lost_found_entry: {e}");
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
        "UPDATE lostfoundentries set item_name = $1, description = $2, is_found = $3, img_urls = $4 WHERE id = $5 AND added_by_email = $6"
    )
        .bind(&lost_found_entry.item_name)
        .bind(&lost_found_entry.description)
        .bind(&lost_found_entry.is_found)
        .bind(&lost_found_entry.img_urls)
        .bind(&lost_found_entry.id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(_) => {
                log::info!("LostFound: Added item lost_found_entry");
                Ok(Json(lost_found_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("LostFound: didn't find any lost_found_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get lost_found_entry from db")))
            },
            Err(e) => {
                log::error!("LostFound: Error adding lost_found_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't edit lost found entry in the database")))
            }
        }
}

async fn delete_lost_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(lost_found_entry): Json<LostFoundEntry>) -> Result<JsonResponse<LostFoundEntry>, (StatusCode, String)>{
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("LostFound: Found user for saving lost_found_entry");
            user
        },
        Err(e) => {
            log::error!("LostFound: Couldn't find user for saving lost_found_entry: {e}");
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
        "DELETE FROM lostfoundentries WHERE id = $1 AND added_by_email = $2"
    )
        .bind(&lost_found_entry.id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(_) => {
                log::info!("LostFound: Delete item lost_found_entry");
                Ok(Json(lost_found_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("LostFound: didn't find any lost_found_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get lost_found_entry from db")))
            },
            Err(e) => {
                log::error!("LostFound: Error adding lost_found_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't delete lost found entry from the database")))
            }
        }
}
