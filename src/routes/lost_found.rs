use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post, put }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::lost_found_schemas::{ LostFoundEntry, LostFoundRequest, LostFoundStatus, LostFoundClaim };


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/lost-found", get(get_all_lost_found))
        .route("/lost-found/{id}", get(get_lost_found_by_id))
        .route("/lost-found/{id}", delete(delete_lost_found))
        .route("/lost-found", put(edit_lost_found))
        .route("/lost-found", post(add_lost_found))
        .route("/lost-found/claim-found", post(claim_found))
        .route("/lost-found/mark-found", put(mark_found))
}

async fn get_all_lost_found(State(state): State<AppState>) -> Result<JsonResponse<Vec<LostFoundEntry>>, (StatusCode, String)> {
    match query_as::<_, LostFoundEntry>(
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls FROM lostfoundentries"
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
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls FROM lostfoundentries WHERE id = $1"
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

async fn add_lost_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(lost_found_request): Json<LostFoundRequest>) -> Result<JsonResponse<LostFoundEntry>, (StatusCode, String)> {
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

    match query_as::<_, LostFoundEntry>(
        "INSERT INTO lostfoundentries(item_name, description, added_on_timestamp, added_by_email, img_urls)
        VALUES($1, $2, $3, $4, $5)
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls;
        "
    )
        .bind(&lost_found_request.item_name)
        .bind(&lost_found_request.description)
        .bind(timestamp)
        .bind(email)
        .bind(img_urls)
        .fetch_one(&state.pool).await {
            Ok(new_lost_found_entry) => {
                log::info!("LostFound: Added item lost_found_entry");
                Ok(Json(new_lost_found_entry))
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
    match query_as::<_, LostFoundEntry>(
        "UPDATE lostfoundentries
        SET item_name = $1, description = $2, img_urls = $3
        WHERE id = $4 AND added_by_email = $5
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls
        "
    )
        .bind(&lost_found_entry.item_name)
        .bind(&lost_found_entry.description)
        .bind(&lost_found_entry.img_urls)
        .bind(&lost_found_entry.id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_lost_found_entry) => {
                log::info!("LostFound: Added item lost_found_entry");
                Ok(Json(updated_lost_found_entry))
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

async fn delete_lost_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>) -> Result<JsonResponse<()>, (StatusCode, String)>{
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
        .bind(id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(result) if result.rows_affected() > 0 => {
                log::info!("LostFound: Delete item lost_found_entry");
                Ok(Json(()))
            },
            Ok(_) => {
                Err((StatusCode::NOT_FOUND, String::from("entry not found")))
            },
            Err(e) => {
                log::error!("LostFound: Error adding lost_found_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't delete lost found entry from the database")))
            }
        }
}

async fn mark_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(mut lost_found_entry): Json<LostFoundEntry>) -> Result<JsonResponse<LostFoundEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("LostFound: Found user for mark_found lost_found_entry");
            user
        },
        Err(e) => {
            log::error!("LostFound: Couldn't find user for mark_found lost_found_entry");
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
    match query_as::<_, LostFoundEntry>(
        "UPDATE lostfoundentries
        SET status = 'found'
        WHERE id = $1 AND added_by_email = $2
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls
        "
    )
        .bind(lost_found_entry.id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_lost_found_entry) => {
                log::info!("LostFound: Marked item found");
                Ok(Json(updated_lost_found_entry))
            },
            Err(e) => {
                log::info!("LostFound: Couldn't update item");
                Err((StatusCode::FORBIDDEN, String::from("Couldn't update item as found")))
            }
        }
}

async fn claim_found(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(mut claim_request): Json<LostFoundClaim>) ->  Result<JsonResponse<LostFoundEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("LostFound: Found user for claim_found lost_found_entry");
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
    claim_request.claimed_by_email = String::from(email);
    match query_as::<_, LostFoundEntry>(
        "UPDATE lostfoundentries
        SET found_claims = COALESCE(found_claims, '[]'::jsonb) || $1::jsonb,
            status = 'claimed_to_be_found'
        WHERE id = $2 AND (status = 'claimed_to_be_found' OR status = 'lost')
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, found_claims, img_urls
        "
    )
        .bind(&serde_json::to_value(&claim_request).expect("found_clams is a vec"))
        .bind(&claim_request.id)
        .fetch_one(&state.pool).await {
            Ok(update_lost_found_entry) => Ok(Json(update_lost_found_entry)),
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("didn't find appropriate entry to be updated");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't add found claim in the database")))
            }
            Err(e) => {
                log::error!("LostFound: Couldn't update found claims: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add found claim in the database")))
            }
        }
}
