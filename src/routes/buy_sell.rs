use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, delete, get, post, put }, http::StatusCode, response::Json as JsonResponse };
use axum_extra::{ typed_header::TypedHeader, headers::Authorization, headers::authorization::Bearer };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::buy_sell_schemas::{ BuySellEntry, BuySellRequest, BuySellStatus, BidEntry };


pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/buy-sell", get(get_all_buy_sell))
        .route("/buy-sell/:id", get(get_buy_sell_by_id))
        .route("/buy-sell/:id", delete(delete_buy_sell))
        .route("/buy-sell", put(edit_buy_sell))
        .route("/buy-sell", post(add_buy_sell))
        .route("/buy-sell/bid", post(add_bid))
        .route("/buy-sell/mark-sold", put(mark_sold))
}

async fn get_all_buy_sell(State(state): State<AppState>) -> Result<JsonResponse<Vec<BuySellEntry>>, (StatusCode, String)> {
    match query_as::<_, BuySellEntry>(
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls FROM buysellentries"
    )
        .fetch_all(&state.pool)
        .await {
            Ok(buys_sell_entries) => {
                log::info!("Sending buy sell entries");
                Ok(Json(buy_sell_entries))
            },
            Err(e) => {
                log::error!("Error fetching buy sell entries: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't fetch buy sell entries from database")))
            }
        }
}

async fn get_buy_sell_by_id(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<BuySellEntry>, (StatusCode, String)> {
    match query_as::<_, BuySellEntry>(
        "SELECT id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls FROM buysellentries WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool).await {
            Ok(buy_sell_entry) => {
                log::info!("BuySell: Sending buy_sell_entry from database");
                Ok(Json(buy_sell_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("BuySell: didn't find any buy_sell_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get buy_sell_entry from db")))
            },
            Err(e) => {
                log::error!("BuySell: Error fetching buy_sell_entry from database: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get buy_sell_entry from db")))
            }
        }
}

async fn add_buy_sell(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(buy_sell_request): Json<BuySellRequest>) -> Result<JsonResponse<BuySellEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("BuySell: Found user for saving buy_sell_entry");
            user
        },
        Err(e) => {
            log::error!("BuySell: Couldn't find user for saving buy_sell_entry: {e}");
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
    for img in &buy_sell_request.base64_images {
        let url = crate::utils::save_image(img).await.unwrap();
        img_urls.push(url);
    }

    match query_as::<_, BuySellEntry>(
        "INSERT INTO buysellentries(item_name, description, added_on_timestamp, added_by_email, img_urls)
        VALUES($1, $2, $3, $4, $5)
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls;
        "
    )
        .bind(&buy_sell_request.item_name)
        .bind(&buy_sell_request.description)
        .bind(timestamp)
        .bind(email)
        .bind(img_urls)
        .fetch_one(&state.pool).await {
            Ok(new_buy_sell_entry) => {
                log::info!("BuySell: Added item buy_sell_entry");
                Ok(Json(new_buy_sell_entry))
            },
            Err(e) => {
                log::error!("BuySell: Error adding buy_sell_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add buy sell entry in the database")))
            }
        }
}

async fn edit_buy_sell(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(buy_sell_entry): Json<BuySellEntry>) -> Result<JsonResponse<BuySellEntry>, (StatusCode, String)>{
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("BuySell: Found user for saving buy_sell_entry");
            user
        },
        Err(e) => {
            log::error!("BuySell: Couldn't find user for saving buy_sell_entry: {e}");
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
    match query_as::<_, BuySellEntry>(
        "UPDATE buysellentries
        SET item_name = $1, description = $2, img_urls = $3
        WHERE id = $4 AND added_by_email = $5
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls
        "
    )
        .bind(&buy_sell_entry.item_name)
        .bind(&buy_sell_entry.description)
        .bind(&buy_sell_entry.img_urls)
        .bind(&buy_sell_entry.id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_buy_sell_entry) => {
                log::info!("BuySell: Added item buy_sell_entry");
                Ok(Json(updated_buy_sell_entry))
            },
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("BuySell: didn't find any buy_sell_entry from database.");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't get buy_sell_entry from db")))
            },
            Err(e) => {
                log::error!("BuySell: Error adding buy_sell_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't edit buy sell entry in the database")))
            }
        }
}

async fn delete_buy_sell(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Path(id): Path<i32>) -> Result<JsonResponse<()>, (StatusCode, String)>{
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("BuySell: Found user for saving buy_sell_entry");
            user
        },
        Err(e) => {
            log::error!("BuySell: Couldn't find user for saving buy_sell_entry: {e}");
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
        "DELETE FROM buysellentries WHERE id = $1 AND added_by_email = $2"
    )
        .bind(id)
        .bind(email)
        .execute(&state.pool).await {
            Ok(result) if result.rows_affected() > 0 => {
                log::info!("BuySell: Delete item buy_sell_entry");
                Ok(Json(()))
            },
            Ok(_) => {
                Err((StatusCode::NOT_FOUND, String::from("entry not found")))
            },
            Err(e) => {
                log::error!("BuySell: Error adding buy_sell_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't delete buy sell entry from the database")))
            }
        }
}

async fn mark_sold(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(mut buy_sell_entry): Json<BuySellEntry>) -> Result<JsonResponse<BuySellEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("BuySell: Found user for mark_sold buy_sell_entry");
            user
        },
        Err(e) => {
            log::error!("BuySell: Couldn't find user for mark_sold buy_sell_entry");
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
    match query_as::<_, BuySellEntry>(
        "UPDATE buysellentries
        SET status = 'sold'
        WHERE id = $1 AND added_by_email = $2
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls
        "
    )
        .bind(buy_sell_entry.id)
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(updated_buy_sell_entry) => {
                log::info!("BuySell: Selling item");
                Ok(Json(updated_buy_sell_entry))
            },
            Err(e) => {
                log::info!("BuySell: Couldn't update item");
                Err((StatusCode::FORBIDDEN, String::from("Couldn't update item as sold")))
            }
        }
}

async fn add_bid(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, Json(mut bid_request): Json<BidEntry>) ->  Result<JsonResponse<BuySellEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => {
            log::info!("BuySell: Found user for add_bid buy_sell_entry");
            user
        },
        Err(e) => {
            log::error!("BuySell: Couldn't find user for saving buy_sell_entry: {e}");
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
    bid_request.bid_by_email = String::from(email);
    match query_as::<_, BuySellEntry>(
        "UPDATE buysellentries
        SET bids = COALESCE(bids, '[]'::jsonb) || $1::jsonb
        WHERE id = $2 AND (status = 'selling')
        RETURNING id, item_name, description, added_on_timestamp, added_by_email, status, bids, img_urls
        "
    )
        .bind(&serde_json::to_value(&bid_request).expect("bids is a vec"))
        .bind(&bid_request.item_id)
        .fetch_one(&state.pool).await {
            Ok(update_buy_sell_entry) => Ok(Json(update_buy_sell_entry)),
            Err(sqlx::error::Error::RowNotFound) => {
                log::info!("didn't find appropriate entry to be updated");
                Err((StatusCode::NOT_FOUND, String::from("Couldn't add bid in the database")))
            }
            Err(e) => {
                log::error!("BuySell: Couldn't update bids: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add bid in the database")))
            }
        }
}
