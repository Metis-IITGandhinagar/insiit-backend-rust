use axum::{ extract::{ FromRequest, Json, Path, Request, State }, routing:: { Router, get, post }, http::StatusCode, response::Json as JsonResponse };
use sqlx::query_as;

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::outlets_schemas::{ Outlet, OutletRequest };
use crate::utils::save_image;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/outlets/{id}", get(get_outlet))
        .route("/outlets", get(get_outlets))
        .route("/outlets", post(verify_and_execute(AdminPermission::PostOutlet, add_outlet)))
}

async fn get_outlets(State(state): State<AppState>) -> Result<JsonResponse<Vec<Outlet>>, (StatusCode, String)> {
    match query_as::<_, Outlet>(
        "SELECT id, name, description, latitude, longitude, landmark, open_time, close_time, menu, image_url FROM outlets;"
    )
    .fetch_all(&state.pool).await {
        Ok(outlets) => Ok(Json(outlets)),
        Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get outlets from database")))
    }
}

async fn get_outlet(State(state): State<AppState>, Path(id): Path<i32>) -> Result<JsonResponse<Outlet>, (StatusCode, String)> {
    match query_as::<_, Outlet>(
        "SELECT id, name, description, latitude, longitude, landmark, open_time, close_time, menu, image_url FROM outlets WHERE id = $1"
    )
        .bind(id)
        .fetch_one(&state.pool)
        .await {
            Ok(outlet) => Ok(Json(outlet)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get outlet from the database")))
        }
}

async fn add_outlet(State(state): State<AppState>, request: Request, _email: String) -> Result<JsonResponse<Outlet>, (StatusCode, String)> {
    let Json(outlet_request) = match Json::<OutletRequest>::from_request(request, &state).await {
        Ok(outlet_request) => outlet_request,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    let image_url = if let Some(base64_image) = &outlet_request.base64_image {
        match save_image(base64_image, &state.image_directory).await {
            Ok(url) => Some(url),
            Err(_) => {
                log::error!("Outlets: Failed to save outlet image");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't save outlet image")));
            }
        }
    } else { None };
    match query_as::<_, Outlet>(
        "INSERT INTO outlets (name, description, latitude, longitude, landmark, open_time, close_time, menu, image_url)
        VALUES($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
        RETURNING id, name, description, latitude, longitude, landmark, open_time, close_time, menu, image_url"
    )
        .bind(&outlet_request.name)
        .bind(&outlet_request.description)
        .bind(&outlet_request.location.latitude)
        .bind(&outlet_request.location.longitude)
        .bind(&outlet_request.landmark)
        .bind(&outlet_request.open_time)
        .bind(&outlet_request.close_time)
        .bind(serde_json::to_value(&outlet_request.menu).expect("will only be invoked if payload is properly structured"))
        .bind(&image_url)
        .fetch_one(&state.pool).await {
            Ok(outlet) => Ok(Json(outlet)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add outlet to database")))
    }
}
