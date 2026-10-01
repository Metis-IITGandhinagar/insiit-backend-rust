use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{Router, delete, post},
};
use axum_extra::{
    headers::Authorization, headers::authorization::Bearer, typed_header::TypedHeader,
};

use crate::AppState;
use crate::auth::email_from_token;
use crate::schemas::push_schemas::{self, DeviceRequest};

/// Generous upper bounds; FCM registration tokens sit well under them.
const MAX_DEVICE_ID_LENGTH: usize = 255;
const MAX_TOKEN_LENGTH: usize = 4096;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/push/devices", post(register_device))
        .route("/push/devices/{device_id}", delete(remove_device))
}

async fn register_device(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Json(device_request): Json<DeviceRequest>,
) -> Result<JsonResponse<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let email = email_from_token(&state, token).await?;
    if device_request.device_id.trim().is_empty()
        || device_request.device_id.len() > MAX_DEVICE_ID_LENGTH
        || device_request.token.trim().is_empty()
        || device_request.token.len() > MAX_TOKEN_LENGTH
    {
        return Err((
            StatusCode::BAD_REQUEST,
            String::from("Invalid device_id or token"),
        ));
    }
    match push_schemas::register_device(&state.pool, &email, &device_request).await {
        Ok(()) => {
            log::info!("Push: Registered device");
            Ok(Json(()))
        }
        Err(e) => {
            log::error!("Push: Error registering device: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't register device in the database"),
            ))
        }
    }
}

async fn remove_device(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Path(device_id): Path<String>,
) -> Result<JsonResponse<()>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let email = email_from_token(&state, token).await?;
    match push_schemas::remove_device(&state.pool, &email, &device_id).await {
        Ok(()) => {
            log::info!("Push: Removed device");
            Ok(Json(()))
        }
        Err(e) => {
            log::error!("Push: Error removing device: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't remove device from the database"),
            ))
        }
    }
}
