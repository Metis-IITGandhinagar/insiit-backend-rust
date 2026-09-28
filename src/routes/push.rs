use axum::{extract::{Json, Path, State}, http::StatusCode, response::Json as JsonResponse, routing::{Router, delete, post}};
use axum_extra::{headers::{Authorization, authorization::Bearer}, typed_header::TypedHeader};
use rs_firebase_admin_sdk::jwt::TokenValidator;
use crate::{AppState, schemas::push_schemas::{self, DeviceRequest}};

pub fn get_routes() -> Router<AppState> {
    Router::new().route("/push/devices", post(register)).route("/push/devices/{device_id}", delete(remove))
}

async fn authenticated_user(state: &AppState, auth: &Authorization<Bearer>) -> Result<String, (StatusCode, String)> {
    let claims = state.firebase_token_validator.clone().validate(auth.token().to_string()).await
        .map_err(|_| (StatusCode::FORBIDDEN, "Couldn't authenticate user".to_owned()))?;
    claims.get("sub").and_then(serde_json::Value::as_str).map(str::to_owned)
        .ok_or((StatusCode::FORBIDDEN, "Invalid user".to_owned()))
}

async fn register(State(state): State<AppState>, TypedHeader(auth): TypedHeader<Authorization<Bearer>>, Json(device): Json<DeviceRequest>) -> Result<JsonResponse<()>, (StatusCode, String)> {
    if device.device_id.trim().is_empty() || device.device_id.len() > 255 || device.token.trim().is_empty() || device.token.len() > 4096 {
        return Err((StatusCode::BAD_REQUEST, "Invalid device_id or token".to_owned()));
    }
    let user_id = authenticated_user(&state, &auth).await?;
    push_schemas::register_device(&state.pool, &user_id, &device).await
        .map_err(|e| { log::error!("Push: Couldn't register device: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, "Couldn't register device".to_owned()) })?;
    Ok(Json(()))
}

async fn remove(State(state): State<AppState>, TypedHeader(auth): TypedHeader<Authorization<Bearer>>, Path(device_id): Path<String>) -> Result<JsonResponse<()>, (StatusCode, String)> {
    let user_id = authenticated_user(&state, &auth).await?;
    push_schemas::remove_device(&state.pool, &user_id, &device_id).await
        .map_err(|e| { log::error!("Push: Couldn't remove device: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, "Couldn't remove device".to_owned()) })?;
    Ok(Json(()))
}
