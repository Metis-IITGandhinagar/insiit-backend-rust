use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{Router, post},
};
use axum_extra::{
    headers::Authorization, headers::authorization::Bearer, typed_header::TypedHeader,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Url;

use crate::AppState;
use crate::auth::institute_user_from_token;
use crate::schemas::sso_schemas::{
    self, CodeRequest, CodeResponse, ExchangeRequest, ExchangeResponse,
};

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/sso/codes", post(create_code))
        .route("/sso/token", post(exchange_code))
}

/// Called by the InsIIT app before it opens a partner app.
async fn create_code(
    State(state): State<AppState>,
    TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>,
    Json(code_request): Json<CodeRequest>,
) -> Result<JsonResponse<CodeResponse>, (StatusCode, String)> {
    let (uid, email) = institute_user_from_token(&state, auth_header.token().to_string()).await?;
    let invalid_redirect = || {
        (
            StatusCode::BAD_REQUEST,
            String::from("Unknown client_id or unregistered redirect_uri"),
        )
    };
    match sso_schemas::redirect_uri_registered(
        &state.pool,
        &code_request.client_id,
        &code_request.redirect_uri,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return Err(invalid_redirect()),
        Err(e) => {
            log::error!("SSO: Error checking redirect uri: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't check the app's redirect uri"),
            ));
        }
    }
    let mut redirect_url =
        Url::parse(&code_request.redirect_uri).map_err(|_| invalid_redirect())?;

    let mut code_bytes = [0u8; 32];
    if let Err(e) = getrandom::fill(&mut code_bytes) {
        log::error!("SSO: Couldn't generate code: {e}");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from("Couldn't generate code"),
        ));
    }
    let code = URL_SAFE_NO_PAD.encode(code_bytes);
    if let Err(e) =
        sso_schemas::create_code(&state.pool, &code, &code_request.client_id, &uid, &email).await
    {
        log::error!("SSO: Error saving code: {e}");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            String::from("Couldn't save code in the database"),
        ));
    }
    redirect_url.query_pairs_mut().append_pair("code", &code);
    Ok(Json(CodeResponse {
        redirect_url: redirect_url.to_string(),
    }))
}

/// Called by the partner app (or its server) with the code from the redirect.
async fn exchange_code(
    State(state): State<AppState>,
    Json(exchange_request): Json<ExchangeRequest>,
) -> Result<JsonResponse<ExchangeResponse>, (StatusCode, String)> {
    let redeemed = match sso_schemas::redeem_code(
        &state.pool,
        &exchange_request.client_id,
        &exchange_request.code,
    )
    .await
    {
        Ok(Some(redeemed)) => redeemed,
        Ok(None) => {
            return Err((
                StatusCode::BAD_REQUEST,
                String::from("Code is invalid, expired, already used, or for another app"),
            ));
        }
        Err(e) => {
            log::error!("SSO: Error redeeming code: {e}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't redeem code"),
            ));
        }
    };
    let firebase_custom_token = state
        .custom_token_signer
        .sign(&redeemed.uid, &exchange_request.client_id)
        .map_err(|e| {
            log::error!("SSO: Error signing custom token: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't create sign-in token"),
            )
        })?;
    log::info!("SSO: Redeemed code for {}", exchange_request.client_id);
    Ok(Json(ExchangeResponse {
        uid: redeemed.uid,
        email: redeemed.email,
        firebase_custom_token,
    }))
}
