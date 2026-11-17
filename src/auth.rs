use axum::{
    extract::{FromRequestParts, Request, State},
    http::StatusCode,
    response::IntoResponse,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rs_firebase_admin_sdk::jwt::TokenValidator;
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::admin_schemas::AdminPermission;

pub fn verify_and_execute<H, F, K>(
    permission: AdminPermission,
    handler: H,
) -> impl Clone
+ Fn(
    State<AppState>,
    Request,
) -> Pin<Box<dyn Future<Output = Result<K, (StatusCode, String)>> + Send>>
where
    H: Fn(State<AppState>, Request, String) -> F + Clone + Send + Sync + 'static,
    K: IntoResponse,
    F: Future<Output = Result<K, (StatusCode, String)>> + Send + 'static,
{
    move |State(state): State<AppState>, request: Request| {
        let handler = handler.clone();
        let state_clone = state.clone();
        let permission_clone = permission.clone();
        let (mut parts, body) = request.into_parts();
        Box::pin(async move {
            let auth_header = match TypedHeader::<Authorization<Bearer>>::from_request_parts(
                &mut parts,
                &state_clone,
            )
            .await
            {
                Ok(auth_header) => auth_header,
                Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid token"))),
            };
            match permission_clone
                .granted_to(auth_header.token().to_string(), state_clone.clone())
                .await
            {
                Ok(Some(email)) => {
                    handler(State(state_clone), Request::from_parts(parts, body), email).await
                }
                Ok(None) => Err((StatusCode::FORBIDDEN, String::from("Forbidden"))),
                Err(e) => {
                    log::error!("Couldn't authenticate user {e}");
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        String::from("Couldn't authenticate user"),
                    ));
                }
            }
        })
    }
}

/// The token -> email step every authenticated handler starts with.
pub async fn email_from_token(
    state: &AppState,
    token: String,
) -> Result<String, (StatusCode, String)> {
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => user,
        Err(e) => {
            log::error!("Couldn't authenticate user: {e}");
            return Err((
                StatusCode::FORBIDDEN,
                String::from("Couldn't authenticate user"),
            ));
        }
    };
    match user.get("email") {
        Some(value) => match value.as_str() {
            Some(email) => Ok(String::from(email)),
            None => Err((StatusCode::FORBIDDEN, String::from("Invalid user"))),
        },
        None => Err((StatusCode::FORBIDDEN, String::from("Invalid user"))),
    }
}

/// Claim added to custom tokens we mint for partner apps. It carries over into
/// the ID tokens Firebase issues from them, so we can tell those sessions apart.
pub const SSO_CLIENT_CLAIM: &str = "sso_client";
const ALLOWED_EMAIL_DOMAIN: &str = "@iitgn.ac.in";

/// The token -> (uid, email) step for handing a sign-in to a partner app. Only
/// verified institute accounts signed in to InsIIT itself get through.
pub async fn institute_user_from_token(
    state: &AppState,
    token: String,
) -> Result<(String, String), (StatusCode, String)> {
    let user = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => user,
        Err(e) => {
            log::error!("Couldn't authenticate user: {e}");
            return Err((
                StatusCode::FORBIDDEN,
                String::from("Couldn't authenticate user"),
            ));
        }
    };
    if user.contains_key(SSO_CLIENT_CLAIM) {
        return Err((
            StatusCode::FORBIDDEN,
            String::from("Sign in through InsIIT to use this"),
        ));
    }
    let uid = user.get("sub").and_then(|value| value.as_str());
    let email = user.get("email").and_then(|value| value.as_str());
    let email_verified = user.get("email_verified").and_then(|value| value.as_bool());
    match (uid, email, email_verified) {
        (Some(uid), Some(email), Some(true))
            if email.to_lowercase().ends_with(ALLOWED_EMAIL_DOMAIN) =>
        {
            Ok((String::from(uid), String::from(email)))
        }
        _ => Err((
            StatusCode::FORBIDDEN,
            String::from("Only verified IITGN accounts are allowed"),
        )),
    }
}

/// Mints Firebase custom tokens with the service account's key, which
/// rs-firebase-admin-sdk can't do.
pub struct CustomTokenSigner {
    client_email: String,
    key: EncodingKey,
}

#[derive(Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
}

#[derive(Serialize)]
struct CustomTokenClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
    uid: &'a str,
    claims: serde_json::Map<String, serde_json::Value>,
}

const CUSTOM_TOKEN_AUDIENCE: &str =
    "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit";
/// Firebase rejects custom tokens that live longer than an hour.
const CUSTOM_TOKEN_LIFETIME_SECONDS: i64 = 3600;

impl CustomTokenSigner {
    pub fn from_service_account_file(path: &str) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("Couldn't read service account file {path}: {e}"))?;
        let account: ServiceAccount = serde_json::from_str(&contents)
            .map_err(|e| format!("Couldn't parse service account file {path}: {e}"))?;
        let key = EncodingKey::from_rsa_pem(account.private_key.as_bytes())
            .map_err(|e| format!("Invalid private key in service account file: {e}"))?;
        Ok(Self {
            client_email: account.client_email,
            key,
        })
    }

    pub fn sign(&self, uid: &str, client_id: &str) -> Result<String, jsonwebtoken::errors::Error> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let mut claims = serde_json::Map::new();
        claims.insert(
            String::from(SSO_CLIENT_CLAIM),
            serde_json::Value::from(client_id),
        );
        let token_claims = CustomTokenClaims {
            iss: &self.client_email,
            sub: &self.client_email,
            aud: CUSTOM_TOKEN_AUDIENCE,
            iat: now,
            exp: now + CUSTOM_TOKEN_LIFETIME_SECONDS,
            uid,
            claims,
        };
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &token_claims, &self.key)
    }
}
