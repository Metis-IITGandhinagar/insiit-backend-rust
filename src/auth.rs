use axum::{ extract:: { FromRequestParts, Request, State }, http::StatusCode, response::IntoResponse };
use axum_extra::{ headers:: { authorization::Bearer, Authorization }, TypedHeader };
use std::{ future::Future, pin::Pin };

use crate::AppState;
use crate::schemas::admin_schemas::{ AdminPermission };

pub fn verify_and_execute<H, F, K>(
    permission: AdminPermission,
    handler: H
) -> impl Clone + Fn(State<AppState>, Request) -> Pin<Box<dyn Future<Output = Result<K, (StatusCode, String)>> + Send>>
where
    H: Fn(State<AppState>, Request) -> F + Clone + Send + Sync + 'static,
    K: IntoResponse,
    F: Future<Output = Result<K, (StatusCode, String)>> + Send + 'static, {
        move |State(state): State<AppState>, request: Request| {
            let handler = handler.clone();
            let state_clone = state.clone();
            let permission_clone = permission.clone();
            let (mut parts, body) = request.into_parts();
            Box::pin(async move {
                let auth_header = match TypedHeader::<Authorization<Bearer>>::from_request_parts(&mut parts, &state_clone).await {
                    Ok(auth_header) => auth_header,
                    Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid token")))
                };
                match permission_clone.granted_to(auth_header.token().to_string(), state_clone.clone()).await {
                    Ok(true) => handler(State(state_clone), Request::from_parts(parts, body)).await,
                    Ok(false) => Err((StatusCode::FORBIDDEN, String::from("Forbidden"))),
                    Err(e) => {
                        log::error!("Couldn't authenticate user {e}");
                        return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't authenticate user")));
                    }
                }
            })
        }
    }
