use axum::{ extract:: { Json, Request, State }, http::StatusCode, response::Json as JsonResponse, response::Response, middleware::{ self, Next } };
use rs_firebase_admin_sdk::{ auth::FirebaseAuth, client::ReqwestApiClient };
use axum_extra::{ headers:: { authorization::Bearer, Authorization }, TypedHeader };
use sqlx::PgPool;

use crate::schemas::admin_schemas::{ AdminEntry, AdminPermission, AdminPermissions };

pub async fn check_permission(TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>, State(pool): State<PgPool>, State(permission): State<AdminPermission>, State(auth_service): State<FirebaseAuth<ReqwestApiClient>>, request: Request, next: Next) -> Result<Response, (StatusCode, String)> {
    match permission.granted_to(auth_header.token().to_string(), auth_service, &pool).await {
        Ok(true) => Ok(next.run(request).await),
        Ok(false) => Err((StatusCode::UNAUTHORIZED, String::from("Unauthorized"))),
        _ => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't authorize user")))
    }
}
