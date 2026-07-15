use axum::{ extract::{ FromRequest, Json, Request, State }, http::StatusCode, response::{ Json as JsonResponse }, routing:: { Router, get, post} };
use axum_extra::{ headers:: { authorization::Bearer, Authorization }, TypedHeader };
use rs_firebase_admin_sdk::jwt::TokenValidator;
use sqlx::{ query, query_as };


use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::{ AdminEntry, AdminPermission };

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/admin/permissions", get(get_admin_permissions))
        .route("/admin", get(verify_and_execute(AdminPermission::GetAdmin, get_admins)))
        .route("/admin", post(verify_and_execute(AdminPermission::PostAdmin, add_admin)))
}

async fn get_admins(State(state): State<AppState>, _request: Request) -> Result<JsonResponse<Vec<AdminEntry>>, (StatusCode, String)> {
    match query_as::<_, AdminEntry>(
        "SELECT email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlet, delete_outlet, put_outlet FROM admins;"
    )
        .fetch_all(&state.pool).await {
            Ok(admins) => Ok(Json(admins)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't fetch admins from database")))
        }
}


async fn get_admin_permissions(State(state): State<AppState>, TypedHeader(auth_header): TypedHeader<Authorization<Bearer>>) -> Result<JsonResponse<AdminEntry>, (StatusCode, String)> {
    let token = auth_header.token().to_string();
    let user  = match state.firebase_token_validator.clone().validate(token).await {
        Ok(user) => user,
        Err(e) => {
            log::info!("User not found by validator: {e}");
            return Err((StatusCode::FORBIDDEN, String::from("Invalid user")))
        }
    };
    let email = match user.get("email") {
        Some(value) => match value.as_str() {
            Some(email) => email,
            None => {
                log::error!("This just shouldn't ha
                    ppen ever, a user email should always be convertable to str");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("This shouldn't happen in any case")));
            }
        },
        None => {
            log::info!("User entry in firebase doesn't have an email");
            return Err((StatusCode::FORBIDDEN, String::from("Invalid user")))
        }
    };
    // Check if this query breaks, cause if admin doesn't exist, then AdminEntry value may not be
    // fetched from the db and it may return Err, and return internal server error to client
    // rather than forbidden
    match query_as::<_, AdminEntry>(
        "SELECT email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlets, delete_outlet, put_outlet FROM admins WHERE email = $1"
    )
        .bind(email)
        .fetch_one(&state.pool).await {
            Ok(admin) => Ok(Json(admin)),
            Err(e) => {
                log::error!("Failed to fetch admin: {e}");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Failed getting admin permissions from database")))
            }
        }
}

pub async fn add_admin(State(state): State<AppState>, request: Request) -> Result<JsonResponse<AdminEntry>, (StatusCode, String)> {
    let Json(admin) = match Json::<AdminEntry>::from_request(request, &state).await {
        Ok(admin) => admin,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    match query(
        "INSERT INTO admins(email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlet, delete_outlet, put_outlet) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
    )
        .bind(&admin.email)
        .bind(&admin.permissions.get_admin)
        .bind(&admin.permissions.post_admin)
        .bind(&admin.permissions.put_admin)
        .bind(&admin.permissions.post_bus_schedule)
        .bind(&admin.permissions.put_bus_schedule)
        .bind(&admin.permissions.post_event)
        .bind(&admin.permissions.delete_event)
        .bind(&admin.permissions.put_event)
        .bind(&admin.permissions.post_mess_menu)
        .bind(&admin.permissions.post_outlet)
        .bind(&admin.permissions.delete_outlet)
        .bind(&admin.permissions.put_outlet)
        .execute(&state.pool).await {
            Ok(_) => Ok(Json(admin)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add admin to admin to the database")))
        }
}

