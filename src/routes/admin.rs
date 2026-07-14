use axum::{ extract::{ FromRequest, Json, Request, State }, http::StatusCode, response::{ Json as JsonResponse }, routing:: { Router, get, post} };
use sqlx::{ query, query_as };


use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::{ AdminEntry, AdminPermission };

pub fn get_routes() -> Router<AppState> {
    Router::new()
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

pub async fn add_admin(State(state): State<AppState>, request: Request) -> Result<JsonResponse<AdminEntry>, (StatusCode, String)> {
    let Json(admin) = match Json::<AdminEntry>::from_request(request, &state).await {
        Ok(admin) => admin,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    match query(
        "INSERT INTO admins(email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlet, delete_outlet, put_outlet) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
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

