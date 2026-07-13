use axum::{ extract::{ Json, State }, routing:: { Router, get, post, delete, put }, http::StatusCode, response::Json as JsonResponse };
use sqlx::{ PgPool, query, query_as };


use crate::AppState;
use crate::schemas::admin_schemas::AdminEntry;

pub fn get_routes() -> Router<(AppState)> {
    Router::new()
        .route("/admin", get(get_admins))
        .route("/admin", post(add_admin))
}

async fn get_admins(State(state): State<AppState>) -> Result<JsonResponse<Vec<AdminEntry>>, (StatusCode, String)> {
    match query_as::<_, AdminEntry>(
        "SELECT email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlet, delete_outlet, put_outlet from admins;"
    )
        .fetch_all(&state.pool).await {
            Ok(admins) => Ok(Json(admins)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't fetch admins from database")))
        }
}

pub async fn add_admin(State(state): State<AppState>, Json(admin): Json<AdminEntry>) -> Result<JsonResponse<AdminEntry>, (StatusCode, String)> {
    match query(
        "INSERT INTO admins(email, get_admin, post_admin, put_admin, post_bus_schedule, put_bus_schedule, post_event, delete_event, put_event, post_mess_menu, post_outlet, delete_outlet, put_outlet from admins) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
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

