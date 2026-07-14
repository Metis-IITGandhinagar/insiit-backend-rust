use axum::{ extract::{ FromRequest, Json, Request, State }, routing:: { Router, get, post }, http::StatusCode, response::Json as JsonResponse };
use sqlx::{ query, query_as };

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::mess_schemas::{ MessMenuEntry, MessMenu };

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/mess", get(get_mess_menu))
        .route("/mess", post(verify_and_execute(AdminPermission::PostMessMenu, add_mess_menu)))
}

async fn get_mess_menu(State(state): State<AppState>) -> Result<JsonResponse<MessMenu>, (StatusCode, String)> {
    match query_as::<_, MessMenuEntry>(
        "SELECT day, breakfast, lunch, snacks, dinner FROM mess"
    )
        .fetch_all(&state.pool).await {
            Ok(mess_menu) => Ok(Json(mess_menu)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get mess_menu from database")))
        }
}

async fn add_mess_menu(State(state): State<AppState>, request: Request) -> Result<JsonResponse<MessMenu>, (StatusCode, String)> {
    let Json(mess_menu) = match Json::<MessMenu>::from_request(request, &state).await {
        Ok(mess_menu) => mess_menu,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(_) => return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Failed to connect to database"))),
    };

    if let Err(_) = query("TRUNCATE TABLE mess;")
        .execute(&mut *tx)
        .await
    {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't clear old mess_menu")));
    }
    for entry in &mess_menu {
        match query(
            "INSERT INTO mess(day, breakfast, lunch, snacks, dinner) VALUES($1, $2, $3, $4, $5)"
        )
            .bind(&entry.day)
            .bind(&entry.breakfast)
            .bind(&entry.lunch)
            .bind(&entry.snacks)
            .bind(&entry.dinner)
            .execute(&mut *tx)
            .await {
                Ok(_) => {},
                Err(_e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't update mess_menu to database")))
            }
    }
    match tx.commit().await {
        Ok(_) => Ok(Json(mess_menu)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to commit: {e}"))),
    }
}
