use axum::{ extract::{ Json, State }, routing::{ Router, get, post }, http::{ Request, Response, StatusCode }, response::Json as JsonResponse };
use serde::{ Serialize, Deserialize };
use sqlx::{ PgPool, query, query_as };

use crate::schemas::mess_schemas::{ MessMenuEntry, MessMenu };

pub fn get_routes() -> Router<PgPool> {
    Router::new()
        .route("/mess", get(get_mess_menu))
        .route("/mess", post(add_mess_menu))
}

async fn get_mess_menu(State(pool): State<PgPool>) -> Result<JsonResponse<MessMenu>, (StatusCode, String)> {
    match query_as::<_, MessMenuEntry>(
        "SELECT day, breakfast, lunch, snacks, dinner FROM mess"
    )
        .fetch_all(&pool).await {
            Ok(mess_menu) => Ok(Json(mess_menu)),
            Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get mess_menu from database")))
        }
}

async fn add_mess_menu(State(pool): State<PgPool>, Json(mess_menu): Json<MessMenu>) -> Result<JsonResponse<MessMenu>, (StatusCode, String)> {
    match query(
        "DELETE * FROM mess"
    )
        .execute(&pool)
        .await {
            Ok(_) => {},
            Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't update mess_menu to database")))
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
            .execute(&pool)
            .await {
                Ok(_) => {},
                Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't update mess_menu to database")))
            }
    }
    Ok(Json(mess_menu))
}
