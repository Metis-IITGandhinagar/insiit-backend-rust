use axum::{ extract::{ FromRequest, Json, Request, State }, routing:: { Router, get, post }, http::StatusCode, response::Json as JsonResponse };
use sqlx::{ query, query_as };

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::bus_schemas::BusEntry;

// TODO: In future, move from String errors to a good error enums
// TODO: Add logging in Err(e) case. Log to server, don't send them to client
pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/bus", get(get_bus))
        .route("/bus", post(verify_and_execute(AdminPermission::PostBusSchedule, add_bus)))
}

async fn get_bus(State(state): State<AppState>) -> Result<JsonResponse<Vec<BusEntry>>, (StatusCode, String)> {
    match query_as::<_, BusEntry>(
        "SELECT id, name, source, via, destination FROM bus;"
    )
        .fetch_all(&state.pool).await {
            Ok(bus_schedule) => Ok(Json(bus_schedule)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get bus_schedule from database")))
        }
}

async fn add_bus(State(state): State<AppState>, request: Request) -> Result<JsonResponse<BusEntry>, (StatusCode, String)> {
    let Json(bus_entry) = match Json::<BusEntry>::from_request(request, &state).await {
        Ok(bus_entry) => bus_entry,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    match query(
        "INSERT INTO bus (name, source, via, destination) VALUES($1, $2::jsonb, $3::jsonb, $4::jsonb)"
    )
        .bind(&bus_entry.name)
        .bind(serde_json::to_value(&bus_entry.source).expect("will only be invoked if payload is properly structured"))
        .bind(serde_json::to_value(&bus_entry.via).expect("will only be invoked if payload is properly structured"))
        .bind(serde_json::to_value(&bus_entry.destination).expect("will only be invoked if payload is properly structured"))
        .execute(&state.pool).await {
            Ok(_) => Ok(Json(bus_entry)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add bus_entry to database")))
    }
}
