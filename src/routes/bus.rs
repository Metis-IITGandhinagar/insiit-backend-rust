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
        .route("/buses", get(get_bus))
        .route("/buses", post(verify_and_execute(AdminPermission::PostBusSchedule, add_bus)))
}

async fn get_bus(State(state): State<AppState>) -> Result<JsonResponse<Vec<BusEntry>>, (StatusCode, String)> {
    match query_as::<_, BusEntry>(
        // ::text so the TIME column arrives as "HH:MM:SS" without any time::Time plumbing.
        "SELECT id, name, departure_time::text AS departure_time, source, destination, stops
        FROM bus
        ORDER BY departure_time;"
    )
        .fetch_all(&state.pool).await {
            Ok(bus_schedule) => Ok(Json(bus_schedule)),
            Err(_e) => Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't get bus_schedule from database")))
        }
}

async fn add_bus(State(state): State<AppState>, request: Request, _email: String) -> Result<JsonResponse<BusEntry>, (StatusCode, String)> {
    let Json(bus_entry) = match Json::<BusEntry>::from_request(request, &state).await {
        Ok(bus_entry) => bus_entry,
        Err(_e) => return Err((StatusCode::BAD_REQUEST, String::from("Invalid JSON payload"))),
    };
    match query_as::<_, BusEntry>(
        // $2::time makes Postgres validate and normalise the clock string on the way in.
        "INSERT INTO bus (name, departure_time, source, destination, stops)
        VALUES($1, $2::time, $3, $4, $5)
        RETURNING id, name, departure_time::text AS departure_time, source, destination, stops"
    )
        .bind(&bus_entry.name)
        .bind(&bus_entry.departure_time)
        .bind(&bus_entry.source)
        .bind(&bus_entry.destination)
        .bind(&bus_entry.stops)
        .fetch_one(&state.pool).await {
            Ok(new_bus_entry) => Ok(Json(new_bus_entry)),
            Err(e) => {
                // Postgres class 22 is data exception — here, a departure_time the
                // ::time cast couldn't parse. That's the caller's mistake, not ours.
                let is_bad_input = e
                    .as_database_error()
                    .and_then(|db_err| db_err.code())
                    .is_some_and(|code| code.starts_with("22"));

                if is_bad_input {
                    log::info!("Bus: rejected bus_entry with invalid field: {e}");
                    return Err((
                        StatusCode::BAD_REQUEST,
                        String::from("Invalid departure_time — use a clock time such as \"07:30\"")
                    ));
                }

                log::error!("Bus: Couldn't add bus_entry: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, String::from("Couldn't add bus_entry to database")))
            }
    }
}
