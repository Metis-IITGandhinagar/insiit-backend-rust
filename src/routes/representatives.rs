use axum::{
    extract::{FromRequest, FromRequestParts, Json, Path, Request, State},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{Router, delete, get, post, put},
};
use sqlx::{query, query_as};

use crate::AppState;
use crate::auth::verify_and_execute;
use crate::schemas::admin_schemas::AdminPermission;
use crate::schemas::representatives_schemas::RepresentativeEntry;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/representatives", get(get_representatives))
        .route(
            "/representatives",
            post(verify_and_execute(
                AdminPermission::PostRepresentative,
                add_representative,
            )),
        )
        .route(
            "/representatives/{id}",
            put(verify_and_execute(
                AdminPermission::PutRepresentative,
                edit_representative,
            )),
        )
        .route(
            "/representatives/{id}",
            delete(verify_and_execute(
                AdminPermission::DeleteRepresentative,
                delete_representative,
            )),
        )
}

async fn get_representatives(
    State(state): State<AppState>,
) -> Result<JsonResponse<Vec<RepresentativeEntry>>, (StatusCode, String)> {
    match query_as::<_, RepresentativeEntry>(
        "SELECT id, name, position, email, phone FROM representatives ORDER BY id",
    )
    .fetch_all(&state.pool)
    .await
    {
        Ok(representatives) => Ok(Json(representatives)),
        Err(e) => {
            log::error!("Representatives: Error fetching representatives: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't get representatives from database"),
            ))
        }
    }
}

async fn add_representative(
    State(state): State<AppState>,
    request: Request,
    _email: String,
) -> Result<JsonResponse<RepresentativeEntry>, (StatusCode, String)> {
    let Json(representative) =
        match Json::<RepresentativeEntry>::from_request(request, &state).await {
            Ok(representative) => representative,
            Err(_e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    String::from("Invalid JSON payload"),
                ));
            }
        };

    match query_as::<_, RepresentativeEntry>(
        "INSERT INTO representatives(name, position, email, phone)
        VALUES($1, $2, $3, $4)
        RETURNING id, name, position, email, phone",
    )
    .bind(&representative.name)
    .bind(&representative.position)
    .bind(&representative.email)
    .bind(&representative.phone)
    .fetch_one(&state.pool)
    .await
    {
        Ok(new_representative) => {
            log::info!("Representatives: Added representative_entry");
            Ok(Json(new_representative))
        }
        Err(e) => {
            log::error!("Representatives: Error adding representative_entry: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't add representative to database"),
            ))
        }
    }
}

async fn edit_representative(
    State(state): State<AppState>,
    request: Request,
    _email: String,
) -> Result<JsonResponse<RepresentativeEntry>, (StatusCode, String)> {
    let (mut parts, body) = request.into_parts();
    let Path(id) = match Path::<i32>::from_request_parts(&mut parts, &state).await {
        Ok(id) => id,
        Err(_e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                String::from("Invalid representative id"),
            ));
        }
    };
    let request = Request::from_parts(parts, body);
    let Json(representative) =
        match Json::<RepresentativeEntry>::from_request(request, &state).await {
            Ok(representative) => representative,
            Err(_e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    String::from("Invalid JSON payload"),
                ));
            }
        };

    match query_as::<_, RepresentativeEntry>(
        "UPDATE representatives
        SET name = $1, position = $2, email = $3, phone = $4
        WHERE id = $5
        RETURNING id, name, position, email, phone",
    )
    .bind(&representative.name)
    .bind(&representative.position)
    .bind(&representative.email)
    .bind(&representative.phone)
    .bind(id)
    .fetch_one(&state.pool)
    .await
    {
        Ok(updated_representative) => {
            log::info!("Representatives: Edited representative_entry");
            Ok(Json(updated_representative))
        }
        Err(sqlx::error::Error::RowNotFound) => {
            log::info!("Representatives: didn't find representative_entry to edit");
            Err((StatusCode::NOT_FOUND, String::from("entry not found")))
        }
        Err(e) => {
            log::error!("Representatives: Error editing representative_entry: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't edit representative entry in the database"),
            ))
        }
    }
}

async fn delete_representative(
    State(state): State<AppState>,
    request: Request,
    _email: String,
) -> Result<JsonResponse<()>, (StatusCode, String)> {
    let (mut parts, _body) = request.into_parts();
    let Path(id) = match Path::<i32>::from_request_parts(&mut parts, &state).await {
        Ok(id) => id,
        Err(_e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                String::from("Invalid representative id"),
            ));
        }
    };
    match query("DELETE FROM representatives WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await
    {
        Ok(result) if result.rows_affected() > 0 => {
            log::info!("Representatives: Deleted representative_entry");
            Ok(Json(()))
        }
        Ok(_) => Err((StatusCode::NOT_FOUND, String::from("entry not found"))),
        Err(e) => {
            log::error!("Representatives: Error deleting representative_entry: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't delete representative entry from the database"),
            ))
        }
    }
}
