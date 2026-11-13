//! Cabshare endpoints, all under `/cabshare`. Every one needs a signed-in
//! IITGN account; each runs as one locked transaction in `store`.

use std::collections::HashMap;

use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::Json as JsonResponse,
    routing::{Router, delete, get, post, put},
};
use axum_extra::{
    headers::Authorization, headers::authorization::Bearer, typed_header::TypedHeader,
};
use rs_firebase_admin_sdk::jwt::TokenValidator;
use serde::Deserialize;
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::AppState;
use crate::schemas::cabshare_schemas::{self as store, Cx, Direction, Error, RideIn, Vehicle};

type Out = Result<JsonResponse<Value>, (StatusCode, String)>;
type Auth = TypedHeader<Authorization<Bearer>>;

pub fn get_routes() -> Router<AppState> {
    Router::new()
        .route("/cabshare/catalogue", get(catalogue))
        .route("/cabshare/me", get(me))
        .route("/cabshare/me/gender", put(set_gender))
        .route("/cabshare/feed", get(feed))
        .route("/cabshare/open-times", get(open_times))
        .route("/cabshare/requests", post(post_request))
        .route("/cabshare/join", post(join))
        .route("/cabshare/trips", get(my_trips))
        .route("/cabshare/trips/clear-history", post(clear_history))
        .route("/cabshare/requests/{id}", get(card))
        .route("/cabshare/requests/{id}", delete(cancel))
        .route("/cabshare/requests/{id}/dismiss", post(dismiss))
        .route("/cabshare/requests/{id}/quote", put(set_quote))
        .route("/cabshare/requests/{id}/messages", get(messages))
        .route("/cabshare/requests/{id}/messages", post(send_message))
        .route("/cabshare/requests/{id}/checkin", post(check_in))
        .route("/cabshare/requests/{id}/confirm", post(confirm))
        .route("/cabshare/requests/{id}/report", post(report))
        .route("/cabshare/blocks", get(blocks))
        .route("/cabshare/blocks", post(block))
        .route("/cabshare/blocks/remove", post(unblock))
        .route("/cabshare/notifications", get(notifications))
        .route("/cabshare/notifications/read", post(read_notifications))
        .route("/cabshare/notifications/settings", get(notify_prefs))
        .route("/cabshare/notifications/settings", put(set_notify_prefs))
}

pub fn now() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

fn fail(e: Error) -> (StatusCode, String) {
    match e {
        Error::Bad(why) => (StatusCode::BAD_REQUEST, why),
        Error::NotYours => (StatusCode::FORBIDDEN, String::from("not your request")),
        Error::NotFound(what) => (StatusCode::NOT_FOUND, what),
        Error::Db(e) => {
            log::error!("Cabshare: database error: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("Couldn't reach the cabshare database"),
            )
        }
    }
}

/// The signed-in student: an IITGN Google account, with the name Google has.
async fn student(state: &AppState, auth: &Auth) -> Result<(String, String), (StatusCode, String)> {
    let claims = state
        .firebase_token_validator
        .clone()
        .validate(auth.token().to_string())
        .await
        .map_err(|e| {
            log::error!("Cabshare: couldn't authenticate user: {e}");
            (
                StatusCode::FORBIDDEN,
                String::from("Couldn't authenticate user"),
            )
        })?;
    let email = claims
        .get("email")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_lowercase();
    if !email.ends_with("@iitgn.ac.in") {
        return Err((
            StatusCode::FORBIDDEN,
            String::from("Cabshare is for IITGN accounts only"),
        ));
    }
    let name = claims
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok((email, name))
}

async fn open(state: &AppState, auth: &Auth) -> Result<(Cx, String), (StatusCode, String)> {
    let (email, name) = student(state, auth).await?;
    let mut cx = Cx::begin(&state.pool, now()).await.map_err(fail)?;
    cx.sign_in(&email, &name).await.map_err(fail)?;
    Ok((cx, email))
}

/// Run one store call in a locked transaction and answer with its JSON.
macro_rules! run {
    ($state:expr, $auth:expr, |$cx:ident, $email:ident| $body:expr) => {{
        let (mut $cx, $email) = open(&$state, &$auth).await?;
        #[allow(unused_variables)]
        let $email = $email.as_str();
        let out = $body.map_err(fail)?;
        $cx.commit().await.map_err(fail)?;
        Ok(JsonResponse(json!(out)))
    }};
}

async fn catalogue() -> JsonResponse<Value> {
    JsonResponse(store::catalogue())
}

async fn me(State(state): State<AppState>, auth: Auth) -> Out {
    run!(state, auth, |cx, email| cx.profile(email).await)
}

#[derive(Deserialize)]
struct GenderIn {
    gender: Option<String>,
}

async fn set_gender(State(state): State<AppState>, auth: Auth, Json(body): Json<GenderIn>) -> Out {
    run!(state, auth, |cx, email| async {
        cx.set_gender(email, body.gender.as_deref()).await?;
        cx.profile(email).await
    }
    .await)
}

#[derive(Deserialize)]
struct FeedQuery {
    direction: Option<Direction>,
    to: Option<String>,
    limit: Option<usize>,
    after: Option<String>,
}

async fn feed(State(state): State<AppState>, auth: Auth, Query(q): Query<FeedQuery>) -> Out {
    run!(state, auth, |cx, email| cx
        .feed(
            email,
            q.direction,
            q.to.as_deref(),
            q.limit,
            q.after.as_deref()
        )
        .await)
}

#[derive(Deserialize)]
struct OpenQuery {
    destination: String,
    direction: Direction,
    #[serde(default)]
    handbags: i32,
    #[serde(default)]
    suitcases: i32,
    party: Option<i32>,
    vehicle: Option<Vehicle>,
}

async fn open_times(State(state): State<AppState>, auth: Auth, Query(q): Query<OpenQuery>) -> Out {
    run!(state, auth, |cx, email| cx
        .open_times(
            email,
            &q.destination,
            q.direction,
            q.handbags,
            q.suitcases,
            q.party.unwrap_or(1),
            q.vehicle.unwrap_or_default()
        )
        .await)
}

async fn post_request(State(state): State<AppState>, auth: Auth, Json(body): Json<RideIn>) -> Out {
    run!(state, auth, |cx, email| async {
        let rid = cx.post_request(email, &body).await?;
        cx.card(email, rid).await
    }
    .await)
}

async fn join(State(state): State<AppState>, auth: Auth, Json(body): Json<RideIn>) -> Out {
    run!(state, auth, |cx, email| async {
        let rid = cx.join_time(email, &body).await?;
        cx.card(email, rid).await
    }
    .await)
}

#[derive(Deserialize)]
struct TripsQuery {
    past_limit: Option<usize>,
    past_before: Option<String>,
}

async fn my_trips(State(state): State<AppState>, auth: Auth, Query(q): Query<TripsQuery>) -> Out {
    run!(state, auth, |cx, email| cx
        .my_trips(email, q.past_limit, q.past_before.as_deref())
        .await)
}

async fn clear_history(State(state): State<AppState>, auth: Auth) -> Out {
    run!(state, auth, |cx, email| cx
        .clear_history(email)
        .await
        .map(|n| json!({"cleared": n})))
}

async fn card(State(state): State<AppState>, auth: Auth, Path(id): Path<i32>) -> Out {
    run!(state, auth, |cx, email| cx.card(email, id).await)
}

async fn cancel(State(state): State<AppState>, auth: Auth, Path(id): Path<i32>) -> Out {
    run!(state, auth, |cx, email| cx
        .cancel(email, id)
        .await
        .map(|_| json!({"status": "cancelled"})))
}

async fn dismiss(State(state): State<AppState>, auth: Auth, Path(id): Path<i32>) -> Out {
    run!(state, auth, |cx, email| cx
        .dismiss_notice(email, id)
        .await
        .map(|_| json!({"status": "dismissed"})))
}

#[derive(Deserialize)]
struct QuoteIn {
    quote: Option<f64>,
}

async fn set_quote(
    State(state): State<AppState>,
    auth: Auth,
    Path(id): Path<i32>,
    Json(body): Json<QuoteIn>,
) -> Out {
    run!(state, auth, |cx, email| async {
        cx.set_quote(email, id, body.quote).await?;
        cx.card(email, id).await
    }
    .await)
}

#[derive(Deserialize)]
struct ChatQuery {
    limit: Option<i64>,
    before: Option<String>,
}

async fn messages(
    State(state): State<AppState>,
    auth: Auth,
    Path(id): Path<i32>,
    Query(q): Query<ChatQuery>,
) -> Out {
    run!(state, auth, |cx, email| cx
        .messages(email, id, q.limit, q.before.as_deref())
        .await)
}

#[derive(Deserialize)]
struct MessageIn {
    body: String,
}

async fn send_message(
    State(state): State<AppState>,
    auth: Auth,
    Path(id): Path<i32>,
    Json(body): Json<MessageIn>,
) -> Out {
    run!(state, auth, |cx, email| async {
        cx.send_message(email, id, &body.body).await?;
        cx.messages(email, id, None, None).await
    }
    .await)
}

#[derive(Deserialize)]
struct WhereIn {
    lat: f64,
    lng: f64,
}

async fn check_in(
    State(state): State<AppState>,
    auth: Auth,
    Path(id): Path<i32>,
    Json(body): Json<WhereIn>,
) -> Out {
    run!(state, auth, |cx, email| async {
        cx.check_in(email, id, body.lat, body.lng).await?;
        cx.card(email, id).await
    }
    .await)
}

async fn confirm(State(state): State<AppState>, auth: Auth, Path(id): Path<i32>) -> Out {
    run!(state, auth, |cx, email| async {
        cx.confirm_ride(email, id).await?;
        cx.card(email, id).await
    }
    .await)
}

#[derive(Deserialize)]
struct ReportIn {
    handle: String,
}

async fn report(
    State(state): State<AppState>,
    auth: Auth,
    Path(id): Path<i32>,
    Json(body): Json<ReportIn>,
) -> Out {
    run!(state, auth, |cx, email| async {
        cx.report_no_show(email, id, &body.handle).await?;
        cx.card(email, id).await
    }
    .await)
}

async fn blocks(State(state): State<AppState>, auth: Auth) -> Out {
    run!(state, auth, |cx, email| cx
        .blocked(email)
        .await
        .map(|b| json!({"blocked": b})))
}

#[derive(Deserialize)]
struct BlockIn {
    email: String,
}

async fn block(State(state): State<AppState>, auth: Auth, Json(body): Json<BlockIn>) -> Out {
    run!(state, auth, |cx, email| async {
        cx.block(email, &body.email).await?;
        cx.blocked(email).await.map(|b| json!({"blocked": b}))
    }
    .await)
}

async fn unblock(State(state): State<AppState>, auth: Auth, Json(body): Json<BlockIn>) -> Out {
    run!(state, auth, |cx, email| async {
        cx.unblock(email, &body.email).await?;
        cx.blocked(email).await.map(|b| json!({"blocked": b}))
    }
    .await)
}

#[derive(Deserialize)]
struct NotesQuery {
    limit: Option<i64>,
    before: Option<i64>,
}

async fn notifications(
    State(state): State<AppState>,
    auth: Auth,
    Query(q): Query<NotesQuery>,
) -> Out {
    run!(state, auth, |cx, email| async {
        cx.tick().await?;
        cx.notifications(email, q.limit, q.before).await
    }
    .await)
}

#[derive(Deserialize)]
struct ReadIn {
    ids: Option<Vec<i32>>,
}

async fn read_notifications(
    State(state): State<AppState>,
    auth: Auth,
    Json(body): Json<ReadIn>,
) -> Out {
    run!(state, auth, |cx, email| cx
        .read_notifications(email, body.ids)
        .await
        .map(|n| json!({"unread": n})))
}

async fn notify_prefs(State(state): State<AppState>, auth: Auth) -> Out {
    run!(state, auth, |cx, email| cx.notify_prefs(email).await)
}

async fn set_notify_prefs(
    State(state): State<AppState>,
    auth: Auth,
    Json(body): Json<HashMap<String, bool>>,
) -> Out {
    run!(state, auth, |cx, email| cx
        .set_notify_prefs(email, &body)
        .await)
}

/// Regroups at 15 minutes, settles rides, and sends reminders on time even
/// when nobody has the app open.
pub async fn run_clock(pool: sqlx::PgPool) {
    loop {
        let result = async {
            let mut cx = Cx::begin(&pool, now()).await?;
            cx.tick().await?;
            cx.commit().await
        }
        .await;
        if let Err(e) = result {
            log::error!("Cabshare: clock tick failed: {e:?}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    }
}
