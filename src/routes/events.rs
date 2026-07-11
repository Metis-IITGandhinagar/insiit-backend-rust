use axum::{ routing:: { Router, get, post } , http:: { Request, Response } };
use serde::{ Serialize, Deserialize };

pub fn add_routes(router: Router) -> Router {
    router
        .route("/events", get(get_events))
        .route("/events", post(add_event))
}

async fn get_events() -> String {
    "hi".to_string()
}

async fn add_event(request: axum::Json<Event>) -> axum::http::Response<String> {
    axum::http::Response::<String>::new("add an event".to_string())
}

#[derive(Serialize, Deserialize)]
struct Event {

}

