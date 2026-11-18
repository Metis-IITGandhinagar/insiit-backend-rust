use axum::http::Extensions;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use google_cloud_auth::credentials::CacheableResource;
use rs_firebase_admin_sdk::{App, Credentials};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;

use crate::schemas::push_schemas::PushJob;

static IMAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn save_image(base64_data: &String, image_directory_path: &String) -> Result<String, ()> {
    let clean_base64 = if let Some(pos) = base64_data.find(",") {
        &base64_data[pos + 1..]
    } else {
        base64_data
    };
    let image_bytes = match BASE64.decode(clean_base64) {
        Ok(bytes) => bytes,
        Err(_) => return Err(()),
    };
    let unique = IMAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let filename = format!("{timestamp}-{unique}");
    let mut file = match tokio::fs::File::create(format!("{image_directory_path}/{filename}")).await
    {
        Ok(file) => file,
        Err(_) => return Err(()),
    };
    match file.write_all(&image_bytes).await {
        Ok(_) => Ok(format!("images/{filename}")),
        Err(_) => Err(()),
    }
}

pub struct Notifier {
    client: reqwest::Client,
    credentials: Credentials,
    project_id: String,
}

pub enum SendError {
    /// FCM no longer recognises the token, so the device should be forgotten.
    UnknownDevice,
    Transient(String),
}

impl Notifier {
    // Reuses the Firebase app's credentials, which cache and refresh FCM access tokens.
    pub fn new(app: &App, project_id: String) -> Self {
        let credentials = match app {
            App::Live { credentials, .. } => credentials.clone(),
            App::Emulated { .. } => panic!("The Firebase emulator can't send push notifications"),
        };
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
        {
            Ok(client) => client,
            Err(e) => panic!("Couldn't configure FCM HTTP client: {e}"),
        };
        Self {
            client,
            credentials,
            project_id,
        }
    }

    pub async fn send(&self, job: &PushJob) -> Result<(), SendError> {
        let headers = match self.credentials.headers(Extensions::new()).await {
            Ok(CacheableResource::New { data, .. }) => data,
            // Only returned when an entity tag is passed, which we never do.
            Ok(CacheableResource::NotModified) => {
                return Err(SendError::Transient(String::from(
                    "Credentials reported unchanged headers without being asked to",
                )));
            }
            Err(e) => return Err(SendError::Transient(format!("{e}"))),
        };
        let response = match self
            .client
            .post(format!(
                "https://fcm.googleapis.com/v1/projects/{}/messages:send",
                self.project_id
            ))
            .headers(headers)
            .json(&json!({
                "message": {
                    "token": job.token,
                    "notification": { "title": job.title(), "body": job.body() },
                    "data": { "event_id": job.event_id.to_string() },
                    "android": { "notification": { "channel_id": "events_announcements" } }
                }
            }))
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => return Err(SendError::Transient(format!("{e}"))),
        };
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let detail = response.text().await.unwrap_or_default();
        // FCM reports a token it has dropped as 404, or as UNREGISTERED in the body.
        if status == reqwest::StatusCode::NOT_FOUND || detail.contains("UNREGISTERED") {
            Err(SendError::UnknownDevice)
        } else {
            Err(SendError::Transient(format!(
                "FCM returned {status}: {detail}"
            )))
        }
    }
}
