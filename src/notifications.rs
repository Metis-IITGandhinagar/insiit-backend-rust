//! Delivery of queued event notifications through Firebase Cloud Messaging.
//!
//! Handlers only ever write rows to `event_push_jobs`; the worker started in
//! `main` is what turns those rows into pushes, so a slow or unreachable FCM
//! never holds up a request.

use axum::http::Extensions;
use google_cloud_auth::credentials::CacheableResource;
use rs_firebase_admin_sdk::{App, Credentials};
use serde_json::json;
use sqlx::PgPool;

use crate::schemas::push_schemas::{self, PushJob};

/// How often the worker looks for due notifications.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// Notifications delivered per poll, so one backlog can't monopolise the pool.
const BATCH_SIZE: i64 = 100;

pub struct Notifier {
    client: reqwest::Client,
    credentials: Credentials,
    project_id: String,
}

enum SendError {
    /// FCM no longer recognises the token, so the device should be forgotten.
    UnknownDevice,
    /// Anything worth another attempt later.
    Transient(String),
}

impl Notifier {
    /// Borrows the credentials the Firebase app already authenticated with.
    ///
    /// They carry the `cloud-platform` scope that FCM needs, and they cache and
    /// refresh access tokens internally, so every send doesn't pay for a fresh
    /// token exchange.
    pub fn new(app: &App, project_id: String) -> Self {
        let credentials = match app {
            App::Live { credentials, .. } => credentials.clone(),
            App::Emulated { .. } => {
                panic!("The Firebase emulator can't send push notifications")
            }
        };
        Self {
            client: reqwest::Client::new(),
            credentials,
            project_id,
        }
    }

    async fn send(&self, job: &PushJob) -> Result<(), SendError> {
        let headers = match self.credentials.headers(Extensions::new()).await {
            // Without an entity tag the credentials always hand back fresh headers.
            Ok(CacheableResource::New { data, .. }) => data,
            Ok(CacheableResource::NotModified) => {
                return Err(SendError::Transient(String::from(
                    "Credentials reported unchanged headers without being asked to",
                )));
            }
            Err(e) => return Err(SendError::Transient(format!("{e}"))),
        };
        let response = self
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
                    "data": { "event_id": job.event_id.to_string() }
                }
            }))
            .send()
            .await
            .map_err(|e| SendError::Transient(format!("{e}")))?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
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

pub async fn run_worker(pool: PgPool, notifier: Notifier) {
    loop {
        if let Err(e) = deliver_due(&pool, &notifier).await {
            log::error!("Notifications: Couldn't deliver due notifications: {e}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn deliver_due(pool: &PgPool, notifier: &Notifier) -> Result<(), sqlx::Error> {
    for job in push_schemas::due_jobs(pool, BATCH_SIZE).await? {
        match notifier.send(&job).await {
            Ok(()) => push_schemas::mark_sent(pool, &job).await?,
            Err(SendError::UnknownDevice) => {
                log::info!("Notifications: Forgetting device FCM no longer recognises");
                // The job rows cascade away with the device.
                push_schemas::remove_device(pool, &job.user_email, &job.device_id).await?;
            }
            Err(SendError::Transient(e)) => {
                log::warn!("Notifications: Delivery failed, will retry: {e}");
                push_schemas::record_failed_attempt(pool, &job).await?;
            }
        }
    }
    Ok(())
}
