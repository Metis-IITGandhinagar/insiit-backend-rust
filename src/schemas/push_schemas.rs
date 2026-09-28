use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, query};
use std::{env, time::{SystemTime, UNIX_EPOCH}};
use time::OffsetDateTime;

#[derive(serde::Deserialize)]
pub struct DeviceRequest {
    pub device_id: String,
    pub token: String,
}

#[derive(serde::Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    token_uri: String,
}

pub async fn register_device(pool: &PgPool, user_id: &str, device: &DeviceRequest) -> Result<(), sqlx::Error> {
    query("INSERT INTO push_devices(user_id, device_id, token) VALUES ($1, $2, $3) ON CONFLICT (user_id, device_id) DO UPDATE SET token = EXCLUDED.token, updated_at = NOW()")
        .bind(user_id).bind(&device.device_id).bind(&device.token).execute(pool).await?;
    // New devices are included in reminders for already approved upcoming events.
    query("INSERT INTO event_push_jobs(event_id, user_id, device_id, kind, scheduled_at) SELECT id, $1, $2, 'reminder', start_datetime - INTERVAL '1 hour' FROM events WHERE approved AND start_datetime > NOW() + INTERVAL '1 hour' ON CONFLICT DO NOTHING")
        .bind(user_id).bind(&device.device_id).execute(pool).await?;
    Ok(())
}

pub async fn remove_device(pool: &PgPool, user_id: &str, device_id: &str) -> Result<(), sqlx::Error> {
    query("DELETE FROM push_devices WHERE user_id = $1 AND device_id = $2").bind(user_id).bind(device_id).execute(pool).await?;
    query("DELETE FROM event_push_jobs WHERE user_id = $1 AND device_id = $2 AND sent_at IS NULL").bind(user_id).bind(device_id).execute(pool).await?;
    Ok(())
}

pub async fn enqueue_approval(pool: &PgPool, event_id: i32, starts_at: OffsetDateTime) -> Result<(), sqlx::Error> {
    query("INSERT INTO event_push_jobs(event_id, user_id, device_id, kind, scheduled_at) SELECT $1, user_id, device_id, 'approved', NOW() FROM push_devices ON CONFLICT DO NOTHING")
        .bind(event_id).execute(pool).await?;
    if starts_at > OffsetDateTime::now_utc() + time::Duration::hours(1) {
        query("INSERT INTO event_push_jobs(event_id, user_id, device_id, kind, scheduled_at) SELECT $1, user_id, device_id, 'reminder', $2 FROM push_devices ON CONFLICT DO NOTHING")
            .bind(event_id).bind(starts_at - time::Duration::hours(1)).execute(pool).await?;
    }
    Ok(())
}

pub async fn reschedule_reminders(pool: &PgPool, event_id: i32, starts_at: OffsetDateTime) -> Result<(), sqlx::Error> {
    if starts_at > OffsetDateTime::now_utc() + time::Duration::hours(1) {
        query("UPDATE event_push_jobs SET scheduled_at = $2, attempts = 0 WHERE event_id = $1 AND kind = 'reminder' AND sent_at IS NULL")
            .bind(event_id).bind(starts_at - time::Duration::hours(1)).execute(pool).await?;
    } else {
        query("DELETE FROM event_push_jobs WHERE event_id = $1 AND kind = 'reminder' AND sent_at IS NULL")
            .bind(event_id).execute(pool).await?;
    }
    Ok(())
}

pub async fn cancel_pending(pool: &PgPool, event_id: i32) -> Result<(), sqlx::Error> {
    query("DELETE FROM event_push_jobs WHERE event_id = $1 AND sent_at IS NULL")
        .bind(event_id).execute(pool).await?;
    Ok(())
}

pub async fn run_worker(pool: PgPool, project_id: String) {
    loop {
        if let Err(error) = process_due(&pool, &project_id).await {
            log::error!("Push worker failed: {error}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn process_due(pool: &PgPool, project_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let jobs = query("SELECT j.event_id, j.user_id, j.device_id, j.kind, d.token, e.name FROM event_push_jobs j JOIN push_devices d USING (user_id, device_id) JOIN events e ON e.id = j.event_id WHERE j.sent_at IS NULL AND j.scheduled_at <= NOW() AND e.approved ORDER BY j.scheduled_at LIMIT 100")
        .fetch_all(pool).await?;
    for job in jobs {
        let event_id: i32 = job.try_get("event_id")?;
        let user_id: String = job.try_get("user_id")?;
        let device_id: String = job.try_get("device_id")?;
        let kind: String = job.try_get("kind")?;
        let token: String = job.try_get("token")?;
        let name: String = job.try_get("name")?;
        let title = if kind == "approved" { "New event" } else { "Event reminder" };
        let body = if kind == "approved" { format!("{name} is now in the events feed") } else { format!("{name} starts in one hour") };
        match send_fcm(project_id, &token, title, &body, event_id).await {
            Ok(()) => { query("UPDATE event_push_jobs SET sent_at = NOW() WHERE event_id=$1 AND user_id=$2 AND device_id=$3 AND kind=$4 AND sent_at IS NULL").bind(event_id).bind(&user_id).bind(&device_id).bind(&kind).execute(pool).await?; }
            Err(SendError::InvalidToken) => { query("DELETE FROM push_devices WHERE user_id=$1 AND device_id=$2 AND token=$3").bind(&user_id).bind(&device_id).bind(&token).execute(pool).await?; }
            Err(SendError::Transient(error)) => { log::warn!("Push send failed; will retry: {error}"); query("UPDATE event_push_jobs SET attempts = attempts + 1, scheduled_at = NOW() + LEAST(INTERVAL '1 hour', INTERVAL '15 seconds' * power(2, LEAST(attempts, 8))) WHERE event_id=$1 AND user_id=$2 AND device_id=$3 AND kind=$4").bind(event_id).bind(&user_id).bind(&device_id).bind(&kind).execute(pool).await?; }
        }
    }
    Ok(())
}

enum SendError { InvalidToken, Transient(String) }

async fn send_fcm(project_id: &str, device_token: &str, title: &str, body: &str, event_id: i32) -> Result<(), SendError> {
    let path = env::var("GOOGLE_APPLICATION_CREDENTIALS").map_err(|e| SendError::Transient(e.to_string()))?;
    let account: ServiceAccount = serde_json::from_slice(&std::fs::read(path).map_err(|e| SendError::Transient(e.to_string()))?).map_err(|e| SendError::Transient(e.to_string()))?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| SendError::Transient(e.to_string()))?.as_secs();
    let claims = json!({"iss": account.client_email, "scope":"https://www.googleapis.com/auth/firebase.messaging", "aud":account.token_uri, "iat":now, "exp":now+3600});
    let assertion = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &EncodingKey::from_rsa_pem(account.private_key.as_bytes()).map_err(|e| SendError::Transient(e.to_string()))?).map_err(|e| SendError::Transient(e.to_string()))?;
    let client = reqwest::Client::new();
    let token_response: Value = client.post(account.token_uri).form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", assertion.as_str())]).send().await.map_err(|e| SendError::Transient(e.to_string()))?.json().await.map_err(|e| SendError::Transient(e.to_string()))?;
    let access_token = token_response.get("access_token").and_then(Value::as_str).ok_or_else(|| SendError::Transient(token_response.to_string()))?;
    let response = client.post(format!("https://fcm.googleapis.com/v1/projects/{project_id}/messages:send")).bearer_auth(access_token).json(&json!({"message":{"token":device_token,"notification":{"title":title,"body":body},"data":{"event_id":event_id.to_string()}}})).send().await.map_err(|e| SendError::Transient(e.to_string()))?;
    if response.status().is_success() { return Ok(()); }
    let status = response.status();
    let detail = response.text().await.unwrap_or_default();
    if detail.contains("UNREGISTERED") { Err(SendError::InvalidToken) }
    else { Err(SendError::Transient(format!("FCM returned {status}: {detail}"))) }
}
