use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgQueryResult, query};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct EventEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub added_by_email: String,
    pub address: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub start_datetime: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub end_datetime: Option<OffsetDateTime>,
    #[serde(skip_deserializing)]
    pub approved: bool,
}

#[derive(Serialize, Deserialize)]
pub struct EventRequest {
    pub name: String,
    pub description: Option<String>,
    pub poster_base64: Option<String>,
    pub address: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub start_datetime: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub end_datetime: Option<OffsetDateTime>,
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query(
        "
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            poster_url TEXT,
            added_by_email VARCHAR(255) NOT NULL,
            address TEXT,
            start_datetime TIMESTAMPTZ NOT NULL,
            end_datetime TIMESTAMPTZ,
            approved BOOLEAN NOT NULL DEFAULT FALSE
        );
    ",
    )
    .execute(pool)
    .await?;
    query("ALTER TABLE events ADD COLUMN IF NOT EXISTS end_datetime TIMESTAMPTZ;")
        .execute(pool)
        .await?;
    query("ALTER TABLE events ADD COLUMN IF NOT EXISTS approved BOOLEAN NOT NULL DEFAULT TRUE;")
        .execute(pool)
        .await?;
    query("ALTER TABLE events ALTER COLUMN approved SET DEFAULT FALSE;")
        .execute(pool)
        .await?;
    query(r#"CREATE TABLE IF NOT EXISTS push_devices (
            user_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, device_id)
        );"#).execute(pool).await?;
    query(r#"CREATE TABLE IF NOT EXISTS event_push_jobs (
            event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            user_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('approved', 'reminder')),
            scheduled_at TIMESTAMPTZ NOT NULL,
            sent_at TIMESTAMPTZ,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (event_id, user_id, device_id, kind)
        );"#).execute(pool).await?;
    query("CREATE INDEX IF NOT EXISTS event_push_jobs_due_idx ON event_push_jobs (scheduled_at) WHERE sent_at IS NULL")
        .execute(pool).await
}
