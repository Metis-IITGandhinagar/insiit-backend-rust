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
            approved BOOLEAN NOT NULL DEFAULT FALSE
        );
    ",
    )
    .execute(pool)
    .await?;
    query("ALTER TABLE events ADD COLUMN IF NOT EXISTS approved BOOLEAN NOT NULL DEFAULT TRUE;")
        .execute(pool)
        .await?;
    query("ALTER TABLE events ALTER COLUMN approved SET DEFAULT FALSE;")
        .execute(pool)
        .await
}
