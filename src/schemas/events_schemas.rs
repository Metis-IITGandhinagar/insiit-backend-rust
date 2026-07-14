use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query };
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct EventEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub poster_base64: Option<String>,
    pub added_by_email: String,
    pub address: Option<String>,
    pub start_datetime: OffsetDateTime
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            poster_base64 TEXT,
            added_by_email VARCHAR(255) NOT NULL,
            address TEXT,
            start_datetime TIMESTAMPTZ NOT NULL
        );
    ")
        .execute(pool)
        .await
}
