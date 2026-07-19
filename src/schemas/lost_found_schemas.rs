use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query };
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct LostFoundEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub item_name: String,
    pub description: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub added_on_timestamp: OffsetDateTime,
    pub added_by_email: String,
    pub is_found: bool,
    pub img_urls: Vec<String>
}

#[derive(Serialize, Deserialize)]
pub struct LostFoundRequest {
    pub item_name: String,
    pub description: String,
    pub base64_images: Vec<String>,
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS lostfoundentries (
            id SERIAL PRIMARY KEY,
            item_name VARCHAR(255) NOT NULL,
            description VARCHAR(1023) NOT NULL,
            added_on_timestamp TIMESTAMPTZ NOT NULL,
            added_by_email VARCHAR(255) NOT NULL,
            is_found BOOLEAN NOT NULL DEFAULT false,
            img_urls VARCHAR(255)[] NOT NULL DEFAULT '{}'
        );
    ")
        .execute(pool)
        .await
}
