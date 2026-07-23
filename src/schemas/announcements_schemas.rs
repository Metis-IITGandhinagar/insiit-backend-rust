use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query, Type };
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct AnnouncementEntry {
    pub id: i32,
    pub title: String,
    pub description: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub added_on_timestamp: OffsetDateTime,
    pub added_by_email: String,
    pub img_url: String
}


#[derive(Serialize, Deserialize)]
pub struct AnnouncementRequest {
    pub title: String,
    pub description: String,
    pub img_base64: Option<String>,
    // BUG/WARNING/TODO get email not from request header but from auth header token
    pub added_by_email: String
}


pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS announcements (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description VARCHAR(1023) NOT NULL,
            added_on_timestamp TIMESTAMPTZ NOT NULL,
            added_by_email VARCHAR(255) NOT NULL,
            img_url VARCHAR(255)
        );
    ")
        .execute(pool)
        .await
}
