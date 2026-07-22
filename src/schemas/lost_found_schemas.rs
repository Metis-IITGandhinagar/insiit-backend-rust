use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query, Type };
use time::OffsetDateTime;

// WARNING / BUG: status has skip_deserializing, but it's an enum without default value. In the
// routes file, check what value comes when a request is sent.
#[derive(Serialize, Deserialize, FromRow)]
pub struct LostFoundEntry {
    pub id: i32,
    pub item_name: String,
    pub description: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub added_on_timestamp: OffsetDateTime,
    pub added_by_email: String,
    #[serde(skip_deserializing)]
    pub status: LostFoundStatus,
    #[sqlx(json)]
    pub found_claims: Vec<LostFoundClaim>,
    pub img_urls: Vec<String>
}

#[derive(Serialize, Deserialize)]
pub struct LostFoundRequest {
    pub item_name: String,
    pub description: String,
    pub base64_images: Vec<String>,
}


#[derive(Serialize, Deserialize, FromRow)]
pub struct LostFoundClaim {
    pub id: i32,
    pub item_name: String,
    #[serde(skip_deserializing)]
    pub claimed_by_email: String,
    pub remarks: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub claim_timestamp: OffsetDateTime
}

#[derive(Type, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[sqlx(type_name = "item_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum LostFoundStatus {
    #[default] Lost, Found, ClaimedToBeFound
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::query(
        "DO $$ BEGIN
            CREATE TYPE item_status as ENUM('lost', 'found', 'claimed_to_be_found');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        "
    )
        .execute(pool)
        .await?;
    query("
        CREATE TABLE IF NOT EXISTS lostfoundentries (
            id SERIAL PRIMARY KEY,
            item_name VARCHAR(255) NOT NULL,
            description VARCHAR(1023) NOT NULL,
            added_on_timestamp TIMESTAMPTZ NOT NULL,
            added_by_email VARCHAR(255) NOT NULL,
            status item_status NOT NULL DEFAULT 'lost',
            found_claims JSONB NOT NULL DEFAULT '[]',
            img_urls VARCHAR(255)[] NOT NULL DEFAULT '{}'
        );
    ")
        .execute(pool)
        .await
}
