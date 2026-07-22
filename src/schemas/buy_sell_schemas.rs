use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query, Type };
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct BuySellEntry {
    pub id: i32,
    pub item_name: String,
    pub description: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub added_on_timestamp: OffsetDateTime,
    pub added_by_email: String,
    #[serde(skip_deserializing)]
    pub status: BuySellStatus,
    #[sqlx(json)]
    pub bids: Vec<BidEntry>,
    pub img_urls: Vec<String>
}

#[derive(Serialize, Deserialize)]
pub struct BuySellRequest {
    pub item_name: String,
    pub description: String,
    pub base64_images: Vec<String>,
}


#[derive(Type, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[sqlx(type_name = "item_status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum BuySellStatus {
    #[default] Selling, Sold
}


#[derive(Serialize, Deserialize, FromRow)]
pub struct BidEntry {
    pub item_id: i32,
    pub item_name: String,
    #[serde(skip_deserializing)]
    pub bid_by_email: String,
    pub bid_amount_in_rs: f64,
    pub remarks: String,
    #[serde(skip_deserializing, default = "OffsetDateTime::now_utc", with = "time::serde::timestamp")]
    pub bid_timestamp: OffsetDateTime
}



pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    sqlx::query(
        "DO $$ BEGIN
            CREATE TYPE item_status as ENUM('selling', 'sold');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        "
    )
        .execute(pool)
        .await?;
    query("
        CREATE TABLE IF NOT EXISTS buysellentries (
            id SERIAL PRIMARY KEY,
            item_name VARCHAR(255) NOT NULL,
            description VARCHAR(1023) NOT NULL,
            added_on_timestamp TIMESTAMPTZ NOT NULL,
            added_by_email VARCHAR(255) NOT NULL,
            status item_status NOT NULL DEFAULT 'selling',
            bids JSONB NOT NULL DEFAULT '[]',
            img_urls VARCHAR(255)[] NOT NULL DEFAULT '{}'
        );
    ")
        .execute(pool)
        .await
}
