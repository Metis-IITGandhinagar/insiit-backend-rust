use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query };
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, FromRow)]
pub struct BusEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    #[sqlx(json)]
    pub source: BusStop,
    #[sqlx(json)]
    pub via: Vec<BusStop>,
    #[sqlx(json)]
    pub destination: BusStop
}
#[derive(Serialize, Deserialize, FromRow)]
pub struct BusStop {
    #[serde(with = "time::serde::rfc3339")]
    pub time: OffsetDateTime,
    pub location: String
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS bus (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            source JSONB NOT NULL,
            via JSONB NOT NULL,
            destination JSONB NOT NULL
        );
    ")
        .execute(pool)
        .await
}
