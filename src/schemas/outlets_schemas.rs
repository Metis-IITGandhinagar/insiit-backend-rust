use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::{ PgQueryResult }, query };
use time::OffsetDateTime;


#[derive(Serialize, Deserialize, FromRow)]
pub struct Outlet {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    #[sqlx(flatten)]
    pub location: Point,
    pub landmark: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub open_time: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub close_time: OffsetDateTime,
    #[sqlx(json)]
    pub menu: Vec<OutletMenuEntry>,
    pub image_url: Option<String>
}

#[derive(Serialize, Deserialize)]
pub struct OutletRequest {
    pub name: String,
    pub description: Option<String>,
    pub location: Point,
    pub landmark: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub open_time: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub close_time: OffsetDateTime,
    pub menu: Vec<OutletMenuEntry>,
    pub base64_image: Option<String>
}

#[derive(Serialize, Deserialize, FromRow)]
pub struct OutletMenuEntry {
    pub name: String,
    pub price: f64
}

#[derive(Serialize, Deserialize, FromRow)]
pub struct Point {
    pub latitude: f64,
    pub longitude:f64
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS outlets (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            landmark TEXT,
            open_time TIMESTAMPTZ NOT NULL,
            close_time TIMESTAMPTZ NOT NULL,
            menu JSONB NOT NULL DEFAULT '[]',
            image_url TEXT
        );
    ")
        .execute(pool)
        .await
}
