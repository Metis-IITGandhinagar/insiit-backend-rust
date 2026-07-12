use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::{ PgQueryResult }, query };
use time::OffsetDateTime;


#[derive(Serialize, Deserialize, FromRow)]
pub struct Outlet {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    #[sqlx(flatten)]
    pub location: Point,
    pub landmark: Option<String>,
    pub open_time: OffsetDateTime,
    pub close_time: OffsetDateTime,
    #[sqlx(json)]
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
    ")
        .execute(pool)
        .await
}
