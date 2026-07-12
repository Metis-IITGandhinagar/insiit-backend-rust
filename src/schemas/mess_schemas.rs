use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query };


#[derive(Serialize, Deserialize, FromRow)]
pub struct MessMenuEntry {
    pub day: i32,
    pub breakfast: Vec<String>,
    pub lunch: Vec<String>,
    pub snacks: Vec<String>,
    pub dinner: Vec<String>
}

pub type MessMenu = Vec<MessMenuEntry>;


pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS mess (
            day INTEGER PRIMARY KEY,
            breakfast TEXT[] NOT NULL DEFAULT '{}',
            lunch TEXT[] NOT NULL DEFAULT '{}',
            snacks TEXT[] NOT NULL DEFAULT '{}',
            dinner TEXT[] NOT NULL DEFAULT '{}'
        );
    ")
        .execute(pool)
        .await
}
