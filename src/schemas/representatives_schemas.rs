use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgQueryResult, query};

#[derive(Serialize, Deserialize, FromRow)]
pub struct RepresentativeEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    pub position: String,
    pub email: String,
    pub phone: String,
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query(
        "
        CREATE TABLE IF NOT EXISTS representatives (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            position TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL
        );
    ",
    )
    .execute(pool)
    .await
}
