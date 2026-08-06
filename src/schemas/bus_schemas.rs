use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query };

/// A recurring daily service, so a departure has a clock time and no meaningful date.
///
/// `departure_time` is a Postgres `TIME` column read and written through `::text` /
/// `::time` casts, which keeps this a plain `String` without any `time::Time` serde
/// plumbing. Postgres does the validating and normalising: `'7:30'`, `'07:30'` and
/// `'7:30 AM'` all land as `"07:30:00"`, and anything unparseable is rejected at insert
/// rather than breaking a phone at render time.
#[derive(Serialize, Deserialize, FromRow)]
pub struct BusEntry {
    #[serde(skip_deserializing)]
    pub id: i32,
    pub name: String,
    /// Always `HH:MM:SS` on the wire.
    pub departure_time: String,
    pub source: String,
    pub destination: String,
    /// Intermediate stops in order, without times.
    pub stops: Vec<String>
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query("
        CREATE TABLE IF NOT EXISTS bus (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            departure_time TIME NOT NULL,
            source VARCHAR(255) NOT NULL,
            destination VARCHAR(255) NOT NULL,
            stops TEXT[] NOT NULL DEFAULT '{}'
        );
    ")
        .execute(pool)
        .await
}
