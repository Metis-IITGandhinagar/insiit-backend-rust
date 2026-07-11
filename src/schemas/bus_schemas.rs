use serde::{ Serialize, Deserialize };
use serde_json;
use sqlx::{ FromRow, types::Json };
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
    #[serde(with = "time::serde::timestamp")]
    time: OffsetDateTime,
    location: String
}

impl From<serde_json::Value> for BusStop {
    fn from(value: serde_json::Value) -> Self {
        serde_json::from_value(value).expect("Database JSON is malformed")
    }
}
