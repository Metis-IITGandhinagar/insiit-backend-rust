use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow };


#[derive(Serialize, Deserialize)]
pub enum Day {
    Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String
}
