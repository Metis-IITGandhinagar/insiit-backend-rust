use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow };


use crate::schemas::common_structs;

#[derive(Clone, Serialize, Deserialize, FromRow)]
pub struct MessMenuEntry {
    pub day: i32,
    pub breakfast: Vec<String>,
    pub lunch: Vec<String>,
    pub snacks: Vec<String>,
    pub dinner: Vec<String>
}

pub type MessMenu = Vec<MessMenuEntry>;
