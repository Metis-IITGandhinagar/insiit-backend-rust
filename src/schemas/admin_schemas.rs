use rs_firebase_admin_sdk::jwt::TokenValidator;
use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query, query_as };

use crate::AppState;

#[derive(Serialize, Deserialize, FromRow, Default)]
pub struct AdminEntry {
    pub email: String,
    #[sqlx(flatten)]
    pub permissions: AdminPermissions,
}

#[derive(Clone, Serialize, Deserialize, FromRow, Default)]
pub struct AdminPermissions {
    pub get_admin: bool,
    pub post_admin: bool,
    pub put_admin: bool,
    pub post_bus_schedule: bool,
    pub put_bus_schedule: bool,
    pub post_event: bool,
    pub put_event: bool,
    pub post_mess_menu: bool,
    pub post_outlet: bool,
    pub delete_outlet: bool,
    pub put_outlet: bool,
    pub post_announcement: bool,
}

#[derive(Clone, Debug, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum AdminPermission {
    GetAdmin, PostAdmin, PutAdmin, PostBusSchedule, PutBusSchedule, PostEvent, PutEvent, PostMessMenu, PostOutlet, DeleteOutlet, PutOutlet, PostAnnouncement
}

impl AdminPermission {
    pub async fn granted_to(&self, token: String, state: AppState) -> Result<bool, String> {
        let validator = state.firebase_token_validator;
        let pool = &state.pool;
        let user = match validator.clone().validate(token).await {
            Ok(user) => user,
            Err(e) => {
                log::error!("Failed to authorize user: {e}");
                return Err(String::from("Could not authorize user"))
            }
        };
        let email = match user.get("email") {
            Some(value) => match value.as_str() {
                Some(email) => email,
                None => return Err(String::from("Invalid user")),
            },
            None => return Err(String::from("Invalid user")),
        };
        // Check if this query breaks, cause if admin doesn't exist, then bool value may not be
        // fetched from the db and it may return Err, and return internal server error to client
        // rather than forbidden
        let sql = format!("SELECT {} FROM admins WHERE email = $1", self);
        match query_as::<_, (bool,)>(sqlx::AssertSqlSafe(sql))
            .bind(email)
            .fetch_one(pool).await {
                Ok(p) => return Ok(p.0),
                Err(e) => {
                    log::error!("Failed to execute query {e}");
                    return Err(String::from("Couldn't check permission in database"));
                }
            };
    }
}


pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query(
        "CREATE TABLE IF NOT EXISTS admins(
            email VARCHAR(255) NOT NULL UNIQUE,
            get_admin BOOLEAN NOT NULL DEFAULT FALSE,
            post_admin BOOLEAN NOT NULL DEFAULT FALSE,
            put_admin BOOLEAN NOT NULL DEFAULT FALSE,
            post_bus_schedule BOOLEAN NOT NULL DEFAULT FALSE,
            put_bus_schedule BOOLEAN NOT NULL DEFAULT FALSE,
            post_event BOOLEAN NOT NULL DEFAULT FALSE,
            put_event BOOLEAN NOT NULL DEFAULT FALSE,
            post_mess_menu BOOLEAN NOT NULL DEFAULT FALSE,
            post_outlet BOOLEAN NOT NULL DEFAULT FALSE,
            delete_outlet BOOLEAN NOT NULL DEFAULT FALSE,
            put_outlet BOOLEAN NOT NULL DEFAULT FALSE,
            post_announcement BOOLEAN NOT NULL DEFAULT FALSE
        );"
    )
        .execute(pool)
        .await
}
