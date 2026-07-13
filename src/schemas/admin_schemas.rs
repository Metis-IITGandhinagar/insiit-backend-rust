use rs_firebase_admin_sdk::{ auth::{ FirebaseAuth, FirebaseAuthService, UserIdentifiers }, client::ReqwestApiClient };
use serde::{ Serialize, Deserialize };
use sqlx::{ FromRow, PgPool, postgres::PgQueryResult, query, query_as };

#[derive(Serialize, Deserialize, FromRow)]
pub struct AdminEntry {
    pub email: String,
    #[sqlx(flatten)]
    pub permissions: AdminPermissions,
}

#[derive(Serialize, Deserialize, FromRow)]
pub struct AdminPermissions {
    pub get_admin: bool,
    pub post_admin: bool,
    pub put_admin: bool,
    pub post_bus_schedule: bool,
    pub put_bus_schedule: bool,
    pub post_event: bool,
    pub delete_event: bool,
    pub put_event: bool,
    pub post_mess_menu: bool,
    pub post_outlet: bool,
    pub delete_outlet: bool,
    pub put_outlet: bool
}

#[derive(sqlx::Type, Debug)]
#[sqlx(type_name = "VARCHAR", rename_all = "snake_case")]
pub enum AdminPermission {
    GetAdmin, PostAdmin, PutAdmin, PostBusSchedule, PutBusSchedule, PostEvent, DeleteEvent, PutEvent, PostMessMenu, PostOutlet, DeleteOutlet, PutOutlet
}

impl AdminPermission {
    pub async fn granted_to(&self, token: String, auth: rs_firebase_admin_sdk::auth::FirebaseAuth<ReqwestApiClient>, pool: &PgPool) -> Result<bool, String> {
        let user = match auth.get_user(UserIdentifiers::builder().with_uid(token).build()).await {
            Ok(Some(user)) => user,
            Ok(None) => { return Err(String::from("Could not authorize user")) }
            Err(_e) => { return Err(String::from("Could not authorize user")) }
        };
        let email = match user.email {
            Some(email) => email,
            None => { return Err(String::from("Invalid user")) }
        };
        match query_as::<_, (bool,)>(
            "SELECT $1 FROM admins WHERE email = $2"
        )
            .bind(self)
            .bind(email)
            .fetch_one(pool).await {
                Ok(p) => return Ok(p.0),
                Err(_e) => return Err(String::from("Hi"))
            };
        Ok(true)
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
            delete_event BOOLEAN NOT NULL DEFAULT FALSE,
            put_event BOOLEAN NOT NULL DEFAULT FALSE,
            post_mess_menu BOOLEAN NOT NULL DEFAULT FALSE,
            post_outlet BOOLEAN NOT NULL DEFAULT FALSE,
            delete_outlet BOOLEAN NOT NULL DEFAULT FALSE,
            put_outlet BOOLEAN NOT NULL DEFAULT FALSE
        );"
    )
        .execute(pool)
        .await
}
