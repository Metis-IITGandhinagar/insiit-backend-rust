use sqlx::postgres::PgPool;

use crate::schemas::admin_schemas;
use crate::schemas::bus_schemas;
use crate::schemas::events_schemas;
use crate::schemas::mess_schemas;
use crate::schemas::outlets_schemas;

pub async fn initialize_database(pool: &PgPool) -> Result<(), String> {
    admin_schemas::initialize_table(pool).await.expect("Couldn't initialize admins table");
    bus_schemas::initialize_table(pool).await.expect("Couldn't initialize bus table");
    events_schemas::initialize_table(pool).await.expect("Couldn't initialize events table");
    mess_schemas::initialize_table(pool).await.expect("Couldn't initialize mess table");
    outlets_schemas::initialize_table(pool).await.expect("Couldn't initialize outlets table");
    log::info!("Created all required tables");
    Ok(())
}
