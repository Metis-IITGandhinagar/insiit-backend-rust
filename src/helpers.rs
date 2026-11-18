use sqlx::postgres::PgPool;

use crate::schemas::admin_schemas;
use crate::schemas::announcements_schemas;
use crate::schemas::bus_schemas;
use crate::schemas::buy_sell_schemas;
use crate::schemas::events_schemas;
use crate::schemas::lost_found_schemas;
use crate::schemas::mess_schemas;
use crate::schemas::outlets_schemas;
use crate::schemas::push_schemas;
use crate::schemas::representatives_schemas;
use crate::utils::{Notifier, SendError};

const NOTIFICATION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
// Caps each poll so one backlog can't monopolise the pool.
const NOTIFICATION_BATCH_SIZE: i64 = 10;

pub async fn initialize_database(pool: &PgPool) -> Result<(), String> {
    admin_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize admins table");
    announcements_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize announcements table");
    bus_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize bus table");
    buy_sell_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize buy sell table");
    events_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize events table");
    lost_found_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize lost found table");
    mess_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize mess table");
    outlets_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize outlets table");
    push_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize push notification tables");
    representatives_schemas::initialize_table(pool)
        .await
        .expect("Couldn't initialize representatives table");
    log::info!("Created all required tables");
    Ok(())
}

// Handlers only queue rows in event_push_jobs; this loop delivers them, so FCM never holds up a request.
pub async fn run_notification_worker(pool: PgPool, notifier: Notifier) {
    loop {
        if let Err(e) = deliver_due_notifications(&pool, &notifier).await {
            log::error!("Notifications: Couldn't deliver due notifications: {e}");
        }
        tokio::time::sleep(NOTIFICATION_POLL_INTERVAL).await;
    }
}

async fn deliver_due_notifications(pool: &PgPool, notifier: &Notifier) -> Result<(), sqlx::Error> {
    let jobs = match push_schemas::due_jobs(pool, NOTIFICATION_BATCH_SIZE).await {
        Ok(jobs) => jobs,
        Err(e) => return Err(e),
    };
    for job in jobs {
        let result = match notifier.send(&job).await {
            Ok(()) => push_schemas::mark_sent(pool, &job).await,
            Err(SendError::UnknownDevice) => {
                log::info!("Notifications: Forgetting device FCM no longer recognises");
                // The job rows cascade away with the device.
                push_schemas::remove_device(pool, &job.user_email, &job.device_id).await
            }
            Err(SendError::Transient(e)) => {
                log::warn!("Notifications: Delivery failed, will retry: {e}");
                push_schemas::record_failed_attempt(pool, &job).await
            }
        };
        if let Err(e) = result {
            return Err(e);
        }
    }
    Ok(())
}
