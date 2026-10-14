use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgQueryResult, query, query_as};
use time::OffsetDateTime;

/// How long before an event starts its reminder notification goes out.
pub const REMINDER_LEAD: time::Duration = time::Duration::minutes(10);
/// A job is abandoned after this many failed delivery attempts.
pub const MAX_ATTEMPTS: i32 = 10;

#[derive(Serialize, Deserialize)]
pub struct DeviceRequest {
    pub device_id: String,
    pub token: String,
    #[serde(default = "default_reminders_enabled")]
    pub reminders_enabled: bool,
}

fn default_reminders_enabled() -> bool { true }

#[derive(Deserialize)]
pub struct PreferenceRequest {
    pub reminders_enabled: bool,
}

/// One pending notification, joined with everything needed to deliver it.
#[derive(FromRow)]
pub struct PushJob {
    pub event_id: i32,
    pub user_email: String,
    pub device_id: String,
    pub kind: String,
    pub token: String,
    pub event_name: String,
    pub start_datetime: OffsetDateTime,
}

impl PushJob {
    pub fn title(&self) -> String {
        match self.kind.as_str() {
            "approved" => {
                let date = self.start_datetime
                    .format(&time::format_description::parse("[day] [month repr:long] [year]").unwrap())
                    .unwrap_or_else(|_| self.start_datetime.date().to_string());
                format!("{} — {date}", self.event_name)
            }
            _ => format!("{} — Starting Soon", self.event_name),
        }
    }

    pub fn body(&self) -> String {
        match self.kind.as_str() {
            "approved" => "Your event has been approved!".to_string(),
            _ => format!("{} starts in 10 minutes. Tap to view the event.", self.event_name),
        }
    }
}

pub async fn initialize_table(pool: &PgPool) -> Result<PgQueryResult, sqlx::Error> {
    query(
        "
        CREATE TABLE IF NOT EXISTS push_devices (
            user_email VARCHAR(255) NOT NULL,
            device_id VARCHAR(255) NOT NULL,
            token TEXT NOT NULL,
            reminders_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_email, device_id)
        );
    ",
    )
    .execute(pool)
    .await?;
    query("ALTER TABLE push_devices ADD COLUMN IF NOT EXISTS reminders_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
        .execute(pool)
        .await?;
    query(
        "
        CREATE TABLE IF NOT EXISTS event_push_jobs (
            event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            user_email VARCHAR(255) NOT NULL,
            device_id VARCHAR(255) NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('approved', 'reminder')),
            scheduled_at TIMESTAMPTZ NOT NULL,
            sent_at TIMESTAMPTZ,
            lease_until TIMESTAMPTZ,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (event_id, user_email, device_id, kind),
            FOREIGN KEY (user_email, device_id)
                REFERENCES push_devices(user_email, device_id) ON DELETE CASCADE
        );
    ",
    )
    .execute(pool)
    .await?;
    query("ALTER TABLE event_push_jobs ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;")
        .execute(pool)
        .await?;
    query(
        "CREATE INDEX IF NOT EXISTS event_push_jobs_due_idx
        ON event_push_jobs (scheduled_at) WHERE sent_at IS NULL;",
    )
    .execute(pool)
    .await
}

/// Stores a device's FCM token, replacing whatever it was registered under before.
///
/// FCM hands the same token to a reinstalled app, so a token can migrate between
/// devices and users. The stale row has to go first or it keeps delivering this
/// device's notifications to the previous owner.
pub async fn register_device(
    pool: &PgPool,
    user_email: &str,
    device: &DeviceRequest,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    query("DELETE FROM push_devices WHERE token = $1 AND NOT (user_email = $2 AND device_id = $3)")
        .bind(&device.token)
        .bind(user_email)
        .bind(&device.device_id)
        .execute(&mut *transaction)
        .await?;
    query(
        "INSERT INTO push_devices(user_email, device_id, token, reminders_enabled)
        VALUES($1, $2, $3, $4)
        ON CONFLICT (user_email, device_id)
        DO UPDATE SET token = EXCLUDED.token, reminders_enabled = EXCLUDED.reminders_enabled, updated_at = NOW();
        ",
    )
    .bind(user_email)
    .bind(&device.device_id)
    .bind(&device.token)
    .bind(device.reminders_enabled)
    .execute(&mut *transaction)
    .await?;
    // A device that registers late still gets reminded about events already approved.
    query(
        "INSERT INTO event_push_jobs(event_id, user_email, device_id, kind, scheduled_at)
        SELECT id, $1, $2, 'reminder', start_datetime - $3
        FROM events
        WHERE approved AND added_by_email = $1 AND start_datetime > NOW() + $3 AND $4
        ON CONFLICT DO NOTHING;
        ",
    )
    .bind(user_email)
    .bind(&device.device_id)
    .bind(REMINDER_LEAD)
    .bind(device.reminders_enabled)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

pub async fn remove_device(
    pool: &PgPool,
    user_email: &str,
    device_id: &str,
) -> Result<(), sqlx::Error> {
    // event_push_jobs cascades from push_devices, so its rows go with it.
    query("DELETE FROM push_devices WHERE user_email = $1 AND device_id = $2")
        .bind(user_email)
        .bind(device_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_reminder_preference(
    pool: &PgPool,
    user_email: &str,
    enabled: bool,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    query("UPDATE push_devices SET reminders_enabled = $2 WHERE user_email = $1")
        .bind(user_email).bind(enabled).execute(&mut *transaction).await?;
    if enabled {
        query("INSERT INTO event_push_jobs(event_id, user_email, device_id, kind, scheduled_at)
            SELECT event.id, device.user_email, device.device_id, 'reminder', event.start_datetime - $1
            FROM events event JOIN push_devices device ON device.user_email = event.added_by_email
            WHERE event.approved AND device.user_email = $2 AND event.start_datetime > NOW() + $1
                AND device.reminders_enabled
            ON CONFLICT DO NOTHING")
            .bind(REMINDER_LEAD).bind(user_email).execute(&mut *transaction).await?;
    } else {
        query("DELETE FROM event_push_jobs WHERE user_email = $1 AND kind = 'reminder' AND sent_at IS NULL")
            .bind(user_email).execute(&mut *transaction).await?;
    }
    transaction.commit().await
}

/// Queues the "event approved" push for every device, plus a reminder if the
/// event is still far enough away for one to be useful.
pub async fn enqueue_approval(
    pool: &PgPool,
    event_id: i32,
    start_datetime: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    query(
        "INSERT INTO event_push_jobs(event_id, user_email, device_id, kind, scheduled_at)
        SELECT $1, user_email, device_id, 'approved', NOW() FROM push_devices
        WHERE user_email = (SELECT added_by_email FROM events WHERE id = $1)
        ON CONFLICT DO NOTHING;
        ",
    )
    .bind(event_id)
    .execute(pool)
    .await?;
    if start_datetime <= OffsetDateTime::now_utc() + REMINDER_LEAD {
        return Ok(());
    }
    query(
        "INSERT INTO event_push_jobs(event_id, user_email, device_id, kind, scheduled_at)
        SELECT $1, user_email, device_id, 'reminder', $2 FROM push_devices
        WHERE reminders_enabled AND user_email = (SELECT added_by_email FROM events WHERE id = $1)
        ON CONFLICT DO NOTHING;
        ",
    )
    .bind(event_id)
    .bind(start_datetime - REMINDER_LEAD)
    .execute(pool)
    .await?;
    Ok(())
}

/// Moves an event's unsent reminders to match a new start time, dropping them
/// altogether once the event is too close for a reminder to make sense.
pub async fn reschedule_reminders(
    pool: &PgPool,
    event_id: i32,
    start_datetime: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    if start_datetime <= OffsetDateTime::now_utc() + REMINDER_LEAD {
        query(
            "DELETE FROM event_push_jobs
            WHERE event_id = $1 AND kind = 'reminder' AND sent_at IS NULL;",
        )
        .bind(event_id)
        .execute(pool)
        .await?;
        return Ok(());
    }
    query(
        "INSERT INTO event_push_jobs(event_id, user_email, device_id, kind, scheduled_at)
        SELECT event.id, device.user_email, device.device_id, 'reminder', $2
        FROM events event
        JOIN push_devices device ON device.user_email = event.added_by_email
        WHERE event.id = $1 AND event.approved AND device.reminders_enabled
        ON CONFLICT (event_id, user_email, device_id, kind)
        DO UPDATE SET scheduled_at = EXCLUDED.scheduled_at, attempts = 0,
            lease_until = NULL, sent_at = NULL;
        ",
    )
    .bind(event_id)
    .bind(start_datetime - REMINDER_LEAD)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn cancel_pending(pool: &PgPool, event_id: i32) -> Result<(), sqlx::Error> {
    query("DELETE FROM event_push_jobs WHERE event_id = $1 AND sent_at IS NULL")
        .bind(event_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Notifications that are due now, newest schedule last.
///
/// Reminders that are more than a minute late are skipped rather than sent with a
/// misleading "starts in 10 minutes" message.
pub async fn due_jobs(pool: &PgPool, limit: i64) -> Result<Vec<PushJob>, sqlx::Error> {
    query_as::<_, PushJob>(
        "WITH due AS (
            SELECT job.event_id, job.user_email, job.device_id, job.kind
            FROM event_push_jobs job
            JOIN events event ON event.id = job.event_id
            WHERE job.sent_at IS NULL AND job.scheduled_at <= NOW()
                AND job.attempts < $1 AND event.approved
                AND (job.lease_until IS NULL OR job.lease_until < NOW())
                AND (job.kind = 'approved' OR event.start_datetime > NOW() + INTERVAL '9 minutes')
            ORDER BY job.scheduled_at
            LIMIT $2
            FOR UPDATE OF job SKIP LOCKED
        ), claimed AS (
            UPDATE event_push_jobs job SET lease_until = NOW() + INTERVAL '5 minutes'
            FROM due
            WHERE job.event_id = due.event_id AND job.user_email = due.user_email
                AND job.device_id = due.device_id AND job.kind = due.kind
            RETURNING job.*
        )
        SELECT job.event_id, job.user_email, job.device_id, job.kind,
            device.token, event.name AS event_name, event.start_datetime
        FROM claimed job
        JOIN push_devices device USING (user_email, device_id)
        JOIN events event ON event.id = job.event_id
        WHERE event.approved AND (job.kind = 'approved' OR device.reminders_enabled)
            AND (job.kind = 'approved' OR event.start_datetime > NOW() + INTERVAL '9 minutes')
        ORDER BY job.scheduled_at;
        ",
    )
    .bind(MAX_ATTEMPTS)
    .bind(limit)
    .fetch_all(pool)
    .await
}

pub async fn mark_sent(pool: &PgPool, job: &PushJob) -> Result<(), sqlx::Error> {
    query(
        "UPDATE event_push_jobs SET sent_at = NOW(), lease_until = NULL
        WHERE event_id = $1 AND user_email = $2 AND device_id = $3 AND kind = $4
            AND sent_at IS NULL;
        ",
    )
    .bind(job.event_id)
    .bind(&job.user_email)
    .bind(&job.device_id)
    .bind(&job.kind)
    .execute(pool)
    .await?;
    Ok(())
}

/// Backs a failed job off exponentially, capped at an hour between attempts.
pub async fn record_failed_attempt(pool: &PgPool, job: &PushJob) -> Result<(), sqlx::Error> {
    query(
        "UPDATE event_push_jobs
        SET attempts = attempts + 1,
            lease_until = NULL,
            scheduled_at = NOW() + LEAST(
                INTERVAL '1 hour',
                INTERVAL '15 seconds' * POWER(2, LEAST(attempts, 8))
            )
        WHERE event_id = $1 AND user_email = $2 AND device_id = $3 AND kind = $4;
        ",
    )
    .bind(job.event_id)
    .bind(&job.user_email)
    .bind(&job.device_id)
    .bind(&job.kind)
    .execute(pool)
    .await?;
    Ok(())
}

/// Exercises the queries above against a real Postgres.
///
/// They are skipped unless `TEST_POSTGRES_URL` points at a scratch database,
/// because these queries are untyped and nothing else checks them:
/// `TEST_POSTGRES_URL=postgres://... cargo test -- --test-threads=1`
#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// A database with empty tables, or `None` when there is none to test against.
    async fn test_pool() -> Option<PgPool> {
        let url = std::env::var("TEST_POSTGRES_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("Couldn't connect to the test database");
        for statement in [
            "DROP TABLE IF EXISTS event_push_jobs",
            "DROP TABLE IF EXISTS push_devices",
            "DROP TABLE IF EXISTS events",
        ] {
            query(statement).execute(&pool).await.unwrap();
        }
        crate::schemas::events_schemas::initialize_table(&pool)
            .await
            .expect("Couldn't initialize events table");
        initialize_table(&pool)
            .await
            .expect("Couldn't initialize push notification tables");
        Some(pool)
    }

    fn device(device_id: &str, token: &str) -> DeviceRequest {
        DeviceRequest {
            device_id: String::from(device_id),
            token: String::from(token),
            reminders_enabled: true,
        }
    }

    /// An approved event starting `hours` from now.
    async fn approved_event(pool: &PgPool, hours: i64) -> i32 {
        let (id,): (i32,) = query_as(
            "INSERT INTO events(name, added_by_email, start_datetime, approved)
            VALUES('Hackathon', 'a@iit.in', $1, TRUE) RETURNING id;
            ",
        )
        .bind(OffsetDateTime::now_utc() + time::Duration::hours(hours))
        .fetch_one(pool)
        .await
        .unwrap();
        id
    }

    async fn count(pool: &PgPool, filter: &'static str) -> i64 {
        let (count,): (i64,) = query_as(filter).fetch_one(pool).await.unwrap();
        count
    }

    #[tokio::test]
    async fn approved_event_reaches_every_device_once() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "a@iit.in", &device("phone", "tok-1"))
            .await
            .unwrap();
        register_device(&pool, "a@iit.in", &device("tab", "tok-2"))
            .await
            .unwrap();
        let start_datetime = OffsetDateTime::now_utc() + time::Duration::hours(5);
        let event_id = approved_event(&pool, 5).await;

        enqueue_approval(&pool, event_id, start_datetime)
            .await
            .unwrap();
        // An admin approving twice must not push twice.
        enqueue_approval(&pool, event_id, start_datetime)
            .await
            .unwrap();

        let due = due_jobs(&pool, 100).await.unwrap();
        assert_eq!(
            due.len(),
            2,
            "one approved push per device, reminders are not due"
        );
        assert!(due.iter().all(|job| job.kind == "approved"));
        assert_eq!(due[0].event_name, "Hackathon");
        assert!(due[0].title().starts_with("Hackathon — "));
        assert_eq!(due[0].body(), "Your event has been approved!");

        mark_sent(&pool, &due[0]).await.unwrap();
        assert_eq!(
            due_jobs(&pool, 100).await.unwrap().len(),
            0,
            "the other job is already leased by this worker batch"
        );

        record_failed_attempt(&pool, &due[1]).await.unwrap();
        assert_eq!(
            due_jobs(&pool, 100).await.unwrap().len(),
            0,
            "a failed job backs off"
        );
    }

    #[tokio::test]
    async fn event_pushes_never_reach_unrelated_users() {
        let Some(pool) = test_pool().await else { return; };
        register_device(&pool, "a@iit.in", &device("owner-phone", "owner-token"))
            .await.unwrap();
        register_device(&pool, "b@iit.in", &device("other-phone", "other-token"))
            .await.unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(&pool, event_id, OffsetDateTime::now_utc() + time::Duration::hours(5))
            .await.unwrap();
        let due = due_jobs(&pool, 100).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].user_email, "a@iit.in");
        assert_eq!(due[0].token, "owner-token");
    }

    #[tokio::test]
    async fn reminder_preference_cancels_and_recreates_pending_jobs() {
        let Some(pool) = test_pool().await else { return; };
        register_device(&pool, "a@iit.in", &device("owner-phone", "owner-token"))
            .await.unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(&pool, event_id, OffsetDateTime::now_utc() + time::Duration::hours(5))
            .await.unwrap();
        set_reminder_preference(&pool, "a@iit.in", false).await.unwrap();
        assert_eq!(count(&pool, "SELECT count(*) FROM event_push_jobs WHERE kind = 'reminder' AND sent_at IS NULL").await, 0);
        set_reminder_preference(&pool, "a@iit.in", true).await.unwrap();
        assert_eq!(count(&pool, "SELECT count(*) FROM event_push_jobs WHERE kind = 'reminder' AND sent_at IS NULL").await, 1);
    }

    #[tokio::test]
    async fn reminders_follow_the_event_and_vanish_when_it_gets_too_close() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "a@iit.in", &device("phone", "tok-1"))
            .await
            .unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(
            &pool,
            event_id,
            OffsetDateTime::now_utc() + time::Duration::hours(5),
        )
        .await
        .unwrap();
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM event_push_jobs WHERE kind = 'reminder'"
            )
            .await,
            1
        );

        reschedule_reminders(
            &pool,
            event_id,
            OffsetDateTime::now_utc() + time::Duration::minutes(5),
        )
        .await
        .unwrap();
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM event_push_jobs WHERE kind = 'reminder'"
            )
            .await,
            0,
            "an event starting inside the lead window has nothing left to remind about"
        );
    }

    #[tokio::test]
    async fn an_event_owner_registering_late_still_gets_reminded() {
        let Some(pool) = test_pool().await else {
            return;
        };
        approved_event(&pool, 6).await;
        register_device(&pool, "a@iit.in", &device("phone", "tok-late"))
            .await
            .unwrap();
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM event_push_jobs WHERE kind = 'reminder'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn reinstalling_moves_a_token_instead_of_colliding() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "a@iit.in", &device("phone", "shared-tok"))
            .await
            .unwrap();
        register_device(&pool, "b@iit.in", &device("phone", "shared-tok"))
            .await
            .expect("FCM can hand one token to a reinstalled app under a new account");
        let owners: Vec<(String,)> =
            query_as("SELECT user_email FROM push_devices WHERE token = 'shared-tok'")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            owners.len(),
            1,
            "the previous owner stops receiving this device's pushes"
        );
        assert_eq!(owners[0].0, "b@iit.in");
    }

    #[tokio::test]
    async fn forgetting_a_device_takes_its_queued_pushes_with_it() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "a@iit.in", &device("phone", "tok-1"))
            .await
            .unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(
            &pool,
            event_id,
            OffsetDateTime::now_utc() + time::Duration::hours(5),
        )
        .await
        .unwrap();
        remove_device(&pool, "a@iit.in", "phone").await.unwrap();
        assert_eq!(
            count(&pool, "SELECT count(*) FROM event_push_jobs").await,
            0
        );
    }

    #[tokio::test]
    async fn a_reminder_is_dropped_once_the_event_has_started() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "d@iit.in", &device("phone", "tok-d"))
            .await
            .unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(
            &pool,
            event_id,
            OffsetDateTime::now_utc() + time::Duration::hours(5),
        )
        .await
        .unwrap();
        // Stand in for the worker being down across the whole reminder window.
        query("UPDATE events SET start_datetime = NOW() - INTERVAL '10 minutes' WHERE id = $1")
            .bind(event_id)
            .execute(&pool)
            .await
            .unwrap();
        query("UPDATE event_push_jobs SET scheduled_at = NOW() - INTERVAL '70 minutes'")
            .execute(&pool)
            .await
            .unwrap();
        query("UPDATE event_push_jobs SET sent_at = NOW() WHERE kind = 'approved'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            due_jobs(&pool, 100).await.unwrap().len(),
            0,
            "a late reminder must not go out after the event has started"
        );
    }

    #[tokio::test]
    async fn cancelling_leaves_already_sent_pushes_alone() {
        let Some(pool) = test_pool().await else {
            return;
        };
        register_device(&pool, "a@iit.in", &device("phone", "tok-1"))
            .await
            .unwrap();
        let event_id = approved_event(&pool, 5).await;
        enqueue_approval(
            &pool,
            event_id,
            OffsetDateTime::now_utc() + time::Duration::hours(5),
        )
        .await
        .unwrap();
        let due = due_jobs(&pool, 100).await.unwrap();
        mark_sent(&pool, &due[0]).await.unwrap();

        cancel_pending(&pool, event_id).await.unwrap();
        assert_eq!(
            count(&pool, "SELECT count(*) FROM event_push_jobs").await,
            1,
            "the reminder goes, the delivered approval stays recorded"
        );
    }
}
