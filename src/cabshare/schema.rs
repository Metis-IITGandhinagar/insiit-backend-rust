//! Cabshare tables. Every table is prefixed `cab_` so it never collides with
//! the rest of INSIIT. Times are unix seconds (BIGINT): a ride's departure is
//! compared for equality when riders are grouped, so it must not be rounded by
//! a timestamp type on the way through.

use sqlx::{PgPool, query};

const TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS cab_users (
        email VARCHAR(255) PRIMARY KEY,
        name TEXT NOT NULL DEFAULT '',
        handle TEXT NOT NULL UNIQUE,
        score DOUBLE PRECISION NOT NULL DEFAULT 70,
        gender TEXT,
        created BIGINT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS cab_rides (
        id BIGSERIAL PRIMARY KEY,
        destination TEXT NOT NULL,
        direction TEXT NOT NULL,
        departure BIGINT NOT NULL,
        settled BOOLEAN NOT NULL DEFAULT FALSE
    )",
    "CREATE INDEX IF NOT EXISTS cab_rides_when ON cab_rides (direction, departure)",
    "CREATE TABLE IF NOT EXISTS cab_requests (
        id BIGSERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        destination TEXT NOT NULL,
        direction TEXT NOT NULL,
        handbags BIGINT NOT NULL DEFAULT 0,
        suitcases BIGINT NOT NULL DEFAULT 0,
        party BIGINT NOT NULL DEFAULT 1,
        vehicle TEXT NOT NULL DEFAULT 'cab',
        state TEXT NOT NULL DEFAULT 'waiting',
        cab_id BIGINT REFERENCES cab_rides(id) ON DELETE SET NULL,
        rode BOOLEAN NOT NULL DEFAULT FALSE,
        checked_in BIGINT,
        notice TEXT,
        quote BIGINT,
        place_note TEXT,
        cleared BOOLEAN NOT NULL DEFAULT FALSE,
        created BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS cab_requests_email ON cab_requests (email)",
    "CREATE INDEX IF NOT EXISTS cab_requests_cab ON cab_requests (cab_id)",
    // one student never holds two seats in one ride
    "CREATE UNIQUE INDEX IF NOT EXISTS cab_requests_one_seat
        ON cab_requests (cab_id, email) WHERE state = 'grouped'",
    "CREATE TABLE IF NOT EXISTS cab_request_slots (
        request_id BIGINT NOT NULL REFERENCES cab_requests(id) ON DELETE CASCADE,
        departure BIGINT NOT NULL,
        PRIMARY KEY (request_id, departure)
    )",
    "CREATE INDEX IF NOT EXISTS cab_request_slots_when ON cab_request_slots (departure)",
    "CREATE TABLE IF NOT EXISTS cab_messages (
        id BIGSERIAL PRIMARY KEY,
        cab_id BIGINT NOT NULL,
        sender TEXT NOT NULL,
        body TEXT NOT NULL,
        created BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS cab_messages_cab ON cab_messages (cab_id, created)",
    "CREATE TABLE IF NOT EXISTS cab_reports (
        cab_id BIGINT NOT NULL,
        reporter TEXT NOT NULL,
        accused TEXT NOT NULL,
        created BIGINT NOT NULL,
        PRIMARY KEY (cab_id, reporter, accused)
    )",
    "CREATE TABLE IF NOT EXISTS cab_regroups (
        direction TEXT NOT NULL,
        departure BIGINT NOT NULL,
        PRIMARY KEY (direction, departure)
    )",
    "CREATE TABLE IF NOT EXISTS cab_blocks (
        blocker VARCHAR(255) NOT NULL,
        blocked VARCHAR(255) NOT NULL,
        created BIGINT NOT NULL,
        PRIMARY KEY (blocker, blocked)
    )",
    "CREATE INDEX IF NOT EXISTS cab_blocks_blocked ON cab_blocks (blocked)",
    "CREATE TABLE IF NOT EXISTS cab_score_log (
        id BIGSERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        reason TEXT NOT NULL,
        change DOUBLE PRECISION NOT NULL,
        destination TEXT,
        created BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS cab_score_log_email ON cab_score_log (email, created)",
    "CREATE TABLE IF NOT EXISTS cab_notifications (
        id BIGSERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        trip BIGINT,
        request BIGINT,
        created BIGINT NOT NULL,
        read BOOLEAN NOT NULL DEFAULT FALSE,
        dedupe TEXT,
        times BIGINT NOT NULL DEFAULT 1
    )",
    "CREATE INDEX IF NOT EXISTS cab_notifications_email ON cab_notifications (email, id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS cab_notifications_once
        ON cab_notifications (email, dedupe) WHERE dedupe IS NOT NULL",
    "CREATE TABLE IF NOT EXISTS cab_notification_prefs (
        email VARCHAR(255) NOT NULL,
        category TEXT NOT NULL,
        enabled BOOLEAN NOT NULL,
        PRIMARY KEY (email, category)
    )",
    // pushes to phones, delivered by the worker in notifications.rs
    "CREATE TABLE IF NOT EXISTS cab_push_jobs (
        id BIGSERIAL PRIMARY KEY,
        user_email VARCHAR(255) NOT NULL,
        device_id VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        request BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        sent_at TIMESTAMPTZ,
        lease_until TIMESTAMPTZ,
        attempts INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (user_email, device_id)
            REFERENCES push_devices(user_email, device_id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS cab_push_jobs_due ON cab_push_jobs (created_at) WHERE sent_at IS NULL",
];

pub async fn initialize_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    for sql in TABLES {
        query(*sql).execute(pool).await?;
    }
    Ok(())
}
