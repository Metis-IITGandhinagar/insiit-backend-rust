//! Cabshare (shared cabs and autos from campus), ported from the standalone
//! app (github.com/GellaNagaSaiKrishna/cs437-9-hack). The HTTP layer is
//! `routes/cabshare.rs`; this file holds the tables, the state and the rules.
//!
//! Cabshare state: requests, grouping, the final regroup, chat, check-in,
//! settling, blocks, prices and notifications, on Postgres.
//!
//! Every operation runs in one transaction that first takes a Postgres
//! advisory lock, so grouping decisions never race, even across several
//! backend instances. The clock is passed in (`Cx::now`, unix seconds), so the
//! tests move time instead of waiting for it.
//!
//! Privacy rules, kept here so no handler can get them wrong:
//! - Before 15 minutes to departure, co-riders are "Rider 2", "Rider 3": no
//!   name, no email, no handle.
//! - From 15 minutes, names and relay handles show. An email address never
//!   leaves this module for anyone but its owner.
//! - The app never takes money; fares are estimates.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::{PgPool, Postgres, Row, Transaction, query};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use serde::{Deserialize, Serialize};
use sqlx::Type;

use credibility as cred;
use matching::Req;

/// Which way a ride goes: out from campus, or back to it.
#[derive(Type, Serialize, Deserialize, Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[sqlx(type_name = "cab_direction", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    FromCampus,
    ToCampus,
}

/// What the group books.
#[derive(Type, Serialize, Deserialize, Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[sqlx(type_name = "cab_vehicle", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Vehicle {
    #[default]
    Cab,
    Auto,
}

/// Where a request is: waiting for a group, seated in one, or cancelled.
#[derive(Type, Serialize, Deserialize, Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[sqlx(type_name = "cab_request_state", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    #[default]
    Waiting,
    Grouped,
    Cancelled,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::FromCampus => "from_campus",
            Direction::ToCampus => "to_campus",
        }
    }
}

impl Vehicle {
    pub const ALL: [Vehicle; 2] = [Vehicle::Cab, Vehicle::Auto];
    pub fn as_str(self) -> &'static str {
        match self {
            Vehicle::Cab => "cab",
            Vehicle::Auto => "auto",
        }
    }
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Display for Vehicle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Tables. Every one is prefixed `cab_` so it never collides with the rest
// of INSIIT. Times are unix seconds (BIGINT): a ride's departure is compared
// for equality when riders are grouped, so it must not be rounded by a
// timestamp type on the way through.

const ENUMS: &[&str] = &[
    "DO $$ BEGIN CREATE TYPE cab_direction AS ENUM ('from_campus', 'to_campus');
     EXCEPTION WHEN duplicate_object THEN null; END $$;",
    "DO $$ BEGIN CREATE TYPE cab_vehicle AS ENUM ('cab', 'auto');
     EXCEPTION WHEN duplicate_object THEN null; END $$;",
    "DO $$ BEGIN CREATE TYPE cab_request_state AS ENUM ('waiting', 'grouped', 'cancelled');
     EXCEPTION WHEN duplicate_object THEN null; END $$;",
];

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
        id SERIAL PRIMARY KEY,
        destination TEXT NOT NULL,
        direction cab_direction NOT NULL,
        departure BIGINT NOT NULL,
        settled BOOLEAN NOT NULL DEFAULT FALSE
    )",
    "CREATE INDEX IF NOT EXISTS cab_rides_when ON cab_rides (direction, departure)",
    "CREATE TABLE IF NOT EXISTS cab_requests (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        destination TEXT NOT NULL,
        direction cab_direction NOT NULL,
        handbags INTEGER NOT NULL DEFAULT 0,
        suitcases INTEGER NOT NULL DEFAULT 0,
        party INTEGER NOT NULL DEFAULT 1,
        vehicle cab_vehicle NOT NULL DEFAULT 'cab',
        state cab_request_state NOT NULL DEFAULT 'waiting',
        cab_id INTEGER REFERENCES cab_rides(id) ON DELETE SET NULL,
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
        request_id INTEGER NOT NULL REFERENCES cab_requests(id) ON DELETE CASCADE,
        departure BIGINT NOT NULL,
        PRIMARY KEY (request_id, departure)
    )",
    "CREATE INDEX IF NOT EXISTS cab_request_slots_when ON cab_request_slots (departure)",
    "CREATE TABLE IF NOT EXISTS cab_messages (
        id SERIAL PRIMARY KEY,
        cab_id INTEGER NOT NULL,
        sender TEXT NOT NULL,
        body TEXT NOT NULL,
        created BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS cab_messages_cab ON cab_messages (cab_id, created)",
    "CREATE TABLE IF NOT EXISTS cab_reports (
        cab_id INTEGER NOT NULL,
        reporter TEXT NOT NULL,
        accused TEXT NOT NULL,
        created BIGINT NOT NULL,
        PRIMARY KEY (cab_id, reporter, accused)
    )",
    "CREATE TABLE IF NOT EXISTS cab_regroups (
        direction cab_direction NOT NULL,
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
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        reason TEXT NOT NULL,
        change DOUBLE PRECISION NOT NULL,
        destination TEXT,
        created BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS cab_score_log_email ON cab_score_log (email, created)",
    "CREATE TABLE IF NOT EXISTS cab_notifications (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        trip INTEGER,
        request INTEGER,
        created BIGINT NOT NULL,
        read BOOLEAN NOT NULL DEFAULT FALSE,
        dedupe TEXT,
        times INTEGER NOT NULL DEFAULT 1
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
        id SERIAL PRIMARY KEY,
        user_email VARCHAR(255) NOT NULL,
        device_id VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        request INTEGER,
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
    for sql in ENUMS.iter().chain(TABLES) {
        query(*sql).execute(pool).await?;
    }
    Ok(())
}

pub const REVEAL_BEFORE: i64 = 15 * 60;
pub const FREE_CANCEL_BEFORE: i64 = 2 * 60 * 60;
pub const MAX_AHEAD: i64 = 30 * 24 * 60 * 60;
pub const MAX_SLOTS: usize = 12;
pub const SETTLE_AFTER: i64 = 2 * 60 * 60;
pub const MESSAGE_MAX: usize = 500;
pub const NOTE_MAX: usize = 60;
pub const BLOCK_MAX: i64 = 100;
pub const CHECKIN_OPENS_BEFORE: i64 = 10 * 60;
pub const CHECKIN_CLOSES_AFTER: i64 = 10 * 60;
pub const DIRECTIONS: [Direction; 2] = [Direction::FromCampus, Direction::ToCampus];
pub const FEED_PAGE: usize = 50;
pub const FEED_PAGE_MAX: usize = 200;
pub const CHAT_PAGE: i64 = 50;
pub const PAST_PAGE: usize = 20;
pub const NOTIFY_PAGE: i64 = 30;
const QUOTE_WINDOW: i64 = 30 * 24 * 3600;
const QUOTES_TO_LEARN: usize = 3;
const FACTOR_RANGE: (f64, f64) = (0.5, 2.5);
const LOCK_KEY: i64 = 0x0CAB_5A4E;

pub const NOTIFY_CATEGORIES: [&str; 5] = ["group", "messages", "checkins", "reminders", "score"];

fn category(kind: &str) -> &'static str {
    match kind {
        "message" => "messages",
        "checked_in" => "checkins",
        "soon" | "reveal" | "checkin_open" | "confirm" => "reminders",
        "settled" => "score",
        _ => "group",
    }
}

#[derive(Debug)]
pub enum Error {
    /// The request was refused; the text says why, in words for the rider.
    Bad(String),
    NotYours,
    NotFound(String),
    Db(sqlx::Error),
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::Db(e)
    }
}

pub type R<T> = Result<T, Error>;

fn bad<T>(why: impl Into<String>) -> R<T> {
    Err(Error::Bad(why.into()))
}

/// Unix seconds as the RFC 3339 text every INSIIT endpoint uses.
pub fn iso(t: i64) -> String {
    OffsetDateTime::from_unix_timestamp(t)
        .ok()
        .and_then(|d| d.format(&Rfc3339).ok())
        .unwrap_or_default()
}

/// A time from the app: RFC 3339 text, or unix seconds.
pub fn parse_time(v: &Value) -> Option<i64> {
    match v {
        Value::String(s) => OffsetDateTime::parse(s, &Rfc3339)
            .ok()
            .map(|d| d.unix_timestamp()),
        Value::Number(n) => n.as_i64().or_else(|| {
            n.as_f64()
                .filter(|f| f.is_finite() && f.fract() == 0.0)
                .map(|f| f as i64)
        }),
        _ => None,
    }
}

/// "18:30" in India time, for notification text.
fn clock(t: i64) -> String {
    let d = OffsetDateTime::from_unix_timestamp(t + 19_800).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    format!("{:02}:{:02}", d.hour(), d.minute())
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Where riders meet the vehicle: the campus gate leaving campus (set with
/// CABSHARE_CHECKIN_LAT / _LNG), the rider's stop coming back.
pub fn pickup(destination: &str, direction: Direction) -> (f64, f64) {
    if direction == Direction::ToCampus {
        return places::coords(destination);
    }
    let (lat, lng) = places::coords(places::CAMPUS);
    (
        env_f64("CABSHARE_CHECKIN_LAT", lat),
        env_f64("CABSHARE_CHECKIN_LNG", lng),
    )
}

pub fn checkin_radius_m() -> f64 {
    env_f64("CABSHARE_CHECKIN_RADIUS_M", 150.0)
}

/// What a rider sends to post or join a ride.
#[derive(serde::Deserialize, Default, Clone)]
pub struct RideIn {
    pub destination: String,
    pub direction: Direction,
    #[serde(default)]
    pub slots: Vec<Value>,
    #[serde(default)]
    pub departure: Option<Value>,
    pub handbags: Option<i32>,
    pub suitcases: Option<i32>,
    #[serde(default)]
    pub party: Option<i32>,
    #[serde(default)]
    pub vehicle: Option<Vehicle>,
    #[serde(default)]
    pub quote: Option<f64>,
    #[serde(default)]
    pub place_note: Option<String>,
}

/// One vehicle: a direction, a departure, its riders.
#[derive(Clone)]
pub struct Trip {
    pub id: i32,
    pub destination: String,
    pub direction: Direction,
    pub departure: i64,
    pub settled: bool,
    pub members: Vec<Req>,
}

impl Trip {
    pub fn vehicle(&self) -> Option<Vehicle> {
        self.members.first().map(|m| m.vehicle)
    }
    pub fn riders(&self) -> i32 {
        matching::people(&self.members)
    }
    pub fn has_student(&self, email: &str) -> bool {
        self.members.iter().any(|m| m.email == email)
    }

    /// Why `r` cannot take a seat here, or None if it can. The one seating rule.
    pub fn refusal(&self, r: &Req) -> Option<&'static str> {
        let mut stops = vec![self.destination.as_str(), r.destination.as_str()];
        stops.extend(self.members.iter().map(|m| m.destination.as_str()));
        if !places::one_route(stops) {
            return Some("goes somewhere off this ride's route");
        }
        if r.direction != self.direction {
            return Some("goes the other way");
        }
        if !r.slots.contains(&self.departure) {
            return Some("leaves at a time this request did not offer");
        }
        if self.has_student(&r.email) {
            return Some("this student already has a seat in this trip");
        }
        if self.vehicle().is_some_and(|v| v != r.vehicle) {
            return Some("wants a different vehicle");
        }
        let mut all = self.members.clone();
        all.push(r.clone());
        if matching::apart(&all) {
            return Some("someone in this trip and this student must never ride together");
        }
        if !matching::fits(&all) {
            return Some("no room for these riders and their luggage");
        }
        None
    }
}

fn req_from_row(r: &PgRow) -> Req {
    Req {
        id: r.get("id"),
        email: r.get("email"),
        destination: r.get("destination"),
        direction: r.get("direction"),
        handbags: r.get("handbags"),
        suitcases: r.get("suitcases"),
        state: r.get("state"),
        cab_id: r.get("cab_id"),
        rode: r.get("rode"),
        checked_in: r.get("checked_in"),
        notice: r.get("notice"),
        created: r.get("created"),
        party: r.get("party"),
        vehicle: r.get("vehicle"),
        quote: r.get("quote"),
        place_note: r.get("place_note"),
        ..Default::default()
    }
}

fn clean_note(note: Option<&str>) -> R<Option<String>> {
    let Some(v) = note else { return Ok(None) };
    if v.chars().any(|c| (c as u32) < 32) {
        return bad("where exactly must be one line");
    }
    let v = v.split_whitespace().collect::<Vec<_>>().join(" ");
    if v.chars().count() > NOTE_MAX {
        return bad(format!(
            "where exactly can be at most {NOTE_MAX} characters"
        ));
    }
    Ok(if v.is_empty() { None } else { Some(v) })
}

fn shape(input: &RideIn) -> R<(i32, Vehicle, i32, i32)> {
    let vehicle = input.vehicle.unwrap_or_default();
    let cap = matching::capacity(vehicle);
    let party = input.party.unwrap_or(1);
    if !(1..=cap.seats - 1).contains(&party) {
        return bad(format!(
            "by {vehicle}, 1 to {} people can travel together, so there is a seat left to share",
            cap.seats - 1
        ));
    }
    let (Some(handbags), Some(suitcases)) = (input.handbags, input.suitcases) else {
        return bad("say how many suitcases and handbags you are carrying");
    };
    if handbags < 0 || suitcases < 0 {
        return bad("luggage counts cannot be negative");
    }
    if handbags > cap.handbags {
        return bad(format!(
            "one {vehicle} carries at most {} handbags",
            cap.handbags
        ));
    }
    if suitcases > cap.suitcases {
        let noun = if cap.suitcases == 1 {
            "suitcase"
        } else {
            "suitcases"
        };
        return bad(format!(
            "one {vehicle} carries at most {} {noun}",
            cap.suitcases
        ));
    }
    Ok((party, vehicle, handbags, suitcases))
}

fn place_check(destination: &str, direction: Direction) -> R<()> {
    if !places::is_stop(destination) {
        return bad(format!("unknown destination: {destination}"));
    }
    Ok(())
}

fn with(group: &[Req], one: &Req) -> Vec<Req> {
    let mut out = group.to_vec();
    out.push(one.clone());
    out
}

fn stops_json(reqs: &[Req], direction: Direction, mine: Option<&str>) -> Value {
    let mut order = places::order_for_drop(reqs.iter().map(|r| r.destination.as_str()));
    if direction == Direction::ToCampus {
        order.reverse();
    }
    Value::Array(
        order
            .iter()
            .map(|p| {
                json!({"place": p, "name": places::name(p), "you": Some(p.as_str()) == mine,
                       "people": reqs.iter().filter(|r| &r.destination == p).map(|r| r.party).sum::<i32>()})
            })
            .collect(),
    )
}

/// Places on the road a ride drives that no rider has picked yet.
fn passes(reqs: &[Req], way: Direction) -> Vec<String> {
    let taken: HashSet<&str> = reqs.iter().map(|r| r.destination.as_str()).collect();
    let fin = places::farthest(taken.iter().copied());
    let path = places::path_from_campus(&fin);
    let mut road: Vec<String> = path[1..path.len().saturating_sub(1)]
        .iter()
        .filter(|p| !taken.contains(p.as_str()))
        .cloned()
        .collect();
    if way == Direction::ToCampus {
        road.reverse();
    }
    road
}

/// One locked transaction with a clock.
pub struct Cx {
    pub tx: Transaction<'static, Postgres>,
    pub now: i64,
}

type Q<'q> = Query<'q, Postgres, PgArguments>;

impl Cx {
    pub async fn begin(pool: &PgPool, now: i64) -> R<Cx> {
        let mut tx = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(LOCK_KEY)
            .execute(&mut *tx)
            .await?;
        Ok(Cx { tx, now })
    }

    pub async fn commit(self) -> R<()> {
        self.tx.commit().await?;
        Ok(())
    }

    async fn all(&mut self, q: Q<'_>) -> R<Vec<PgRow>> {
        Ok(q.fetch_all(&mut *self.tx).await?)
    }
    async fn one(&mut self, q: Q<'_>) -> R<Option<PgRow>> {
        Ok(q.fetch_optional(&mut *self.tx).await?)
    }
    async fn exec(&mut self, q: Q<'_>) -> R<u64> {
        Ok(q.execute(&mut *self.tx).await?.rows_affected())
    }
    async fn id(&mut self, q: Q<'_>) -> R<i32> {
        Ok(q.fetch_one(&mut *self.tx).await?.get::<i32, _>(0))
    }

    // --- students -----------------------------------------------------------

    /// Make sure a cabshare profile exists; Google's name is kept up to date.
    pub async fn sign_in(&mut self, email: &str, name: &str) -> R<()> {
        self.exec(
            sqlx::query(
                "INSERT INTO cab_users(email, name, handle, score, created)
                 VALUES ($1, $2, 'relay-' || substr(md5(random()::text || clock_timestamp()::text), 1, 10) || '@share.iitgn', $3, $4)
                 ON CONFLICT (email) DO UPDATE SET name = CASE WHEN EXCLUDED.name = '' THEN cab_users.name ELSE EXCLUDED.name END",
            )
            .bind(email)
            .bind(name)
            .bind(cred::START)
            .bind(self.now),
        )
        .await?;
        Ok(())
    }

    async fn handle(&mut self, email: &str) -> R<String> {
        let row = self
            .one(sqlx::query("SELECT handle FROM cab_users WHERE email=$1").bind(email))
            .await?;
        match row {
            Some(r) => Ok(r.get("handle")),
            None => {
                self.sign_in(email, "").await?;
                Box::pin(self.handle(email)).await
            }
        }
    }

    async fn handles(&mut self, emails: &[String]) -> R<HashMap<String, String>> {
        let rows = self
            .all(
                sqlx::query("SELECT email, handle FROM cab_users WHERE email = ANY($1)")
                    .bind(emails),
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get("email"), r.get("handle")))
            .collect())
    }

    async fn note(
        &mut self,
        email: &str,
        reason: &str,
        change: f64,
        destination: Option<&str>,
    ) -> R<()> {
        self.exec(
            sqlx::query("INSERT INTO cab_score_log(email, reason, change, destination, created) VALUES ($1,$2,$3,$4,$5)")
                .bind(email)
                .bind(reason)
                .bind(change)
                .bind(destination)
                .bind(self.now),
        )
        .await?;
        Ok(())
    }

    async fn set_score(
        &mut self,
        email: &str,
        new: f64,
        reason: &str,
        destination: Option<&str>,
    ) -> R<()> {
        let old = self.score(email).await?;
        self.exec(
            sqlx::query("UPDATE cab_users SET score=$1 WHERE email=$2")
                .bind(new)
                .bind(email),
        )
        .await?;
        self.note(email, reason, new - old, destination).await
    }

    /// The score, after letting old mistakes fade, in whole points.
    pub async fn score(&mut self, email: &str) -> R<f64> {
        let Some(row) = self
            .one(sqlx::query("SELECT score FROM cab_users WHERE email=$1").bind(email))
            .await?
        else {
            return Ok(cred::START);
        };
        let score: f64 = row.get("score");
        if score >= cred::START {
            return Ok(score);
        }
        let last: Option<i64> = self
            .one(
                sqlx::query("SELECT MAX(created) AS t FROM cab_score_log WHERE email=$1")
                    .bind(email),
            )
            .await?
            .and_then(|r| r.get("t"));
        let Some(last) = last else { return Ok(score) };
        let target = cred::faded(score, (self.now - last) as f64);
        let mut gain = (target - score).floor();
        if cred::START - target < 1.0 {
            gain = cred::START - score;
        }
        if gain < 1.0 {
            return Ok(score);
        }
        self.exec(
            sqlx::query("UPDATE cab_users SET score=$1 WHERE email=$2")
                .bind(score + gain)
                .bind(email),
        )
        .await?;
        self.note(email, "fading", gain, None).await?;
        Ok(score + gain)
    }

    pub async fn profile(&mut self, email: &str) -> R<Value> {
        let score = self.score(email).await?;
        let row = self
            .one(
                sqlx::query("SELECT name, handle, gender FROM cab_users WHERE email=$1")
                    .bind(email),
            )
            .await?
            .ok_or_else(|| Error::NotFound("no such student".into()))?;
        let log = self
            .all(
                sqlx::query(
                    "SELECT reason, change, destination, created FROM cab_score_log
                     WHERE email=$1 ORDER BY id DESC LIMIT 20",
                )
                .bind(email),
            )
            .await?;
        Ok(json!({
            "email": email, "name": row.get::<String, _>("name"),
            "handle": row.get::<String, _>("handle"),
            "gender": row.get::<Option<String>, _>("gender"),
            "score": score.round(), "start": cred::START,
            "history": log.iter().map(|r| json!({
                "reason": r.get::<String, _>("reason"),
                "change": r.get::<f64, _>("change").round(),
                "destination": r.get::<Option<String>, _>("destination"),
                "at": iso(r.get("created")),
            })).collect::<Vec<_>>(),
        }))
    }

    /// Shown to co-riders with the name, from 15 minutes before, and only if given.
    pub async fn set_gender(&mut self, email: &str, gender: Option<&str>) -> R<()> {
        let gender = match gender.map(str::trim) {
            None | Some("") => None,
            Some(g @ ("woman" | "man" | "non_binary")) => Some(g),
            Some(_) => return bad("gender must be woman, man, non_binary, or empty"),
        };
        self.exec(
            sqlx::query("UPDATE cab_users SET gender=$1 WHERE email=$2")
                .bind(gender)
                .bind(email),
        )
        .await?;
        Ok(())
    }

    // --- reading requests and rides ------------------------------------------

    async fn row(&mut self, rid: i32) -> R<Req> {
        let row = self
            .one(sqlx::query("SELECT * FROM cab_requests WHERE id=$1").bind(rid))
            .await?
            .ok_or_else(|| Error::NotFound("no such request".into()))?;
        Ok(req_from_row(&row))
    }

    async fn own(&mut self, email: &str, rid: i32) -> R<Req> {
        let r = self.row(rid).await?;
        if r.email != email {
            return Err(Error::NotYours);
        }
        Ok(r)
    }

    async fn offered(&mut self, rid: i32) -> R<Vec<i64>> {
        let rows = self
            .all(
                sqlx::query("SELECT departure FROM cab_request_slots WHERE request_id=$1 ORDER BY departure")
                    .bind(rid),
            )
            .await?;
        Ok(rows.iter().map(|r| r.get("departure")).collect())
    }

    async fn avoids(&mut self, emails: &[String]) -> R<HashMap<String, HashSet<String>>> {
        let mut out: HashMap<String, HashSet<String>> =
            emails.iter().map(|e| (e.clone(), HashSet::new())).collect();
        let rows = self
            .all(
                sqlx::query("SELECT blocker, blocked FROM cab_blocks WHERE blocker = ANY($1) OR blocked = ANY($1)")
                    .bind(emails),
            )
            .await?;
        for r in rows {
            let (a, b): (String, String) = (r.get("blocker"), r.get("blocked"));
            if let Some(s) = out.get_mut(&a) {
                s.insert(b.clone());
            }
            if let Some(s) = out.get_mut(&b) {
                s.insert(a);
            }
        }
        Ok(out)
    }

    async fn avoid(&mut self, email: &str) -> R<HashSet<String>> {
        Ok(self
            .avoids(&[email.to_string()])
            .await?
            .remove(email)
            .unwrap_or_default())
    }

    /// Rows as full requests: their times, scores and blocks, in a fixed
    /// number of queries. `scored` lets low scores fade first.
    async fn load(&mut self, rows: Vec<Req>, scored: bool) -> R<Vec<Req>> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let cab_ids: Vec<i32> = rows
            .iter()
            .filter(|r| r.is_grouped())
            .filter_map(|r| r.cab_id)
            .collect();
        let departs: HashMap<i32, i64> = self
            .all(
                sqlx::query("SELECT id, departure FROM cab_rides WHERE id = ANY($1)")
                    .bind(&cab_ids),
            )
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("departure")))
            .collect();
        let open_ids: Vec<i32> = rows
            .iter()
            .filter(|r| !r.is_grouped())
            .map(|r| r.id)
            .collect();
        let mut offered: HashMap<i32, Vec<i64>> = HashMap::new();
        for r in self
            .all(
                sqlx::query(
                    "SELECT request_id, departure FROM cab_request_slots WHERE request_id = ANY($1)
                     ORDER BY request_id, departure",
                )
                .bind(&open_ids),
            )
            .await?
        {
            offered
                .entry(r.get("request_id"))
                .or_default()
                .push(r.get("departure"));
        }
        let emails: Vec<String> = rows
            .iter()
            .map(|r| r.email.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut scores: HashMap<String, f64> = self
            .all(
                sqlx::query("SELECT email, score FROM cab_users WHERE email = ANY($1)")
                    .bind(&emails),
            )
            .await?
            .iter()
            .map(|r| (r.get("email"), r.get("score")))
            .collect();
        if scored {
            let low: Vec<String> = scores
                .iter()
                .filter(|(_, s)| **s < cred::START)
                .map(|(e, _)| e.clone())
                .collect();
            for e in low {
                let s = self.score(&e).await?;
                scores.insert(e, s);
            }
        }
        let avoid = self.avoids(&emails).await?;
        Ok(rows
            .into_iter()
            .map(|mut r| {
                r.slots = if r.is_grouped() {
                    r.cab_id
                        .and_then(|c| departs.get(&c).copied())
                        .into_iter()
                        .collect()
                } else {
                    offered.get(&r.id).cloned().unwrap_or_default()
                };
                r.score = scores.get(&r.email).copied().unwrap_or(cred::START);
                r.avoid = avoid.get(&r.email).cloned().unwrap_or_default();
                r
            })
            .collect())
    }

    async fn reqs(&mut self, q: Q<'_>, scored: bool) -> R<Vec<Req>> {
        let rows: Vec<Req> = self.all(q).await?.iter().map(req_from_row).collect();
        self.load(rows, scored).await
    }

    pub async fn request(&mut self, rid: i32) -> R<Req> {
        let r = self.row(rid).await?;
        Ok(self.load(vec![r], true).await?.remove(0))
    }

    async fn members(&mut self, cab_id: i32, scored: bool) -> R<Vec<Req>> {
        self.reqs(
            sqlx::query(
                "SELECT * FROM cab_requests WHERE cab_id=$1 AND state='grouped' ORDER BY id",
            )
            .bind(cab_id),
            scored,
        )
        .await
    }

    async fn cab(&mut self, cab_id: i32) -> R<Option<(String, Direction, i64, bool)>> {
        Ok(self
            .one(
                sqlx::query(
                    "SELECT destination, direction, departure, settled FROM cab_rides WHERE id=$1",
                )
                .bind(cab_id),
            )
            .await?
            .map(|r| {
                (
                    r.get("destination"),
                    r.get("direction"),
                    r.get("departure"),
                    r.get("settled"),
                )
            }))
    }

    pub async fn trip(&mut self, cab_id: i32) -> R<Trip> {
        let (destination, direction, departure, settled) = self
            .cab(cab_id)
            .await?
            .ok_or_else(|| Error::NotFound("no such trip".into()))?;
        let members = self.members(cab_id, true).await?;
        Ok(Trip {
            id: cab_id,
            destination,
            direction,
            departure,
            settled,
            members,
        })
    }

    /// Waiting requests going `direction` (any stop), offering `at`, or with
    /// no `at` any time still ahead.
    async fn waiting(
        &mut self,
        direction: Direction,
        at: Option<i64>,
        scored: bool,
    ) -> R<Vec<Req>> {
        let q = match at {
            Some(at) => sqlx::query(
                "SELECT * FROM cab_requests WHERE state='waiting' AND direction=$1 AND id IN
                 (SELECT request_id FROM cab_request_slots WHERE departure=$2) ORDER BY id",
            )
            .bind(direction)
            .bind(at),
            None => sqlx::query(
                "SELECT * FROM cab_requests WHERE state='waiting' AND direction=$1 AND id IN
                 (SELECT request_id FROM cab_request_slots WHERE departure>$2) ORDER BY id",
            )
            .bind(direction)
            .bind(self.now),
        };
        self.reqs(q, scored).await
    }

    fn fixed(&self, departure: i64) -> bool {
        departure - self.now <= REVEAL_BEFORE
    }

    /// Times this student already holds on a live trip.
    async fn busy_times(&mut self, email: &str) -> R<HashSet<i64>> {
        let rows = self
            .all(
                sqlx::query(
                    "SELECT c.departure FROM cab_requests q JOIN cab_rides c ON c.id=q.cab_id
                     WHERE q.email=$1 AND q.state='grouped' AND c.departure>$2
                     UNION SELECT s.departure FROM cab_requests q JOIN cab_request_slots s ON s.request_id=q.id
                     WHERE q.email=$1 AND q.state='waiting' AND s.departure>$2",
                )
                .bind(email)
                .bind(self.now),
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    // --- posting and joining -------------------------------------------------

    fn slots(&self, input: &RideIn) -> R<Vec<i64>> {
        let mut slots: Vec<i64> = Vec::new();
        for v in &input.slots {
            match parse_time(v) {
                Some(t) => slots.push(t),
                None => return bad("each slot must be a time"),
            }
        }
        slots.sort();
        slots.dedup();
        if slots.is_empty() {
            return bad("offer at least one departure slot");
        }
        if slots.len() > MAX_SLOTS {
            return bad(format!("offer at most {MAX_SLOTS} times on one trip"));
        }
        if slots.iter().any(|&s| s <= self.now) {
            return bad("every slot must be in the future");
        }
        if slots.iter().any(|&s| s > self.now + MAX_AHEAD) {
            return bad("a slot cannot be more than 30 days ahead");
        }
        Ok(slots)
    }

    async fn refuse_busy(&mut self, email: &str, slots: &[i64]) -> R<()> {
        let busy = self.busy_times(email).await?;
        if slots.iter().any(|s| busy.contains(s)) {
            return bad("you already have a trip at that time; cancel it or pick another time");
        }
        Ok(())
    }

    /// A trip with one or more departure times, its luggage and vehicle.
    pub async fn post_request(&mut self, email: &str, input: &RideIn) -> R<i32> {
        self.regroup_due().await?;
        place_check(&input.destination, input.direction)?;
        let (party, vehicle, handbags, suitcases) = shape(input)?;
        let note = clean_note(input.place_note.as_deref())?;
        let slots = self.slots(input)?;
        let quote = self.check_quote(input.quote, &input.destination, vehicle)?;
        self.refuse_busy(email, &slots).await?;
        self.insert_and_place(
            email, input, &slots, party, vehicle, handbags, suitcases, quote, note,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_and_place(
        &mut self,
        email: &str,
        input: &RideIn,
        slots: &[i64],
        party: i32,
        vehicle: Vehicle,
        handbags: i32,
        suitcases: i32,
        quote: Option<i64>,
        note: Option<String>,
    ) -> R<i32> {
        let rid = self
            .id(sqlx::query(
                "INSERT INTO cab_requests(email, destination, direction, handbags, suitcases, party, vehicle,
                 quote, place_note, created) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id",
            )
            .bind(email)
            .bind(&input.destination)
            .bind(input.direction)
            .bind(handbags)
            .bind(suitcases)
            .bind(party)
            .bind(vehicle)
            .bind(quote)
            .bind(note)
            .bind(self.now))
            .await?;
        for s in slots {
            self.exec(
                sqlx::query("INSERT INTO cab_request_slots(request_id, departure) VALUES ($1,$2)")
                    .bind(rid)
                    .bind(*s),
            )
            .await?;
        }
        self.place(rid).await?;
        Ok(rid)
    }

    /// Join a time from the feed, only if it is still open.
    pub async fn join_time(&mut self, email: &str, input: &RideIn) -> R<i32> {
        self.regroup_due().await?;
        place_check(&input.destination, input.direction)?;
        let Some(departure) = input.departure.as_ref().and_then(parse_time) else {
            return bad("that is not a time");
        };
        let (party, vehicle, handbags, suitcases) = shape(input)?;
        let note = clean_note(input.place_note.as_deref())?;
        let open = self
            .open_slots(
                email,
                &input.destination,
                input.direction,
                handbags,
                suitcases,
                party,
                vehicle,
            )
            .await?;
        if !open.iter().any(|o| o.0 == departure) {
            let why = self
                .why_not_open(
                    email,
                    &input.destination,
                    input.direction,
                    departure,
                    handbags,
                    suitcases,
                    party,
                    vehicle,
                )
                .await?;
            return bad(why);
        }
        self.refuse_busy(email, &[departure]).await?;
        let quote = self.check_quote(input.quote, &input.destination, vehicle)?;
        self.insert_and_place(
            email,
            input,
            &[departure],
            party,
            vehicle,
            handbags,
            suitcases,
            quote,
            note,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn why_not_open(
        &mut self,
        email: &str,
        destination: &str,
        direction: Direction,
        departure: i64,
        handbags: i32,
        suitcases: i32,
        party: i32,
        vehicle: Vehicle,
    ) -> R<String> {
        if self.busy_times(email).await?.contains(&departure) {
            return Ok("You already have a trip at that time. Open it in My trips, or cancel it to join this one.".into());
        }
        if departure - self.now <= REVEAL_BEFORE {
            return Ok("That ride leaves in under 15 minutes, so its group is fixed.".into());
        }
        let avoid = self.avoid(email).await?;
        let ids: Vec<i32> = self
            .all(
                sqlx::query(
                    "SELECT id FROM cab_rides WHERE direction=$1 AND departure=$2 AND NOT settled",
                )
                .bind(direction)
                .bind(departure),
            )
            .await?
            .iter()
            .map(|r| r.get("id"))
            .collect();
        let mut others: Vec<Vec<Req>> = Vec::new();
        for id in ids {
            let m = self.members(id, false).await?;
            if !m.is_empty() && !m.iter().any(|x| x.email == email) {
                others.push(m);
            }
        }
        for w in self.waiting(direction, Some(departure), false).await? {
            if w.email != email {
                others.push(vec![w]);
            }
        }
        others.retain(|m| !m.iter().any(|x| avoid.contains(&x.email)));
        if others.is_empty() {
            return Ok("Nobody is going at that time any more. Pick another time.".into());
        }
        let me = Req {
            destination: destination.into(),
            direction,
            handbags,
            suitcases,
            party,
            vehicle,
            email: email.into(),
            ..Default::default()
        };
        let on_route: Vec<&Vec<Req>> = others
            .iter()
            .filter(|m| matching::on_one_route(&with(m, &me)))
            .collect();
        if on_route.is_empty() {
            return Ok("No ride at that time goes your way: it splits off before your stop. Pick another time or post your own.".into());
        }
        if !on_route
            .iter()
            .any(|m| m.iter().all(|x| x.vehicle == vehicle))
        {
            return Ok(format!("The ride at that time is not a {vehicle}."));
        }
        let mut why = String::from("Not enough room at that time for you");
        if party > 1 {
            why += &format!(" and {} more", party - 1);
        }
        if handbags > 0 || suitcases > 0 {
            why += " with your luggage";
        }
        Ok(why + ". Pick another time.")
    }

    // --- grouping ---------------------------------------------------------------

    async fn seat(&mut self, rid: i32, cab_id: i32, announce: bool) -> R<()> {
        // posting or joining a time already says "I am going then": no Accept step
        self.exec(
            sqlx::query("UPDATE cab_requests SET state='grouped', cab_id=$1 WHERE id=$2")
                .bind(cab_id)
                .bind(rid),
        )
        .await?;
        let (dest, _, departure, _) = self.cab(cab_id).await?.unwrap();
        let (where_, at) = (places::name(&dest).to_string(), clock(departure));
        let mine = self.row(rid).await?.email;
        self.notify(
            &mine,
            "grouped",
            &format!("You’re in a ride to {where_}"),
            &format!("Leaving at {at}. Say hello in the group chat."),
            Some(cab_id),
            Some(rid),
            Some(format!("grouped:{rid}:{cab_id}")),
        )
        .await?;
        if announce {
            let rows = self.member_rows(cab_id).await?;
            let n = rows.len();
            for (other, email) in rows {
                if email != mine {
                    self.notify(
                        &email,
                        "joined",
                        &format!("Someone joined your {at} ride"),
                        &format!("{n} going to {where_} now."),
                        Some(cab_id),
                        Some(other),
                        Some(format!("joined:{cab_id}:{rid}")),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn member_rows(&mut self, cab_id: i32) -> R<Vec<(i32, String)>> {
        Ok(self
            .all(sqlx::query("SELECT id, email FROM cab_requests WHERE cab_id=$1 AND state='grouped' ORDER BY id").bind(cab_id))
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("email")))
            .collect())
    }

    /// Back to waiting, offering again only times not since taken elsewhere.
    async fn unseat(&mut self, rid: i32) -> R<()> {
        let row = self.row(rid).await?;
        let seated = match row.cab_id {
            Some(c) => self.cab(c).await?.map(|c| c.2),
            None => None,
        };
        self.exec(
            sqlx::query("UPDATE cab_requests SET state='waiting', cab_id=NULL WHERE id=$1")
                .bind(rid),
        )
        .await?;
        let mut taken: HashSet<i64> = HashSet::new();
        let others = self
            .all(
                sqlx::query(
                    "SELECT q.id, q.state, c.departure FROM cab_requests q LEFT JOIN cab_rides c ON c.id=q.cab_id
                     WHERE q.email=$1 AND q.id<>$2 AND q.state IN ('waiting','grouped')",
                )
                .bind(&row.email)
                .bind(rid),
            )
            .await?;
        for r in others {
            if r.get::<RequestState, _>("state") == RequestState::Grouped {
                if let Some(d) = r.get::<Option<i64>, _>("departure") {
                    taken.insert(d);
                }
            } else {
                taken.extend(self.offered(r.get("id")).await?);
            }
        }
        for d in self.offered(rid).await? {
            if taken.contains(&d) && Some(d) != seated {
                self.exec(
                    sqlx::query(
                        "DELETE FROM cab_request_slots WHERE request_id=$1 AND departure=$2",
                    )
                    .bind(rid)
                    .bind(d),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// A ride goes as far as its farthest rider.
    async fn reroute(&mut self, cab_id: i32) -> R<()> {
        let stops: Vec<String> = self
            .all(
                sqlx::query(
                    "SELECT destination FROM cab_requests WHERE cab_id=$1 AND state='grouped'",
                )
                .bind(cab_id),
            )
            .await?
            .iter()
            .map(|r| r.get("destination"))
            .collect();
        if !stops.is_empty() {
            let far = places::farthest(stops.iter().map(String::as_str));
            self.exec(
                sqlx::query("UPDATE cab_rides SET destination=$1 WHERE id=$2")
                    .bind(far)
                    .bind(cab_id),
            )
            .await?;
        }
        Ok(())
    }

    /// Leave a notice on a rider whose group changed under them.
    async fn tell(&mut self, rid: i32, why: &str) -> R<()> {
        let row = self.row(rid).await?;
        let why = if row.is_grouped() { "moved" } else { why };
        self.exec(
            sqlx::query("UPDATE cab_requests SET notice=$1 WHERE id=$2")
                .bind(why)
                .bind(rid),
        )
        .await?;
        let (title, body) = match why {
            "moved" => ("Your ride changed", "Check your new group and time."),
            "partner_left" => (
                "You’re back to waiting",
                "Your co-rider cancelled. We’ll group you again.",
            ),
            "displaced" => (
                "You’re back to waiting",
                "We’ll group you again at one of your times.",
            ),
            _ => ("Your ride changed", "Open it to see what changed."),
        };
        let dedupe = format!("changed:{rid}:{why}:{}", row.cab_id.unwrap_or(0));
        self.notify(
            &row.email,
            "changed",
            title,
            body,
            row.cab_id,
            Some(rid),
            Some(dedupe),
        )
        .await
    }

    pub async fn dismiss_notice(&mut self, email: &str, rid: i32) -> R<()> {
        self.own(email, rid).await?;
        self.exec(sqlx::query("UPDATE cab_requests SET notice=NULL WHERE id=$1").bind(rid))
            .await?;
        Ok(())
    }

    /// Put a waiting request into a ride if the rules allow. For each time,
    /// earliest first: join a ride with room; else take the seat of a less
    /// reliable member, who is placed again; else start a ride with the most
    /// reliable waiting requests that fit.
    pub fn place(&mut self, rid: i32) -> Pin<Box<dyn Future<Output = R<bool>> + Send + '_>> {
        Box::pin(async move {
            let me = self.request(rid).await?;
            if !me.is_waiting() {
                return Ok(false);
            }
            for slot in me.slots.clone() {
                if self.fixed(slot) {
                    continue;
                }
                let ids: Vec<i32> = self
                    .all(
                        sqlx::query("SELECT id FROM cab_rides WHERE direction=$1 AND departure=$2 ORDER BY id")
                            .bind(me.direction)
                            .bind(slot),
                    )
                    .await?
                    .iter()
                    .map(|r| r.get("id"))
                    .collect();
                let mut trips = Vec::new();
                for id in ids {
                    let t = self.trip(id).await?;
                    if !t.has_student(&me.email) {
                        trips.push(t);
                    }
                }
                // a ride already going at least this far comes first
                trips.sort_by_key(|t| !places::on_route(&me.destination, &t.destination));
                for t in &trips {
                    if !t.members.is_empty() && t.refusal(&me).is_none() {
                        self.seat(rid, t.id, true).await?;
                        self.reroute(t.id).await?;
                        return Ok(true);
                    }
                }
                for t in &trips {
                    let Some(out) = matching::weakest(&t.members).cloned() else {
                        continue;
                    };
                    let rest: Vec<Req> = t
                        .members
                        .iter()
                        .filter(|m| m.id != out.id)
                        .cloned()
                        .collect();
                    if me.score > out.score && matching::fits(&with(&rest, &me)) {
                        self.unseat(out.id).await?;
                        self.seat(rid, t.id, true).await?;
                        self.reroute(t.id).await?;
                        self.place(out.id).await?;
                        self.tell(out.id, "displaced").await?;
                        return Ok(true);
                    }
                }
                let mut others = Vec::new();
                let mut seen: HashSet<String> = HashSet::from([me.email.clone()]);
                for r in
                    matching::by_credibility(self.waiting(me.direction, Some(slot), true).await?)
                {
                    if r.id != rid && r.slots.contains(&slot) && seen.insert(r.email.clone()) {
                        others.push(r);
                    }
                }
                let group = matching::fill(&me, others);
                if group.len() >= 2 {
                    let far = places::farthest(group.iter().map(|r| r.destination.as_str()));
                    let cab = self
                        .id(sqlx::query("INSERT INTO cab_rides(destination, direction, departure) VALUES ($1,$2,$3) RETURNING id")
                            .bind(far)
                            .bind(me.direction)
                            .bind(slot))
                        .await?;
                    for r in &group {
                        self.seat(r.id, cab, false).await?;
                    }
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// A seat came free: fill it, most reliable first. A ride left with one
    /// rider is dissolved and that rider waits again.
    async fn refill(&mut self, cab_id: i32) -> R<()> {
        let members = self.members(cab_id, true).await?;
        if members.len() <= 1 {
            for m in &members {
                self.unseat(m.id).await?;
            }
            self.exec(sqlx::query("DELETE FROM cab_rides WHERE id=$1").bind(cab_id))
                .await?;
            for m in &members {
                self.place(m.id).await?;
                self.tell(m.id, "partner_left").await?;
            }
            return Ok(());
        }
        self.reroute(cab_id).await?;
        let mut trip = self.trip(cab_id).await?;
        for cand in matching::by_credibility(
            self.waiting(trip.direction, Some(trip.departure), true)
                .await?,
        ) {
            if trip.refusal(&cand).is_none() {
                self.seat(cand.id, cab_id, true).await?;
                trip.destination =
                    places::farthest([trip.destination.as_str(), cand.destination.as_str()]);
                trip.members.push(cand);
            }
        }
        self.reroute(cab_id).await
    }

    // --- the feed -----------------------------------------------------------------

    /// Every ride still ahead in `direction` with its members, and every
    /// rider waiting that way.
    async fn candidates(&mut self, direction: Direction) -> R<(Vec<(i64, Vec<Req>)>, Vec<Req>)> {
        let waiting = self.waiting(direction, None, false).await?;
        let cabs: Vec<(i32, i64)> = self
            .all(
                sqlx::query("SELECT id, departure FROM cab_rides WHERE departure>$1 AND direction=$2 ORDER BY departure, id")
                    .bind(self.now)
                    .bind(direction),
            )
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("departure")))
            .collect();
        let ids: Vec<i32> = cabs.iter().map(|c| c.0).collect();
        let mut members: HashMap<i32, Vec<Req>> = HashMap::new();
        for m in self
            .reqs(
                sqlx::query("SELECT * FROM cab_requests WHERE state='grouped' AND cab_id = ANY($1) ORDER BY id").bind(ids),
                false,
            )
            .await?
        {
            members.entry(m.cab_id.unwrap()).or_default().push(m);
        }
        Ok((
            cabs.iter()
                .map(|(id, d)| (*d, members.remove(id).unwrap_or_default()))
                .collect(),
            waiting,
        ))
    }

    /// Rides in `direction` this student could join now: (departure, members, stop).
    #[allow(clippy::too_many_arguments)]
    async fn joinable(
        &mut self,
        email: &str,
        direction: Direction,
        handbags: i32,
        suitcases: i32,
        party: i32,
        vehicle: Vehicle,
        destination: Option<&str>,
        loaded: &(Vec<(i64, Vec<Req>)>, Vec<Req>),
    ) -> R<Vec<(i64, Vec<Req>, String)>> {
        let avoid = self.avoid(email).await?;
        let (cabs, waiting) = loaded;
        let mut rides: Vec<(i64, Vec<Req>)> = Vec::new();
        for (d, members) in cabs {
            if *d > self.now + REVEAL_BEFORE
                && !members.is_empty()
                && !members.iter().any(|m| m.email == email)
            {
                rides.push((*d, members.clone()));
            }
        }
        for r in waiting {
            if r.email != email {
                for &d in &r.slots {
                    if !self.fixed(d) && d > self.now {
                        rides.push((d, vec![r.clone()]));
                    }
                }
            }
        }
        let mut out = Vec::new();
        for (d, members) in rides {
            let stop = match destination {
                Some(s) => s.to_string(),
                None => places::farthest(members.iter().map(|m| m.destination.as_str())),
            };
            let me = Req {
                destination: stop.clone(),
                direction,
                handbags,
                suitcases,
                party,
                vehicle,
                email: email.into(),
                avoid: avoid.clone(),
                ..Default::default()
            };
            if matching::fits(&with(&members, &me)) {
                out.push((d, members, stop));
            }
        }
        Ok(out)
    }

    /// Departure times this student could join now to `destination`:
    /// (departure, members), cabs before lone riders.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_slots(
        &mut self,
        email: &str,
        destination: &str,
        direction: Direction,
        handbags: i32,
        suitcases: i32,
        party: i32,
        vehicle: Vehicle,
    ) -> R<Vec<(i64, Vec<Req>)>> {
        let loaded = self.candidates(direction).await?;
        let mut found: BTreeMap<i64, Vec<Req>> = BTreeMap::new();
        let mut cabs: HashSet<i64> = HashSet::new();
        for (d, members, _) in self
            .joinable(
                email,
                direction,
                handbags,
                suitcases,
                party,
                vehicle,
                Some(destination),
                &loaded,
            )
            .await?
        {
            if members.len() > 1 || members[0].is_grouped() {
                cabs.insert(d);
                let bigger = found.get(&d).is_none_or(|f| members.len() > f.len());
                if bigger {
                    found.insert(d, members);
                }
            } else if !cabs.contains(&d) && !found.contains_key(&d) {
                found.insert(d, members);
            }
        }
        Ok(found.into_iter().collect())
    }

    /// The times a student could join to one stop, with what each costs.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_times(
        &mut self,
        email: &str,
        destination: &str,
        direction: Direction,
        handbags: i32,
        suitcases: i32,
        party: i32,
        vehicle: Vehicle,
    ) -> R<Value> {
        self.regroup_due().await?;
        place_check(destination, direction)?;
        let input = RideIn {
            destination: destination.into(),
            direction,
            handbags: Some(handbags),
            suitcases: Some(suitcases),
            party: Some(party),
            vehicle: Some(vehicle),
            ..Default::default()
        };
        let (party, vehicle, handbags, suitcases) = shape(&input)?;
        let cap = matching::capacity(vehicle);
        let mut out = Vec::new();
        for (d, members) in self
            .open_slots(
                email,
                destination,
                direction,
                handbags,
                suitcases,
                party,
                vehicle,
            )
            .await?
        {
            let me = Req {
                destination: destination.into(),
                direction,
                handbags,
                suitcases,
                party,
                vehicle,
                ..Default::default()
            };
            let all = with(&members, &me);
            out.push(json!({
                "departure": iso(d), "riders": matching::people(&members), "vehicle": vehicle,
                "seats_left": cap.seats - matching::people(&members) - party,
                "suitcases_left": cap.suitcases - members.iter().map(|m| m.suitcases).sum::<i32>() - suitcases,
                "handbags_left": cap.handbags - members.iter().map(|m| m.handbags).sum::<i32>() - handbags,
                "stops": stops_json(&all, direction, Some(destination)),
                "fare": self.trip_fare(&members, &me, direction).await?,
            }));
        }
        Ok(Value::Array(out))
    }

    async fn ride(
        &mut self,
        way: Direction,
        vehicle: Vehicle,
        departure: i64,
        stop: &str,
        members: &[Req],
    ) -> R<Value> {
        let going = matching::people(members);
        let cap = matching::capacity(vehicle);
        let me = Req {
            destination: stop.into(),
            direction: way,
            vehicle,
            party: 1,
            ..Default::default()
        };
        let all = with(members, &me);
        let fin = places::farthest(all.iter().map(|m| m.destination.as_str()));
        Ok(json!({
            "destination": stop, "destination_name": places::name(stop),
            "direction": way, "vehicle": vehicle, "departure": iso(departure),
            "going": going, "seats": cap.seats, "seats_free": cap.seats - going,
            "suitcases_free": cap.suitcases - members.iter().map(|m| m.suitcases).sum::<i32>(),
            "handbags_free": cap.handbags - members.iter().map(|m| m.handbags).sum::<i32>(),
            "final": fin, "final_name": places::name(&fin),
            "stops": stops_json(&all, way, Some(stop)),
            "passes": passes(&all, way).iter().map(|p| json!({"place": p, "name": places::name(p)})).collect::<Vec<_>>(),
            "fare": self.trip_fare(members, &me, way).await?,
        }))
    }

    /// Every ride this student could join now, soonest first, as pages.
    ///
    /// Each ride shows once: a student waiting alone with several times is one
    /// ride offering those times. Only counts, never who. `after` is the cursor
    /// from the previous page.
    pub async fn feed(
        &mut self,
        email: &str,
        direction: Option<Direction>,
        to: Option<&str>,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> R<Value> {
        self.regroup_due().await?;
        if let Some(t) = to
            && !places::is_stop(t)
        {
            return bad(format!("unknown destination: {t}"));
        }
        let limit = limit.unwrap_or(FEED_PAGE).clamp(1, FEED_PAGE_MAX);
        let busy = self.busy_times(email).await?;
        let mut rides: Vec<(i64, String, usize, usize, String, Value)> = Vec::new();
        let ways: Vec<Direction> = match direction {
            Some(d) => vec![d],
            None => DIRECTIONS.to_vec(),
        };
        for way in ways {
            let loaded = self.candidates(way).await?;
            for vehicle in matching::VEHICLES {
                let mut found: BTreeMap<(i64, String), (Vec<Req>, String)> = BTreeMap::new();
                for (d, members, stop) in self
                    .joinable(email, way, 0, 0, 1, vehicle, to, &loaded)
                    .await?
                {
                    let fin = places::farthest(
                        members
                            .iter()
                            .map(|m| m.destination.as_str())
                            .chain([stop.as_str()]),
                    );
                    if !busy.contains(&d) {
                        found.entry((d, fin)).or_insert((members, stop));
                    }
                }
                // a lone waiting rider with several times is one ride
                let mut flexible: Vec<(String, Vec<Req>, String, Vec<i64>)> = Vec::new();
                for ((d, fin), (members, stop)) in found {
                    let key = if members.len() == 1 && members[0].is_waiting() {
                        format!("w{}~{stop}", members[0].id)
                    } else {
                        format!("c{d}~{fin}")
                    };
                    match flexible.iter_mut().find(|f| f.0 == key) {
                        Some(f) => f.3.push(d),
                        None => flexible.push((key, members, stop, vec![d])),
                    }
                }
                for (_, members, stop, times) in flexible {
                    let mut ride = self.ride(way, vehicle, times[0], &stop, &members).await?;
                    ride["times"] = json!(times.iter().map(|t| iso(*t)).collect::<Vec<_>>());
                    let fin = ride["final"].as_str().unwrap_or("").to_string();
                    let wi = DIRECTIONS.iter().position(|x| *x == way).unwrap();
                    let vi = matching::VEHICLES
                        .iter()
                        .position(|x| *x == vehicle)
                        .unwrap();
                    rides.push((times[0], stop, wi, vi, fin, ride));
                }
            }
        }
        rides.sort_by(|a, b| (a.0, &a.1, a.2, a.3, &a.4).cmp(&(b.0, &b.1, b.2, b.3, &b.4)));
        let key = |r: &(i64, String, usize, usize, String, Value)| {
            format!("{}~{}~{}~{}~{}", r.0, r.1, r.2, r.3, r.4)
        };
        let start = match after {
            Some(cursor) => {
                let parts: Vec<&str> = cursor.split('~').collect();
                let mark = (|| {
                    Some((
                        parts.first()?.parse::<i64>().ok()?,
                        parts.get(1)?.to_string(),
                        parts.get(2)?.parse::<usize>().ok()?,
                        parts.get(3)?.parse::<usize>().ok()?,
                        parts.get(4)?.to_string(),
                    ))
                })();
                let Some(mark) = mark.filter(|_| parts.len() == 5) else {
                    return bad("that is not a page of the feed");
                };
                rides
                    .iter()
                    .position(|r| {
                        (r.0, &r.1, r.2, r.3, &r.4) > (mark.0, &mark.1, mark.2, mark.3, &mark.4)
                    })
                    .unwrap_or(rides.len())
            }
            None => 0,
        };
        let page: Vec<_> = rides.iter().skip(start).take(limit).collect();
        let next = if start + limit < rides.len() {
            page.last().map(|r| key(r))
        } else {
            None
        };
        Ok(json!({"rides": page.iter().map(|r| r.5.clone()).collect::<Vec<_>>(), "next": next}))
    }

    // --- chat ---------------------------------------------------------------------

    pub async fn send_message(&mut self, email: &str, rid: i32, body: &str) -> R<()> {
        let row = self.own(email, rid).await?;
        if !row.is_grouped() {
            return bad("you are not in a group yet");
        }
        let body = body.trim();
        if body.is_empty() {
            return bad("a message cannot be empty");
        }
        if body.chars().count() > MESSAGE_MAX {
            return bad(format!(
                "a message cannot be longer than {MESSAGE_MAX} characters"
            ));
        }
        let cab = row.cab_id.unwrap();
        let handle = self.handle(email).await?;
        self.exec(
            sqlx::query(
                "INSERT INTO cab_messages(cab_id, sender, body, created) VALUES ($1,$2,$3,$4)",
            )
            .bind(cab)
            .bind(&handle)
            .bind(body)
            .bind(self.now),
        )
        .await?;
        let where_ = self.where_(cab).await?;
        let short: String = body.chars().take(120).collect();
        let who = handle.split('@').next().unwrap_or("").to_string();
        self.notify_cab(
            cab,
            "message",
            &format!("New message · {where_} ride"),
            &format!("{who}: {short}"),
            Some(email),
            None,
        )
        .await
    }

    /// The latest messages of this rider's group, oldest first; `older` pages back.
    pub async fn messages(
        &mut self,
        email: &str,
        rid: i32,
        limit: Option<i64>,
        before: Option<&str>,
    ) -> R<Value> {
        let row = self.own(email, rid).await?;
        let Some(cab) = row.cab_id.filter(|_| row.is_grouped()) else {
            return Ok(json!({"messages": [], "older": null}));
        };
        let limit = limit.unwrap_or(CHAT_PAGE).clamp(1, 200);
        let (at, mid) = match before {
            Some(b) => {
                let mut it = b.split('~');
                match (
                    it.next().and_then(|x| x.parse().ok()),
                    it.next().and_then(|x| x.parse().ok()),
                ) {
                    (Some(a), Some(m)) => (a, m),
                    _ => return bad("that is not a page of this chat"),
                }
            }
            None => (i64::MAX, i64::MAX),
        };
        let mut rows = self
            .all(
                sqlx::query(
                    "SELECT id, sender, body, created FROM cab_messages WHERE cab_id=$1
                     AND (created<$2 OR (created=$2 AND id<$3)) ORDER BY created DESC, id DESC LIMIT $4",
                )
                .bind(cab)
                .bind(at)
                .bind(mid)
                .bind(limit + 1),
            )
            .await?;
        let more = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        rows.reverse();
        let mine = self.handle(email).await?;
        let older = if more {
            rows.first()
                .map(|r| format!("{}~{}", r.get::<i64, _>("created"), r.get::<i32, _>("id")))
        } else {
            None
        };
        Ok(json!({
            "messages": rows.iter().map(|m| {
                let sender: String = m.get("sender");
                json!({"id": m.get::<i32, _>("id"), "from": sender, "mine": sender == mine,
                       "body": m.get::<String, _>("body"), "at": iso(m.get("created"))})
            }).collect::<Vec<_>>(),
            "older": older,
        }))
    }

    // --- after the ride -------------------------------------------------------------

    async fn departed(&mut self, cab_id: i32) -> R<bool> {
        Ok(self.cab(cab_id).await?.is_some_and(|c| self.now >= c.2))
    }

    pub async fn confirm_ride(&mut self, email: &str, rid: i32) -> R<()> {
        let row = self.own(email, rid).await?;
        if !row.is_grouped() {
            return bad("you are not in a group");
        }
        if !self.departed(row.cab_id.unwrap()).await? {
            return bad("the cab has not left yet");
        }
        self.exec(sqlx::query("UPDATE cab_requests SET rode=TRUE WHERE id=$1").bind(rid))
            .await?;
        Ok(())
    }

    /// GPS check-in at the pickup point: inside the window and the radius, the
    /// rider turned up, and nobody can report them as a no-show.
    pub async fn check_in(&mut self, email: &str, rid: i32, lat: f64, lng: f64) -> R<()> {
        let row = self.own(email, rid).await?;
        if !row.is_grouped() {
            return bad("you are not in a group");
        }
        if !(lat.is_finite()
            && lng.is_finite()
            && (-90.0..=90.0).contains(&lat)
            && (-180.0..=180.0).contains(&lng))
        {
            return bad("we could not read your location");
        }
        let cab = row.cab_id.unwrap();
        let departure = self.cab(cab).await?.unwrap().2;
        if self.now < departure - CHECKIN_OPENS_BEFORE {
            return bad("check-in opens 10 minutes before departure");
        }
        if self.now > departure + CHECKIN_CLOSES_AFTER {
            return bad("check-in has closed for this ride");
        }
        let away = places::straight_m(pickup(&row.destination, row.direction), (lat, lng));
        let radius = checkin_radius_m();
        if away > radius {
            return bad(format!(
                "you are {} m from the pickup point; check in within {} m",
                away.round(),
                radius.round()
            ));
        }
        if row.checked_in.is_none() {
            self.exec(
                sqlx::query("UPDATE cab_requests SET checked_in=$1, rode=TRUE WHERE id=$2")
                    .bind(self.now)
                    .bind(rid),
            )
            .await?;
            let done = self
                .one(sqlx::query(
                    "SELECT COUNT(*) FILTER (WHERE checked_in IS NOT NULL) AS done, COUNT(*) AS n
                     FROM cab_requests WHERE cab_id=$1 AND state='grouped'",
                )
                .bind(cab))
                .await?
                .unwrap();
            let (d, n): (i64, i64) = (done.get("done"), done.get("n"));
            self.notify_cab(
                cab,
                "checked_in",
                "A rider is at the pickup",
                &format!("{d} of {n} checked in."),
                Some(email),
                Some(format!("checked_in:{rid}")),
            )
            .await?;
        }
        Ok(())
    }

    /// Report a co-rider, named by relay handle, as not turning up.
    pub async fn report_no_show(&mut self, email: &str, rid: i32, handle: &str) -> R<()> {
        let row = self.own(email, rid).await?;
        if !row.is_grouped() {
            return bad("you are not in a group");
        }
        let cab = row.cab_id.unwrap();
        if !self.departed(cab).await? {
            return bad("the cab has not left yet");
        }
        let members = self.member_rows(cab).await?;
        let emails: Vec<String> = members.iter().map(|m| m.1.clone()).collect();
        let handles = self.handles(&emails).await?;
        let mine = self.handle(email).await?;
        if handle == mine {
            return bad("you cannot report yourself");
        }
        let Some((accused_rid, _)) = members
            .iter()
            .find(|(_, e)| handles.get(e).map(String::as_str) == Some(handle))
        else {
            return bad("that handle is not in your cab");
        };
        if self.row(*accused_rid).await?.checked_in.is_some() {
            return bad("they checked in at the pickup point");
        }
        self.exec(
            sqlx::query("INSERT INTO cab_reports(cab_id, reporter, accused, created) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING")
                .bind(cab)
                .bind(mine)
                .bind(handle)
                .bind(self.now),
        )
        .await?;
        Ok(())
    }

    async fn report_counts(&mut self, reporter: &str, created: i64) -> R<bool> {
        let n: i64 = self
            .one(
                sqlx::query("SELECT COUNT(*) AS n FROM cab_reports WHERE reporter=$1 AND created<$2 AND created>=$3")
                    .bind(reporter)
                    .bind(created)
                    .bind(created - cred::REPORT_WINDOW),
            )
            .await?
            .unwrap()
            .get("n");
        Ok(n < cred::REPORT_CAP)
    }

    /// Apply the outcome of one ride, once, two hours after departure.
    ///
    /// A member loses points when they did not check in and enough co-riders
    /// reported them: two reporters, or the one other rider in a cab of two.
    /// A member who confirmed or checked in, and was not penalised, earns a
    /// little back.
    pub async fn settle(&mut self, cab_id: i32) -> R<bool> {
        let Some((_, _, departure, settled)) = self.cab(cab_id).await? else {
            return Ok(false);
        };
        if settled || self.now < departure + SETTLE_AFTER {
            return Ok(false);
        }
        let members = self.member_rows(cab_id).await?;
        let need = cred::REPORTS_NEEDED.min(members.len().saturating_sub(1));
        let mut reporters: HashMap<String, HashSet<String>> = HashMap::new();
        for r in self
            .all(
                sqlx::query("SELECT reporter, accused, created FROM cab_reports WHERE cab_id=$1")
                    .bind(cab_id),
            )
            .await?
        {
            let (reporter, accused, created): (String, String, i64) =
                (r.get(0), r.get(1), r.get(2));
            if self.report_counts(&reporter, created).await? {
                reporters.entry(accused).or_default().insert(reporter);
            }
        }
        for (rid, email) in members {
            let row = self.row(rid).await?;
            let handle = self.handle(&email).await?;
            let score = self.score(&email).await?;
            let penalised = row.checked_in.is_none()
                && need > 0
                && reporters.get(&handle).map_or(0, |s| s.len()) >= need;
            let (new, why) = if penalised {
                (cred::after_no_show(score), "no_show")
            } else if row.rode || row.checked_in.is_some() {
                (cred::after_rode(score), "rode")
            } else {
                continue;
            };
            self.set_score(&email, new, why, Some(&row.destination))
                .await?;
            let change = (new - score).round();
            let text = format!(
                "{}{change} credibility: {}",
                if change >= 0.0 { "+" } else { "" },
                if why == "no_show" {
                    "you were reported as not turning up."
                } else {
                    "thanks for riding."
                }
            );
            self.notify(
                &email,
                "settled",
                "Your ride is settled",
                &text,
                Some(cab_id),
                Some(rid),
                Some(format!("settled:{rid}")),
            )
            .await?;
        }
        self.exec(sqlx::query("UPDATE cab_rides SET settled=TRUE WHERE id=$1").bind(cab_id))
            .await?;
        Ok(true)
    }

    pub async fn settle_due(&mut self) -> R<usize> {
        let ids: Vec<i32> = self
            .all(
                sqlx::query(
                    "SELECT id FROM cab_rides WHERE NOT settled AND departure<=$1 ORDER BY id",
                )
                .bind(self.now - SETTLE_AFTER),
            )
            .await?
            .iter()
            .map(|r| r.get("id"))
            .collect();
        let mut done = 0;
        for id in ids {
            done += usize::from(self.settle(id).await?);
        }
        Ok(done)
    }

    // --- the final regroup at 15 minutes ------------------------------------------

    pub async fn regroup_due(&mut self) -> R<usize> {
        let rows = self
            .all(
                sqlx::query(
                    "SELECT direction, departure FROM cab_rides WHERE NOT settled AND departure>$1 AND departure<=$2
                     UNION SELECT q.direction, s.departure FROM cab_requests q JOIN cab_request_slots s
                     ON s.request_id=q.id WHERE q.state='waiting' AND s.departure>$1 AND s.departure<=$2
                     ORDER BY 2",
                )
                .bind(self.now)
                .bind(self.now + REVEAL_BEFORE),
            )
            .await?;
        let due: Vec<(Direction, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        let mut done = 0;
        for (direction, departure) in due {
            done += usize::from(self.regroup(direction, departure).await?);
        }
        Ok(done)
    }

    async fn regroup(&mut self, direction: Direction, departure: i64) -> R<bool> {
        let fresh = self
            .exec(
                sqlx::query("INSERT INTO cab_regroups(direction, departure) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                    .bind(direction)
                    .bind(departure),
            )
            .await?;
        if fresh == 0 {
            return Ok(false);
        }
        let cab_ids: Vec<i32> = self
            .all(
                sqlx::query(
                    "SELECT id FROM cab_rides WHERE direction=$1 AND departure=$2 ORDER BY id",
                )
                .bind(direction)
                .bind(departure),
            )
            .await?
            .iter()
            .map(|r| r.get("id"))
            .collect();
        let mut seated = Vec::new();
        for c in &cab_ids {
            seated.extend(self.members(*c, true).await?);
        }
        let waiting: Vec<Req> = self
            .waiting(direction, Some(departure), true)
            .await?
            .into_iter()
            .filter(|w| w.slots.contains(&departure))
            .collect();
        let mut riders = Vec::new();
        let mut seen = HashSet::new();
        for r in seated.iter().chain(&waiting) {
            if seen.insert(r.email.clone()) {
                riders.push(r.clone());
            }
        }
        let before: HashMap<i32, i32> = seated.iter().map(|m| (m.id, m.cab_id.unwrap())).collect();
        let (mut groups, _) = matching::regroup(riders.clone());
        groups.sort_by_key(|g| std::cmp::Reverse(g.len()));
        let mut free = cab_ids.clone();
        let mut placed: HashMap<i32, i32> = HashMap::new();
        for g in &groups {
            let ids: Vec<i32> = g.iter().map(|r| r.id).collect();
            let overlap = |c: i32| ids.iter().filter(|i| before.get(i) == Some(&c)).count();
            let best = free
                .iter()
                .copied()
                .max_by_key(|c| (overlap(*c), std::cmp::Reverse(*c)));
            let cab = match best.filter(|c| overlap(*c) > 0) {
                Some(c) => {
                    free.retain(|x| *x != c);
                    c
                }
                None => {
                    let far = places::farthest(g.iter().map(|r| r.destination.as_str()));
                    self.id(
                        sqlx::query("INSERT INTO cab_rides(destination, direction, departure) VALUES ($1,$2,$3) RETURNING id")
                            .bind(far)
                            .bind(direction)
                            .bind(departure),
                    )
                    .await?
                }
            };
            for i in ids {
                placed.insert(i, cab);
            }
        }
        for r in &riders {
            match (placed.get(&r.id), before.get(&r.id)) {
                (None, Some(_)) => {
                    self.unseat(r.id).await?;
                    self.tell(r.id, "partner_left").await?;
                }
                (None, None) => {}
                (Some(&cab), Some(&was)) => {
                    self.exec(
                        sqlx::query("UPDATE cab_requests SET cab_id=$1 WHERE id=$2")
                            .bind(cab)
                            .bind(r.id),
                    )
                    .await?;
                    let mates: HashSet<i32> = placed
                        .iter()
                        .filter(|(_, c)| **c == cab)
                        .map(|(i, _)| *i)
                        .collect();
                    let mates_before: HashSet<i32> = before
                        .iter()
                        .filter(|(_, c)| **c == was)
                        .map(|(i, _)| *i)
                        .collect();
                    if mates != mates_before {
                        self.exec(
                            sqlx::query("UPDATE cab_requests SET notice='regrouped' WHERE id=$1")
                                .bind(r.id),
                        )
                        .await?;
                        self.notify(
                            &r.email,
                            "changed",
                            "Final groups are set",
                            "Your co-riders changed at 15 minutes to go.",
                            Some(cab),
                            Some(r.id),
                            Some(format!("regrouped:{}:{cab}", r.id)),
                        )
                        .await?;
                    }
                }
                (Some(&cab), None) => self.seat(r.id, cab, true).await?,
            }
        }
        for cab in placed.values().copied().collect::<HashSet<_>>() {
            self.reroute(cab).await?;
        }
        // rides the regroup emptied
        for c in free {
            if self.member_rows(c).await?.is_empty() {
                self.exec(sqlx::query("DELETE FROM cab_rides WHERE id=$1").bind(c))
                    .await?;
            }
        }
        Ok(true)
    }

    /// Run everything that depends only on the clock. Called by the worker
    /// every few seconds and at the start of reads that show groups.
    pub async fn tick(&mut self) -> R<()> {
        self.regroup_due().await?;
        self.settle_due().await?;
        self.notify_due().await
    }

    // --- cancelling -----------------------------------------------------------------

    /// Free until 2 h before, -10 credibility until 15 min before, then refused.
    pub async fn cancel(&mut self, email: &str, rid: i32) -> R<()> {
        let row = self.own(email, rid).await?;
        match row.state {
            RequestState::Cancelled => return bad("already cancelled"),
            RequestState::Waiting => {
                self.exec(
                    sqlx::query("UPDATE cab_requests SET state='cancelled' WHERE id=$1").bind(rid),
                )
                .await?;
                return self
                    .note(email, "cancel_waiting", 0.0, Some(&row.destination))
                    .await;
            }
            _ => {}
        }
        let cab = row.cab_id.unwrap();
        let left = self.cab(cab).await?.unwrap().2 - self.now;
        if left <= REVEAL_BEFORE {
            return bad("too close to departure to cancel in the app");
        }
        self.exec(
            sqlx::query("UPDATE cab_requests SET state='cancelled', cab_id=NULL WHERE id=$1")
                .bind(rid),
        )
        .await?;
        if left <= FREE_CANCEL_BEFORE {
            let s = self.score(email).await?;
            self.set_score(
                email,
                cred::after_late_cancel(s),
                "late_cancel",
                Some(&row.destination),
            )
            .await?;
        } else {
            self.note(email, "cancel_early", 0.0, Some(&row.destination))
                .await?;
        }
        self.refill(cab).await?;
        if self.cab(cab).await?.is_some() && self.member_rows(cab).await?.len() >= 2 {
            self.notify_cab(
                cab,
                "left",
                "A rider left your ride",
                "Their seat is open again; someone may join.",
                None,
                Some(format!("left:{rid}")),
            )
            .await?;
        }
        Ok(())
    }

    // --- prices -----------------------------------------------------------------------

    /// A rider's Ola/Uber price for their own trip: whole rupees, and not wildly
    /// off the rate card, which would be a typo.
    fn check_quote(
        &self,
        quote: Option<f64>,
        destination: &str,
        vehicle: Vehicle,
    ) -> R<Option<i64>> {
        let Some(q) = quote else { return Ok(None) };
        if !q.is_finite() || q.fract() != 0.0 || q < 20.0 {
            return bad("enter the Ola or Uber price in whole rupees");
        }
        let base = fares::total(places::km_from_campus(destination), vehicle, 1.0) as f64;
        if !(0.3 * base..=4.0 * base).contains(&q) {
            return bad(format!(
                "₹{q} looks far off for this trip (we estimate about ₹{base}); check the price and try again"
            ));
        }
        Ok(Some(q as i64))
    }

    pub async fn set_quote(&mut self, email: &str, rid: i32, quote: Option<f64>) -> R<Option<i64>> {
        let row = self.own(email, rid).await?;
        if row.state == RequestState::Cancelled {
            return bad("that trip is cancelled");
        }
        let q = self.check_quote(quote, &row.destination, row.vehicle)?;
        self.exec(
            sqlx::query("UPDATE cab_requests SET quote=$1 WHERE id=$2")
                .bind(q)
                .bind(rid),
        )
        .await?;
        Ok(q)
    }

    /// How far real prices are from the rate card: the quotes in this ride (a
    /// quote to the final stop prices the whole vehicle), else recent quotes
    /// from everyone once there are a few, else 1. The median, clamped.
    async fn price_factor(
        &mut self,
        reqs: &[Req],
        vehicle: Vehicle,
        ride_km: Option<f64>,
    ) -> R<(f64, &'static str, usize)> {
        let factor_of = |rows: &[(i64, String)], km: Option<f64>| -> Option<f64> {
            let mut fs: Vec<f64> = rows
                .iter()
                .map(|(q, d)| {
                    *q as f64
                        / fares::raw(km.unwrap_or_else(|| places::km_from_campus(d)), vehicle)
                            .max(1.0)
                })
                .collect();
            if fs.is_empty() {
                return None;
            }
            fs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mid = fs.len() / 2;
            let f = if fs.len() % 2 == 1 {
                fs[mid]
            } else {
                (fs[mid - 1] + fs[mid]) / 2.0
            };
            Some(f.clamp(FACTOR_RANGE.0, FACTOR_RANGE.1))
        };
        let ids: Vec<i32> = reqs.iter().map(|r| r.id).filter(|i| *i > 0).collect();
        if !ids.is_empty() {
            let rows: Vec<(i64, String)> = self
                .all(sqlx::query("SELECT quote, destination FROM cab_requests WHERE quote IS NOT NULL AND id = ANY($1)").bind(ids))
                .await?
                .iter()
                .map(|r| (r.get(0), r.get(1)))
                .collect();
            let fin = places::farthest(reqs.iter().map(|r| r.destination.as_str()));
            let whole: Vec<(i64, String)> = rows.iter().filter(|r| r.1 == fin).cloned().collect();
            if !whole.is_empty() {
                return Ok((
                    factor_of(&whole, ride_km).unwrap(),
                    "quote_final",
                    whole.len(),
                ));
            }
            if let Some(f) = factor_of(&rows, None) {
                return Ok((f, "quote", rows.len()));
            }
        }
        let rows: Vec<(i64, String)> = self
            .all(
                sqlx::query(
                    "SELECT quote, destination FROM cab_requests WHERE quote IS NOT NULL AND vehicle=$1 AND created>=$2
                     ORDER BY id DESC LIMIT 200",
                )
                .bind(vehicle)
                .bind(self.now - QUOTE_WINDOW),
            )
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        if rows.len() >= QUOTES_TO_LEARN {
            return Ok((factor_of(&rows, None).unwrap(), "riders", rows.len()));
        }
        Ok((1.0, "rate", 0))
    }

    /// What `mine` pays riding with `members`, split leg by leg, against
    /// taking a vehicle alone to its own stop.
    async fn trip_fare(&mut self, members: &[Req], mine: &Req, direction: Direction) -> R<Value> {
        let mut riders: Vec<Req> = members.to_vec();
        riders.sort_by_key(|m| m.id);
        if !riders.iter().any(|m| m.id == mine.id && mine.id != 0) {
            riders.push(mine.clone());
        }
        let stops = places::order_for_drop(riders.iter().map(|r| r.destination.as_str()));
        let mut chain = vec![places::CAMPUS.to_string()];
        chain.extend(stops.iter().cloned());
        let kms: Vec<f64> = chain
            .windows(2)
            .map(|w| places::leg_km(&w[0], &w[1]))
            .collect();
        let last: Vec<usize> = riders
            .iter()
            .map(|r| stops.iter().position(|s| *s == r.destination).unwrap())
            .collect();
        let vehicle = mine.vehicle;
        let (factor, price, n) = self
            .price_factor(&riders, vehicle, Some(kms.iter().sum()))
            .await?;
        let seats: Vec<(usize, i64)> = last
            .iter()
            .zip(&riders)
            .map(|(l, r)| (*l, i64::from(r.party.max(1))))
            .collect();
        let (fare, shares, costs) = fares::by_legs(&kms, &seats, vehicle, factor);
        let me = riders
            .iter()
            .position(|r| r.id == mine.id && (mine.id != 0 || r.email == mine.email))
            .unwrap_or(riders.len() - 1);
        let mine_last = last[me];
        let alone = fares::total(places::km_from_campus(&mine.destination), vehicle, factor);
        let mut legs: Vec<Value> = costs
            .iter()
            .zip(&kms)
            .enumerate()
            .map(|(j, ((cost, aboard), km))| {
                let (mut a, mut b) = (chain[j].clone(), chain[j + 1].clone());
                if direction == Direction::ToCampus {
                    std::mem::swap(&mut a, &mut b);
                }
                let part = if j <= mine_last { (cost * riders[me].party.max(1) as f64 / *aboard as f64).round() } else { 0.0 };
                json!({"from": a, "from_name": places::name(&a), "to": b, "to_name": places::name(&b),
                       "km": km, "cost": cost.round(), "people": aboard, "you": j <= mine_last, "your_part": part})
            })
            .collect();
        if direction == Direction::ToCampus {
            legs.reverse();
        }
        let r = fares::rate(vehicle);
        Ok(json!({
            "km": (kms.iter().sum::<f64>() * 10.0).round() / 10.0, "source": "maps",
            "total": fare, "riders": matching::people(&riders), "your_share": shares[me],
            "alone": alone, "you_save": alone - shares[me], "stop": mine.destination, "legs": legs,
            "vehicle": vehicle, "base": r.minimum, "included_km": r.included_km, "per_km": r.per_km,
            "price_source": price, "quotes": n, "factor": (factor * 1000.0).round() / 1000.0,
            "final_stop": stops.last(), "your_quote": if mine.id > 0 { json!(mine.quote) } else { Value::Null },
        }))
    }

    // --- blocks -------------------------------------------------------------------------

    pub async fn blocked(&mut self, email: &str) -> R<Vec<String>> {
        Ok(self
            .all(sqlx::query("SELECT blocked FROM cab_blocks WHERE blocker=$1 ORDER BY created DESC, blocked").bind(email))
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect())
    }

    /// Never share a ride with `other`. If the two already share one that is
    /// not fixed, the student who blocked moves out, free; the other is told nothing.
    pub async fn block(&mut self, email: &str, other: &str) -> R<()> {
        let other = other.trim().to_lowercase();
        let valid = other.strip_suffix("@iitgn.ac.in").is_some_and(|u| {
            !u.is_empty()
                && u.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._%+-".contains(c))
        });
        if !valid {
            return bad("enter an IITGN email address, like name@iitgn.ac.in");
        }
        if other == email {
            return bad("you cannot block yourself");
        }
        let n = self.blocked(email).await?;
        if n.contains(&other) {
            return Ok(());
        }
        if n.len() as i64 >= BLOCK_MAX {
            return bad(format!("you can block at most {BLOCK_MAX} people"));
        }
        self.exec(
            sqlx::query("INSERT INTO cab_blocks(blocker, blocked, created) VALUES ($1,$2,$3)")
                .bind(email)
                .bind(&other)
                .bind(self.now),
        )
        .await?;
        let mine = self
            .all(
                sqlx::query(
                    "SELECT id, cab_id FROM cab_requests WHERE email=$1 AND state='grouped'",
                )
                .bind(email),
            )
            .await?;
        for r in mine {
            let (rid, cab): (i32, i32) = (r.get(0), r.get(1));
            let departure = self.cab(cab).await?.unwrap().2;
            let emails: Vec<String> = self
                .member_rows(cab)
                .await?
                .into_iter()
                .map(|m| m.1)
                .collect();
            if emails.contains(&other) && !self.fixed(departure) {
                self.unseat(rid).await?;
                self.refill(cab).await?;
                self.place(rid).await?;
            }
        }
        Ok(())
    }

    pub async fn unblock(&mut self, email: &str, other: &str) -> R<()> {
        self.exec(
            sqlx::query("DELETE FROM cab_blocks WHERE blocker=$1 AND blocked=$2")
                .bind(email)
                .bind(other.trim().to_lowercase()),
        )
        .await?;
        Ok(())
    }

    // --- my trips -----------------------------------------------------------------------

    async fn where_(&mut self, cab_id: i32) -> R<String> {
        Ok(self
            .cab(cab_id)
            .await?
            .map(|c| places::name(&c.0).to_string())
            .unwrap_or_default())
    }

    async fn request_card(&mut self, req: &Req) -> R<Value> {
        let km = places::km_from_campus(&req.destination);
        let (factor, price, n) = self
            .price_factor(std::slice::from_ref(req), req.vehicle, None)
            .await?;
        let total = fares::total(km, req.vehicle, factor);
        let r = fares::rate(req.vehicle);
        let seats = matching::capacity(req.vehicle).seats as usize;
        Ok(json!({
            "kind": "request", "request": req.id, "destination": req.destination,
            "destination_name": places::name(&req.destination), "direction": req.direction,
            "slots": req.slots.iter().map(|t| iso(*t)).collect::<Vec<_>>(),
            "over": req.slots.iter().all(|s| *s <= self.now),
            "handbags": req.handbags, "suitcases": req.suitcases, "party": req.party, "vehicle": req.vehicle,
            "place_note": req.place_note, "notice": req.notice, "quote": req.quote,
            "fare": {"km": km, "total": total, "your_share": total, "full_cab_share": *fares::split(total, seats).last().unwrap(),
                     "vehicle": req.vehicle, "base": r.minimum, "included_km": r.included_km, "per_km": r.per_km,
                     "price_source": price, "quotes": n},
        }))
    }

    /// One ride as its rider sees it. Co-riders are "Rider 2" until 15 minutes
    /// before; then names and relay handles. Never an email.
    async fn trip_card(&mut self, email: &str, mine: &Req) -> R<Value> {
        let trip = self.trip(mine.cab_id.unwrap()).await?;
        let departed = self.now >= trip.departure;
        let revealed = self.fixed(trip.departure);
        let emails: Vec<String> = trip.members.iter().map(|m| m.email.clone()).collect();
        let handles = self.handles(&emails).await?;
        let mine_handle = self.handle(email).await?;
        let reported: HashSet<String> = if departed {
            self.all(
                sqlx::query("SELECT accused FROM cab_reports WHERE cab_id=$1 AND reporter=$2")
                    .bind(trip.id)
                    .bind(&mine_handle),
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect()
        } else {
            HashSet::new()
        };
        let users: HashMap<String, (String, Option<String>)> = self
            .all(
                sqlx::query("SELECT email, name, gender FROM cab_users WHERE email = ANY($1)")
                    .bind(&emails),
            )
            .await?
            .iter()
            .map(|r| (r.get("email"), (r.get("name"), r.get("gender"))))
            .collect();
        let mut members = trip.members.clone();
        members.sort_by_key(|m| (m.email != email, m.id));
        let mut riders = Vec::new();
        let mut n = 1;
        for m in &members {
            let you = m.email == email;
            if !you {
                n += 1;
            }
            let mut rider = json!({
                "you": you, "label": if you { "You".to_string() } else { format!("Rider {n}") },
                "handbags": m.handbags, "suitcases": m.suitcases, "party": m.party,
                "checked_in": m.checked_in.is_some(), "place_note": m.place_note,
                "stop": m.destination, "stop_name": places::name(&m.destination),
            });
            let handle = handles.get(&m.email).cloned().unwrap_or_default();
            if !you && (revealed || departed) {
                rider["contact"] = json!(handle);
            }
            if !you && revealed {
                let (name, gender) = users.get(&m.email).cloned().unwrap_or_default();
                rider["name"] = json!(name);
                if gender.is_some() {
                    rider["gender"] = json!(gender);
                }
            }
            if !you && departed {
                rider["reported"] = json!(reported.contains(&handle));
            }
            riders.push(rider);
        }
        let point = pickup(&mine.destination, trip.direction);
        let me = trip
            .members
            .iter()
            .find(|m| m.id == mine.id)
            .cloned()
            .unwrap_or_else(|| mine.clone());
        Ok(json!({
            "kind": "trip", "trip": trip.id, "request": mine.id,
            "fare": self.trip_fare(&trip.members, &me, trip.direction).await?,
            "stops": stops_json(&trip.members, trip.direction, Some(&mine.destination)),
            "final": trip.destination, "final_name": places::name(&trip.destination),
            "checkin": {
                "done": mine.checked_in.is_some(),
                "opens": iso(trip.departure - CHECKIN_OPENS_BEFORE),
                "closes": iso(trip.departure + CHECKIN_CLOSES_AFTER),
                "open": (trip.departure - CHECKIN_OPENS_BEFORE..=trip.departure + CHECKIN_CLOSES_AFTER).contains(&self.now),
                "point": [point.0, point.1], "radius_m": checkin_radius_m(),
            },
            "destination": mine.destination, "destination_name": places::name(&mine.destination),
            "direction": trip.direction, "departure": iso(trip.departure), "riders": riders,
            "vehicle": trip.vehicle(), "people": trip.riders(),
            "seats_left": matching::capacity(trip.vehicle().unwrap_or_default()).seats - trip.riders(),
            "handbags": trip.members.iter().map(|m| m.handbags).sum::<i32>(),
            "suitcases": trip.members.iter().map(|m| m.suitcases).sum::<i32>(),
            "fixed": revealed, "revealed": revealed, "departed": departed,
            "rode": mine.rode, "settled": trip.settled, "notice": mine.notice,
            "can_cancel": !revealed, "late_cancel": !revealed && trip.departure - self.now <= FREE_CANCEL_BEFORE,
            "quote": mine.quote, "place_note": mine.place_note,
        }))
    }

    /// One of my requests, as a trip card once grouped or a request card.
    pub async fn card(&mut self, email: &str, rid: i32) -> R<Value> {
        self.tick().await?;
        self.own(email, rid).await?;
        let req = self.request(rid).await?;
        match req.state {
            RequestState::Grouped => self.trip_card(email, &req).await,
            RequestState::Waiting => self.request_card(&req).await,
            _ => Ok(
                json!({"kind": "cancelled", "request": rid, "destination": req.destination,
                           "destination_name": places::name(&req.destination)}),
            ),
        }
    }

    /// Everything one student has going on: upcoming rides, requests still
    /// waiting, and past rides in pages (`past_before` is the cursor).
    pub async fn my_trips(
        &mut self,
        email: &str,
        past_limit: Option<usize>,
        past_before: Option<&str>,
    ) -> R<Value> {
        self.tick().await?;
        let rows = self
            .reqs(
                sqlx::query("SELECT * FROM cab_requests WHERE email=$1 AND NOT cleared AND state IN ('waiting','grouped') ORDER BY id DESC")
                    .bind(email),
                false,
            )
            .await?;
        let when = |r: &Req| r.slots.iter().max().copied().unwrap_or(0);
        let (mut old, live): (Vec<Req>, Vec<Req>) =
            rows.into_iter().partition(|r| when(r) <= self.now);
        old.sort_by_key(|r| (std::cmp::Reverse(when(r)), std::cmp::Reverse(r.id)));
        if let Some(b) = past_before {
            let mut it = b.split('~');
            let (Some(at), Some(id)) = (
                it.next().and_then(|x| x.parse::<i64>().ok()),
                it.next().and_then(|x| x.parse::<i32>().ok()),
            ) else {
                return bad("that is not a page of your past trips");
            };
            old.retain(|r| (when(r), r.id) < (at, id));
        }
        let limit = past_limit.unwrap_or(PAST_PAGE).clamp(1, 100);
        let more = old.len() > limit;
        old.truncate(limit);
        let past_next = if more {
            old.last().map(|r| format!("{}~{}", when(r), r.id))
        } else {
            None
        };
        let (mut trips, mut waiting, mut past) = (Vec::new(), Vec::new(), Vec::new());
        for r in &live {
            if r.is_grouped() {
                trips.push((when(r), self.trip_card(email, r).await?));
            } else {
                waiting.push(self.request_card(r).await?);
            }
        }
        for r in &old {
            past.push(if r.is_grouped() {
                self.trip_card(email, r).await?
            } else {
                self.request_card(r).await?
            });
        }
        trips.sort_by_key(|t| t.0);
        Ok(
            json!({"trips": trips.into_iter().map(|t| t.1).collect::<Vec<_>>(), "requests": waiting,
                  "past": past, "past_next": past_next}),
        )
    }

    /// Hide finished trips from My trips; the score and its history stay.
    pub async fn clear_history(&mut self, email: &str) -> R<u64> {
        self.exec(
            sqlx::query(
                "UPDATE cab_requests q SET cleared=TRUE WHERE q.email=$1 AND NOT q.cleared AND (
                   (q.state='waiting' AND NOT EXISTS (SELECT 1 FROM cab_request_slots s WHERE s.request_id=q.id AND s.departure>$2))
                   OR (q.state='grouped' AND EXISTS (SELECT 1 FROM cab_rides c WHERE c.id=q.cab_id AND c.departure<=$2 AND c.settled))
                   OR q.state='cancelled')",
            )
            .bind(email)
            .bind(self.now),
        )
        .await
    }

    // --- notifications --------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn notify(
        &mut self,
        email: &str,
        kind: &str,
        title: &str,
        body: &str,
        trip: Option<i32>,
        request: Option<i32>,
        dedupe: Option<String>,
    ) -> R<()> {
        let off = self
            .one(
                sqlx::query("SELECT 1 FROM cab_notification_prefs WHERE email=$1 AND category=$2 AND NOT enabled")
                    .bind(email)
                    .bind(category(kind)),
            )
            .await?;
        if off.is_some() {
            return Ok(());
        }
        let title: String = title.chars().take(120).collect();
        let body: String = body.chars().take(300).collect();
        if kind == "message"
            && let Some(t) = trip
        {
            // many messages in one ride are one notification, not many
            let old = self
                .one(
                    sqlx::query("SELECT id, times FROM cab_notifications WHERE email=$1 AND kind='message' AND trip=$2 AND NOT read")
                        .bind(email)
                        .bind(t),
                )
                .await?;
            if let Some(o) = old {
                let n = o.get::<i32, _>("times") + 1;
                let where_ = self.where_(t).await?;
                self.exec(
                    sqlx::query("UPDATE cab_notifications SET title=$1, body=$2, created=$3, times=$4 WHERE id=$5")
                        .bind(format!("{n} new messages · {where_} ride"))
                        .bind(&body)
                        .bind(self.now)
                        .bind(n)
                        .bind(o.get::<i32, _>("id")),
                )
                .await?;
                return Ok(());
            }
        }
        let added = self
            .exec(
                sqlx::query(
                    "INSERT INTO cab_notifications(email, kind, title, body, trip, request, created, dedupe)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING",
                )
                .bind(email)
                .bind(kind)
                .bind(&title)
                .bind(&body)
                .bind(trip)
                .bind(request)
                .bind(self.now)
                .bind(dedupe),
            )
            .await?;
        if added > 0 {
            self.exec(
                sqlx::query(
                    "INSERT INTO cab_push_jobs(user_email, device_id, title, body, request)
                     SELECT user_email, device_id, $2, $3, $4 FROM push_devices WHERE user_email=$1",
                )
                .bind(email)
                .bind(&title)
                .bind(&body)
                .bind(request),
            )
            .await?;
        }
        Ok(())
    }

    async fn notify_cab(
        &mut self,
        cab: i32,
        kind: &str,
        title: &str,
        body: &str,
        but: Option<&str>,
        dedupe: Option<String>,
    ) -> R<()> {
        for (rid, email) in self.member_rows(cab).await? {
            if Some(email.as_str()) != but {
                let d = dedupe.as_ref().map(|d| format!("{d}:{cab}"));
                self.notify(&email, kind, title, body, Some(cab), Some(rid), d)
                    .await?;
            }
        }
        Ok(())
    }

    /// Reminders that depend only on the clock, each sent once: about an hour
    /// to go, names are in, check-in is open, did you take it, nobody came.
    async fn notify_due(&mut self) -> R<()> {
        let now = self.now;
        let cabs: Vec<(i32, i64)> = self
            .all(
                sqlx::query("SELECT id, departure FROM cab_rides WHERE NOT settled AND departure>$1 AND departure<$2")
                    .bind(now - 2 * 3600)
                    .bind(now + 65 * 60),
            )
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        for (cab, d) in cabs {
            let (where_, at) = (self.where_(cab).await?, clock(d));
            for (rid, email) in self.member_rows(cab).await? {
                let row = self.row(rid).await?;
                if d - 3600 <= now && now < d - REVEAL_BEFORE {
                    self.notify(
                        &email,
                        "soon",
                        &format!("Leaving at {at} for {where_}"),
                        "About an hour to go. Check the pickup and your group chat.",
                        Some(cab),
                        Some(rid),
                        Some(format!("soon:{cab}")),
                    )
                    .await?;
                } else if d - REVEAL_BEFORE <= now && now < d - CHECKIN_OPENS_BEFORE {
                    self.notify(
                        &email,
                        "reveal",
                        "See who you’re riding with",
                        &format!("Names and contacts for your {at} ride to {where_} are in."),
                        Some(cab),
                        Some(rid),
                        Some(format!("reveal:{cab}")),
                    )
                    .await?;
                } else if d - CHECKIN_OPENS_BEFORE <= now
                    && now < d + CHECKIN_CLOSES_AFTER
                    && row.checked_in.is_none()
                {
                    self.notify(
                        &email,
                        "checkin_open",
                        "Check in at the pickup",
                        &format!("Your {at} ride to {where_}: tap to check in when you’re there."),
                        Some(cab),
                        Some(rid),
                        Some(format!("checkin_open:{cab}")),
                    )
                    .await?;
                }
                if row.rode || row.checked_in.is_some() {
                    continue;
                }
                for (n, start, end) in [(1, 30, 90), (2, 90, 120)] {
                    if d + start * 60 <= now && now < d + end * 60 {
                        self.notify(&email, "confirm", "Did you take your ride?",
                            &format!("Tap “I took this ride” for your {at} ride to {where_} before it settles."),
                            Some(cab), Some(rid), Some(format!("confirm{n}:{cab}"))).await?;
                    }
                }
            }
        }
        let lonely = self
            .all(
                sqlx::query(
                    "SELECT q.id, q.email, q.destination FROM cab_requests q JOIN cab_request_slots s ON s.request_id=q.id
                     WHERE q.state='waiting' GROUP BY q.id, q.email, q.destination
                     HAVING MAX(s.departure)<=$1 AND MAX(s.departure)>$2",
                )
                .bind(now)
                .bind(now - 24 * 3600),
            )
            .await?;
        for r in lonely {
            let (rid, email, dest): (i32, String, String) = (r.get(0), r.get(1), r.get(2));
            self.notify(
                &email,
                "no_match",
                "Nobody joined your ride",
                &format!(
                    "No one was going to {} at your times. Try posting more times.",
                    places::name(&dest)
                ),
                None,
                Some(rid),
                Some(format!("no_match:{rid}")),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn notifications(
        &mut self,
        email: &str,
        limit: Option<i64>,
        before: Option<i64>,
    ) -> R<Value> {
        let limit = limit.unwrap_or(NOTIFY_PAGE).clamp(1, 100);
        let mut rows = self
            .all(
                sqlx::query("SELECT * FROM cab_notifications WHERE email=$1 AND id<$2 ORDER BY id DESC LIMIT $3")
                    .bind(email)
                    .bind(before.unwrap_or(i64::MAX))
                    .bind(limit + 1),
            )
            .await?;
        let more = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let unread: i64 = self
            .one(
                sqlx::query(
                    "SELECT COUNT(*) AS n FROM cab_notifications WHERE email=$1 AND NOT read",
                )
                .bind(email),
            )
            .await?
            .unwrap()
            .get("n");
        Ok(json!({
            "notifications": rows.iter().map(|r| {
                let kind: String = r.get("kind");
                json!({"id": r.get::<i32, _>("id"), "category": category(&kind), "kind": kind,
                       "title": r.get::<String, _>("title"), "body": r.get::<String, _>("body"),
                       "trip": r.get::<Option<i32>, _>("trip"), "request": r.get::<Option<i32>, _>("request"),
                       "at": iso(r.get("created")), "read": r.get::<bool, _>("read"), "times": r.get::<i32, _>("times")})
            }).collect::<Vec<_>>(),
            "unread": unread,
            "older": if more { rows.last().map(|r| r.get::<i32, _>("id")) } else { None },
        }))
    }

    pub async fn read_notifications(&mut self, email: &str, ids: Option<Vec<i32>>) -> R<i64> {
        match ids {
            None => {
                self.exec(
                    sqlx::query("UPDATE cab_notifications SET read=TRUE WHERE email=$1")
                        .bind(email),
                )
                .await?
            }
            Some(ids) => {
                self.exec(
                    sqlx::query(
                        "UPDATE cab_notifications SET read=TRUE WHERE email=$1 AND id = ANY($2)",
                    )
                    .bind(email)
                    .bind(ids),
                )
                .await?
            }
        };
        Ok(self
            .one(
                sqlx::query(
                    "SELECT COUNT(*) AS n FROM cab_notifications WHERE email=$1 AND NOT read",
                )
                .bind(email),
            )
            .await?
            .unwrap()
            .get("n"))
    }

    pub async fn notify_prefs(&mut self, email: &str) -> R<Value> {
        let mut prefs: BTreeMap<String, bool> = NOTIFY_CATEGORIES
            .iter()
            .map(|c| (c.to_string(), true))
            .collect();
        for r in self
            .all(
                sqlx::query("SELECT category, enabled FROM cab_notification_prefs WHERE email=$1")
                    .bind(email),
            )
            .await?
        {
            let c: String = r.get(0);
            if let Some(v) = prefs.get_mut(&c) {
                *v = r.get(1);
            }
        }
        Ok(json!(prefs))
    }

    pub async fn set_notify_prefs(
        &mut self,
        email: &str,
        changes: &HashMap<String, bool>,
    ) -> R<Value> {
        for c in changes.keys() {
            if !NOTIFY_CATEGORIES.contains(&c.as_str()) {
                return bad(format!("unknown notification setting: {c}"));
            }
        }
        for (c, on) in changes {
            self.exec(
                sqlx::query(
                    "INSERT INTO cab_notification_prefs(email, category, enabled) VALUES ($1,$2,$3)
                     ON CONFLICT (email, category) DO UPDATE SET enabled=EXCLUDED.enabled",
                )
                .bind(email)
                .bind(c)
                .bind(on),
            )
            .await?;
        }
        self.notify_prefs(email).await
    }
}

/// The places, the vehicles and the rate cards, for the app to draw pickers.
pub fn catalogue() -> Value {
    json!({
        "campus": places::CAMPUS,
        "places": places::PLACES.iter().map(|(id, name, parent, lat, lng)| json!({
            "id": id, "name": name, "parent": parent, "lat": lat, "lng": lng,
            "km": if *id == places::CAMPUS { 0.0 } else { places::km_from_campus(id) },
            "path": places::path_from_campus(id),
        })).collect::<Vec<_>>(),
        "vehicles": matching::VEHICLES.iter().map(|v| {
            let c = matching::capacity(*v);
            let r = fares::rate(*v);
            json!({"id": v, "seats": c.seats, "suitcases": c.suitcases, "handbags": c.handbags,
                   "base": r.minimum, "included_km": r.included_km, "per_km": r.per_km})
        }).collect::<Vec<_>>(),
        "limits": {"max_slots": MAX_SLOTS, "max_days_ahead": MAX_AHEAD / 86_400, "reveal_minutes": REVEAL_BEFORE / 60,
                   "free_cancel_hours": FREE_CANCEL_BEFORE / 3600, "message_max": MESSAGE_MAX, "note_max": NOTE_MAX},
    })
}

pub mod places {
    //! The known stops, the route tree rooted at campus, and road distances.
    //!
    //! Every stop is on one tree rooted at IITGN. Riders share a vehicle only when
    //! all their stops lie on one path from campus, so one vehicle drops (or picks
    //! up) everyone with no detour.
    //!
    //! Distances are road kilometres measured once with OSRM from the coordinates
    //! below. A leg not in the table falls back to the straight line times 1.3.

    pub const CAMPUS: &str = "iitgn";

    /// (id, name, parent, lat, lng). Campus is the root and has no parent.
    pub const PLACES: &[(&str, &str, Option<&str>, f64, f64)] = &[
        ("iitgn", "IITGN (Palaj)", None, 23.210934, 72.683313),
        ("gift_city", "GIFT City", Some("iitgn"), 23.1651, 72.6758),
        (
            "dholakuva_metro",
            "Dholakuva metro station",
            Some("iitgn"),
            23.1859,
            72.6434,
        ),
        (
            "rakshashakti",
            "Rakshashakti circle",
            Some("dholakuva_metro"),
            23.1859,
            72.6388,
        ),
        ("kudasan", "Kudasan", Some("rakshashakti"), 23.1796, 72.6347),
        ("sargasan", "Sargasan", Some("kudasan"), 23.1929, 72.6156),
        (
            "gandhinagar_capital",
            "Gandhinagar Capital railway station",
            Some("sargasan"),
            23.2348,
            72.6303,
        ),
        (
            "airport",
            "SVP International Airport",
            Some("rakshashakti"),
            23.0764,
            72.6310,
        ),
        (
            "sabarmati",
            "Sabarmati railway station",
            Some("rakshashakti"),
            23.0771,
            72.5888,
        ),
        (
            "ahmedabad_jn",
            "Ahmedabad Junction (Kalupur)",
            Some("sabarmati"),
            23.0264,
            72.6010,
        ),
        (
            "maninagar",
            "Maninagar railway station",
            Some("ahmedabad_jn"),
            22.9985,
            72.6115,
        ),
    ];

    /// Road km measured with OSRM: (from, to, km), from campus and between every
    /// two stops on one path.
    const ROAD_KM: &[(&str, &str, f64)] = &[
        ("iitgn", "gift_city", 6.7),
        ("iitgn", "dholakuva_metro", 7.2),
        ("iitgn", "rakshashakti", 7.8),
        ("iitgn", "kudasan", 8.7),
        ("iitgn", "sargasan", 11.4),
        ("iitgn", "gandhinagar_capital", 14.1),
        ("iitgn", "airport", 23.7),
        ("iitgn", "sabarmati", 22.2),
        ("iitgn", "ahmedabad_jn", 29.7),
        ("iitgn", "maninagar", 33.5),
        ("dholakuva_metro", "rakshashakti", 0.6),
        ("dholakuva_metro", "kudasan", 1.5),
        ("rakshashakti", "kudasan", 1.1),
        ("dholakuva_metro", "sargasan", 4.2),
        ("rakshashakti", "sargasan", 3.8),
        ("kudasan", "sargasan", 5.0),
        ("dholakuva_metro", "gandhinagar_capital", 9.2),
        ("rakshashakti", "gandhinagar_capital", 8.8),
        ("kudasan", "gandhinagar_capital", 9.1),
        ("sargasan", "gandhinagar_capital", 7.1),
        ("dholakuva_metro", "airport", 16.5),
        ("rakshashakti", "airport", 16.1),
        ("dholakuva_metro", "sabarmati", 14.9),
        ("rakshashakti", "sabarmati", 14.5),
        ("dholakuva_metro", "ahmedabad_jn", 21.7),
        ("rakshashakti", "ahmedabad_jn", 21.3),
        ("sabarmati", "ahmedabad_jn", 9.6),
        ("dholakuva_metro", "maninagar", 25.5),
        ("rakshashakti", "maninagar", 25.1),
        ("sabarmati", "maninagar", 13.4),
        ("ahmedabad_jn", "maninagar", 3.8),
    ];

    const ROAD_FACTOR: f64 = 1.3;

    fn entry(
        place: &str,
    ) -> Option<&'static (&'static str, &'static str, Option<&'static str>, f64, f64)> {
        PLACES.iter().find(|p| p.0 == place)
    }

    pub fn is_place(place: &str) -> bool {
        entry(place).is_some()
    }

    /// A stop a rider may pick: any known place except campus itself.
    pub fn is_stop(place: &str) -> bool {
        place != CAMPUS && is_place(place)
    }

    pub fn name(place: &str) -> &str {
        entry(place).map(|p| p.1).unwrap_or(place)
    }

    pub fn coords(place: &str) -> (f64, f64) {
        entry(place).map(|p| (p.3, p.4)).unwrap_or((0.0, 0.0))
    }

    pub fn parent(place: &str) -> Option<&'static str> {
        entry(place).and_then(|p| p.2)
    }

    /// Stops from campus to `place`, campus first and `place` last.
    pub fn path_from_campus(place: &str) -> Vec<String> {
        let mut chain = vec![place.to_string()];
        let mut at = place;
        while let Some(up) = parent(at) {
            chain.push(up.to_string());
            at = up;
        }
        chain.reverse();
        chain
    }

    fn depth(place: &str) -> usize {
        path_from_campus(place).len()
    }

    /// True when `stop` is passed on the way to `destination` (zero detour).
    pub fn on_route(stop: &str, destination: &str) -> bool {
        path_from_campus(destination).iter().any(|p| p == stop)
    }

    pub fn can_share(a: &str, b: &str) -> bool {
        on_route(a, b) || on_route(b, a)
    }

    /// Every stop lies on one path from campus, so one vehicle passes them all.
    pub fn one_route<'a>(stops: impl IntoIterator<Item = &'a str>) -> bool {
        let stops: Vec<&str> = stops.into_iter().collect();
        let Some(far) = stops.iter().max_by_key(|s| depth(s)) else {
            return true;
        };
        stops.iter().all(|s| on_route(s, far))
    }

    /// Drop order: nearest campus first, farthest last; each stop once.
    pub fn order_for_drop<'a>(stops: impl IntoIterator<Item = &'a str>) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for s in stops {
            if !out.iter().any(|o| o == s) {
                out.push(s.to_string());
            }
        }
        out.sort_by_key(|s| depth(s));
        out
    }

    /// The final stop of a group: the one farthest from campus.
    pub fn farthest<'a>(stops: impl IntoIterator<Item = &'a str>) -> String {
        order_for_drop(stops)
            .pop()
            .unwrap_or_else(|| CAMPUS.to_string())
    }

    /// Straight-line metres between two points (haversine).
    pub fn straight_m(a: (f64, f64), b: (f64, f64)) -> f64 {
        let r = 6_371_000.0_f64;
        let (la1, la2) = (a.0.to_radians(), b.0.to_radians());
        let dla = (b.0 - a.0).to_radians();
        let dlo = (b.1 - a.1).to_radians();
        let h = (dla / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlo / 2.0).sin().powi(2);
        2.0 * r * h.sqrt().asin()
    }

    /// Road km from `a` to `b` (one is on the other's path): measured, or an estimate.
    pub fn leg_km(a: &str, b: &str) -> f64 {
        if let Some(&(_, _, km)) = ROAD_KM.iter().find(|(x, y, _)| *x == a && *y == b) {
            return km;
        }
        ((straight_m(coords(a), coords(b)) / 1000.0) * ROAD_FACTOR * 10.0).round() / 10.0
    }

    pub fn km_from_campus(place: &str) -> f64 {
        leg_km(CAMPUS, place)
    }

    /// The (from, to, km) legs a vehicle drives to serve `stops`, campus end first.
    pub fn legs_for<'a>(stops: impl IntoIterator<Item = &'a str>) -> Vec<(String, String, f64)> {
        let mut chain = vec![CAMPUS.to_string()];
        chain.extend(order_for_drop(stops));
        chain
            .windows(2)
            .map(|w| (w[0].clone(), w[1].clone(), leg_km(&w[0], &w[1])))
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn paths_and_routes() {
            assert_eq!(
                path_from_campus("airport"),
                vec!["iitgn", "dholakuva_metro", "rakshashakti", "airport"]
            );
            assert!(on_route("rakshashakti", "airport"));
            assert!(!on_route("kudasan", "airport"));
            assert!(one_route(["rakshashakti", "airport", "dholakuva_metro"]));
            assert!(!one_route(["kudasan", "airport"]));
            assert!(!one_route(["gift_city", "airport"]));
            assert_eq!(farthest(["rakshashakti", "airport"]), "airport");
            assert_eq!(
                order_for_drop(["maninagar", "sabarmati", "sabarmati"]),
                vec!["sabarmati", "maninagar"]
            );
        }

        #[test]
        fn every_parent_exists_and_every_stop_reaches_campus() {
            for (id, _, parent, _, _) in PLACES {
                if let Some(p) = parent {
                    assert!(is_place(p), "{id} has unknown parent {p}");
                }
                assert_eq!(path_from_campus(id)[0], CAMPUS);
            }
        }

        #[test]
        fn measured_legs_win_over_estimates() {
            assert_eq!(leg_km("iitgn", "airport"), 23.7);
            assert_eq!(leg_km("rakshashakti", "airport"), 16.1);
            let legs = legs_for(["airport", "rakshashakti"]);
            assert_eq!(legs.len(), 2);
            assert_eq!(legs[0].2, 7.8);
            assert_eq!(legs[1].2, 16.1);
        }
    }
}

pub mod matching {
    //! Who may share one vehicle, kept free of storage and time.
    //!
    //! Requests share only when they go the same direction at a common time and
    //! their stops lie on one route from campus (no detour). A group must fit in
    //! one vehicle. Credibility orders candidates so reliable riders end up
    //! together, and decides who keeps a seat; it never decides whether someone rides.

    use std::collections::HashSet;

    use super::places;
    use super::{Direction, RequestState, Vehicle};

    pub struct Capacity {
        pub seats: i32,
        pub suitcases: i32,
        pub handbags: i32,
    }

    pub const VEHICLES: [Vehicle; 2] = Vehicle::ALL;

    pub fn capacity(vehicle: Vehicle) -> Capacity {
        match vehicle {
            Vehicle::Auto => Capacity {
                seats: 3,
                suitcases: 1,
                handbags: 3,
            },
            _ => Capacity {
                seats: 4,
                suitcases: 3,
                handbags: 4,
            },
        }
    }

    /// One student's request, as the rules see it.
    #[derive(Clone, Debug, Default)]
    pub struct Req {
        pub id: i32,
        pub email: String,
        pub destination: String,
        pub direction: Direction,
        /// Every time offered; once grouped, only the ride's time.
        pub slots: Vec<i64>,
        pub handbags: i32,
        pub suitcases: i32,
        /// waiting | grouped | cancelled
        pub state: RequestState,
        pub cab_id: Option<i32>,
        pub rode: bool,
        pub checked_in: Option<i64>,
        pub notice: Option<String>,
        pub created: i64,
        pub score: f64,
        /// Emails never to share a vehicle with, both ways.
        pub avoid: HashSet<String>,
        /// People on this request, poster included.
        pub party: i32,
        pub vehicle: Vehicle,
        pub quote: Option<i64>,
        pub place_note: Option<String>,
    }

    impl Req {
        pub fn is_waiting(&self) -> bool {
            self.state == RequestState::Waiting
        }
        pub fn is_grouped(&self) -> bool {
            self.state == RequestState::Grouped
        }
    }

    /// Seats these requests take: a rider with a friend takes two.
    pub fn people(reqs: &[Req]) -> i32 {
        reqs.iter().map(|r| r.party).sum()
    }

    pub fn on_one_route(reqs: &[Req]) -> bool {
        places::one_route(reqs.iter().map(|r| r.destination.as_str()))
    }

    /// Two of these riders must never share (one blocked the other).
    pub fn apart(reqs: &[Req]) -> bool {
        reqs.iter().enumerate().any(|(i, r)| {
            reqs.iter()
                .enumerate()
                .any(|(j, o)| i != j && !o.email.is_empty() && r.avoid.contains(&o.email))
        })
    }

    /// Can these requests ride in one vehicle together?
    pub fn fits(reqs: &[Req]) -> bool {
        if apart(reqs) || !on_one_route(reqs) {
            return false;
        }
        let mut kinds = reqs.iter().map(|r| r.vehicle).collect::<HashSet<_>>();
        if kinds.len() > 1 {
            return false;
        }
        let cap = capacity(kinds.drain().next().unwrap_or_default());
        people(reqs) <= cap.seats
            && reqs.iter().map(|r| r.suitcases).sum::<i32>() <= cap.suitcases
            && reqs.iter().map(|r| r.handbags).sum::<i32>() <= cap.handbags
    }

    fn with(group: &[Req], one: &Req) -> Vec<Req> {
        let mut out = group.to_vec();
        out.push(one.clone());
        out
    }

    /// Highest credibility first; among equals, whoever posted first.
    pub fn by_credibility(mut reqs: Vec<Req>) -> Vec<Req> {
        reqs.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap()
                .then(a.created.cmp(&b.created))
                .then(a.id.cmp(&b.id))
        });
        reqs
    }

    /// The member a more reliable newcomer would replace.
    pub fn weakest(reqs: &[Req]) -> Option<&Req> {
        reqs.iter().min_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap()
                .then(b.created.cmp(&a.created))
                .then(a.id.cmp(&b.id))
        })
    }

    /// Grow a group from `seed`, taking the most credible candidates that fit.
    pub fn fill(seed: &Req, candidates: Vec<Req>) -> Vec<Req> {
        let mut group = vec![seed.clone()];
        for cand in by_credibility(candidates) {
            if cand.id != seed.id && fits(&with(&group, &cand)) {
                group.push(cand);
            }
        }
        group
    }

    fn mean(cab: &[Req]) -> f64 {
        cab.iter().map(|r| r.score).sum::<f64>() / cab.len() as f64
    }

    /// The final groups for everyone leaving at one time, run once at the freeze.
    ///
    /// 1. Seat as many riders as possible: nobody is left out for their score
    ///    while a vehicle has room.
    /// 2. Riders with similar scores ride together: highest score down, filling
    ///    one vehicle at a time.
    ///
    /// A vehicle of one is not a group: a lone rider moves into a vehicle with
    /// room, or swaps with the member of the nearest-scored vehicle who rides best
    /// with them, so that vehicle still has two.
    ///
    /// Returns (groups, unmatched).
    pub fn regroup(reqs: Vec<Req>) -> (Vec<Vec<Req>>, Vec<Req>) {
        let mut cabs: Vec<Vec<Req>> = Vec::new();
        for r in by_credibility(reqs) {
            match cabs.iter_mut().find(|c| fits(&with(c, &r))) {
                Some(cab) => cab.push(r),
                None => cabs.push(vec![r]),
            }
        }
        let lonely: Vec<Req> = cabs
            .iter()
            .filter(|c| c.len() == 1)
            .map(|c| c[0].clone())
            .collect();
        let mut unmatched = Vec::new();
        for lone in lonely {
            cabs.retain(|c| !(c.len() == 1 && c[0].id == lone.id));
            let mut order: Vec<usize> = (0..cabs.len()).collect();
            order.sort_by(|&a, &b| {
                (mean(&cabs[a]) - lone.score)
                    .abs()
                    .partial_cmp(&(mean(&cabs[b]) - lone.score).abs())
                    .unwrap()
            });
            if let Some(&home) = order.iter().find(|&&i| fits(&with(&cabs[i], &lone))) {
                cabs[home].push(lone);
                continue;
            }
            let mut swap = None;
            'search: for &i in &order {
                if cabs[i].len() < 3 {
                    continue;
                }
                let mut members: Vec<&Req> = cabs[i].iter().collect();
                members.sort_by(|a, b| {
                    (a.score - lone.score)
                        .abs()
                        .partial_cmp(&(b.score - lone.score).abs())
                        .unwrap()
                });
                for m in members {
                    let rest: Vec<Req> = cabs[i].iter().filter(|x| x.id != m.id).cloned().collect();
                    if fits(&[lone.clone(), m.clone()]) && fits(&rest) {
                        swap = Some((i, m.id));
                        break 'search;
                    }
                }
            }
            match swap {
                Some((i, mid)) => {
                    let pos = cabs[i].iter().position(|x| x.id == mid).unwrap();
                    let m = cabs[i].remove(pos);
                    cabs.push(vec![m, lone]);
                }
                None => unmatched.push(lone),
            }
        }
        (
            cabs.into_iter().filter(|c| c.len() >= 2).collect(),
            unmatched,
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn req(id: i32, dest: &str, score: f64) -> Req {
            Req {
                id,
                email: format!("s{id}@iitgn.ac.in"),
                destination: dest.into(),
                direction: Direction::FromCampus,
                slots: vec![1000],
                state: RequestState::Waiting,
                score,
                party: 1,
                vehicle: Vehicle::Cab,
                ..Default::default()
            }
        }

        #[test]
        fn a_cab_holds_four_people_and_three_suitcases() {
            let four: Vec<Req> = (1..=4).map(|i| req(i, "airport", 70.0)).collect();
            assert!(fits(&four));
            let five: Vec<Req> = (1..=5).map(|i| req(i, "airport", 70.0)).collect();
            assert!(!fits(&five));
            let mut heavy: Vec<Req> = (1..=2).map(|i| req(i, "airport", 70.0)).collect();
            heavy[0].suitcases = 2;
            heavy[1].suitcases = 2;
            assert!(!fits(&heavy));
        }

        #[test]
        fn diverging_branches_never_share() {
            assert!(!fits(&[req(1, "airport", 70.0), req(2, "kudasan", 70.0)]));
            assert!(fits(&[
                req(1, "airport", 70.0),
                req(2, "rakshashakti", 70.0)
            ]));
        }

        #[test]
        fn a_block_keeps_two_riders_apart() {
            let a = req(1, "airport", 70.0);
            let mut b = req(2, "airport", 70.0);
            b.avoid.insert(a.email.clone());
            assert!(!fits(&[a, b]));
        }

        #[test]
        fn autos_and_cabs_never_mix() {
            let a = req(1, "airport", 70.0);
            let mut b = req(2, "airport", 70.0);
            b.vehicle = Vehicle::Auto;
            assert!(!fits(&[a, b]));
        }

        #[test]
        fn regroup_seats_everyone_and_keeps_similar_scores_together() {
            let scores = [95.0, 90.0, 85.0, 80.0, 40.0, 35.0, 30.0];
            let reqs: Vec<Req> = scores
                .iter()
                .enumerate()
                .map(|(i, s)| req(i as i32 + 1, "airport", *s))
                .collect();
            let (groups, unmatched) = regroup(reqs);
            assert!(unmatched.is_empty());
            assert_eq!(groups.iter().map(|g| g.len()).sum::<usize>(), 7);
            assert!(groups[0].iter().all(|r| r.score >= 80.0));
        }

        #[test]
        fn regroup_never_leaves_a_lone_rider_when_a_swap_works() {
            let reqs: Vec<Req> = (1..=5)
                .map(|i| req(i, "airport", 70.0 + i as f64))
                .collect();
            let (groups, unmatched) = regroup(reqs);
            assert!(unmatched.is_empty());
            assert!(groups.iter().all(|g| g.len() >= 2));
        }
    }
}

pub mod fares {
    //! Estimated fares and how a group splits one.
    //!
    //! The app never takes money. It only estimates what the vehicle costs and each
    //! rider's share, so students see what sharing saves them.
    //!
    //! - Auto: Gujarat meter fare (June 2026): ₹25 for the first 1.2 km, then ₹20/km.
    //! - Cab: city taxi benchmark: ₹60 start plus ₹16.5/km. App fares vary with surge,
    //!   so riders may enter the Ola/Uber price they saw; `factor` scales to it.

    pub struct Rate {
        pub minimum: f64,
        pub included_km: f64,
        pub per_km: f64,
    }

    use super::Vehicle;

    pub fn rate(vehicle: Vehicle) -> Rate {
        match vehicle {
            Vehicle::Auto => Rate {
                minimum: 25.0,
                included_km: 1.2,
                per_km: 20.0,
            },
            _ => Rate {
                minimum: 60.0,
                included_km: 0.0,
                per_km: 16.5,
            },
        }
    }

    /// The rate-card fare before rounding.
    pub fn raw(km: f64, vehicle: Vehicle) -> f64 {
        let r = rate(vehicle);
        r.minimum + r.per_km * (km - r.included_km).max(0.0)
    }

    /// Whole-rupee fare for one vehicle over `km`, scaled by `factor`.
    pub fn total(km: f64, vehicle: Vehicle, factor: f64) -> i64 {
        (raw(km, vehicle) * factor).round() as i64
    }

    /// Shares of `amount` for `riders` people that add up exactly; the rupees that
    /// do not divide go one each to the first riders.
    pub fn split(amount: i64, riders: usize) -> Vec<i64> {
        let riders = riders.max(1);
        let each = amount / riders as i64;
        let extra = (amount % riders as i64) as usize;
        (0..riders).map(|i| each + i64::from(i < extra)).collect()
    }

    /// Split one vehicle's fare leg by leg.
    ///
    /// `kms` are the legs, campus end first. `riders` are (last leg, people) per
    /// request, in seat order: a request rides legs 0..=last. Each leg's cost is
    /// shared by everyone aboard it; the minimum fare belongs to the campus leg.
    /// The exact parts are scaled to the whole-rupee fare and rounded by largest
    /// remainder, earlier seats first on a tie, so the shares add up exactly.
    ///
    /// Returns (fare, share per request, (leg cost, people aboard) per leg).
    pub fn by_legs(
        kms: &[f64],
        riders: &[(usize, i64)],
        vehicle: Vehicle,
        factor: f64,
    ) -> (i64, Vec<i64>, Vec<(f64, i64)>) {
        let fare = total(kms.iter().sum(), vehicle, factor);
        let r = rate(vehicle);
        let mut free = r.included_km;
        let mut cost: Vec<f64> = kms
            .iter()
            .map(|&km| {
                let paid = (km - free).max(0.0);
                free = (free - km).max(0.0);
                r.per_km * paid
            })
            .collect();
        cost[0] += r.minimum;
        let aboard: Vec<i64> = (0..kms.len())
            .map(|leg| {
                riders
                    .iter()
                    .filter(|(last, _)| *last >= leg)
                    .map(|(_, p)| p)
                    .sum()
            })
            .collect();
        let seats: Vec<(usize, usize)> = riders
            .iter()
            .enumerate()
            .flat_map(|(i, &(last, people))| (0..people).map(move |_| (i, last)))
            .collect();
        let exact: Vec<f64> = seats
            .iter()
            .map(|&(_, last)| {
                (0..=last)
                    .map(|leg| cost[leg] / aboard[leg].max(1) as f64)
                    .sum()
            })
            .collect();
        let sum: f64 = exact.iter().sum();
        let scale = if sum > 0.0 { fare as f64 / sum } else { 0.0 };
        let want: Vec<f64> = exact.iter().map(|e| e * scale).collect();
        let mut whole: Vec<i64> = want.iter().map(|w| (w + 1e-9).floor() as i64).collect();
        let left = fare - whole.iter().sum::<i64>();
        let mut order: Vec<usize> = (0..seats.len()).collect();
        order.sort_by(|&a, &b| {
            let ra = want[a] - whole[a] as f64;
            let rb = want[b] - whole[b] as f64;
            if (ra - rb).abs() < 1e-9 {
                a.cmp(&b)
            } else {
                rb.partial_cmp(&ra).unwrap()
            }
        });
        for &seat in order.iter().take(left.max(0) as usize) {
            whole[seat] += 1;
        }
        let mut shares = vec![0; riders.len()];
        for (&(i, _), w) in seats.iter().zip(&whole) {
            shares[i] += w;
        }
        let legs = cost
            .iter()
            .zip(&aboard)
            .map(|(c, &n)| (c * scale, n))
            .collect();
        (fare, shares, legs)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn rate_cards() {
            assert_eq!(total(10.0, Vehicle::Cab, 1.0), 225);
            assert_eq!(total(1.0, Vehicle::Auto, 1.0), 25);
            assert_eq!(total(2.2, Vehicle::Auto, 1.0), 45);
        }

        #[test]
        fn split_adds_up_and_differs_by_at_most_one() {
            let s = split(101, 3);
            assert_eq!(s, vec![34, 34, 33]);
            assert_eq!(s.iter().sum::<i64>(), 101);
        }

        #[test]
        fn one_stop_is_an_even_split() {
            let (fare, shares, _) = by_legs(&[10.0], &[(0, 1), (0, 1), (0, 1)], Vehicle::Cab, 1.0);
            assert_eq!(fare, 225);
            assert_eq!(shares, split(225, 3));
        }

        #[test]
        fn the_rider_dropped_first_pays_less_and_shares_add_up() {
            // campus -> rakshashakti (7.8) -> airport (16.1)
            let (fare, shares, legs) = by_legs(&[7.8, 16.1], &[(0, 1), (1, 1)], Vehicle::Cab, 1.0);
            assert_eq!(fare, total(23.9, Vehicle::Cab, 1.0));
            assert_eq!(shares.iter().sum::<i64>(), fare);
            assert!(shares[0] < shares[1]);
            assert_eq!(legs[0].1, 2);
            assert_eq!(legs[1].1, 1);
        }

        #[test]
        fn a_party_pays_a_share_per_person() {
            let (fare, shares, _) = by_legs(&[10.0], &[(0, 2), (0, 1)], Vehicle::Cab, 1.0);
            assert_eq!(shares.iter().sum::<i64>(), fare);
            assert_eq!(shares[0], 150);
            assert_eq!(shares[1], 75);
        }

        #[test]
        fn a_factor_scales_the_fare() {
            assert_eq!(total(10.0, Vehicle::Cab, 2.0), 450);
        }
    }
}

pub mod credibility {
    //! Credibility scores: where they start, what moves them, and how they fade.
    //!
    //! The score only decides who rides with whom and who keeps a seat when a more
    //! reliable student arrives. It never decides whether someone rides.

    /// A new student starts in the middle, so there is room to earn and to lose.
    pub const START: f64 = 70.0;
    pub const LOW: f64 = 0.0;
    pub const HIGH: f64 = 100.0;
    /// Below START, half the gap comes back in this many days.
    pub const FADE_HALF_LIFE_DAYS: f64 = 30.0;
    pub const DAY: f64 = 24.0 * 60.0 * 60.0;
    pub const LATE_CANCEL_PENALTY: f64 = -10.0;
    /// Not turning up strands the others, so it costs more than cancelling late.
    pub const NO_SHOW_PENALTY: f64 = -20.0;
    pub const RODE_REWARD: f64 = 5.0;
    /// A no-show penalty needs this many distinct reporters (fewer in a cab of 2).
    pub const REPORTS_NEEDED: usize = 2;
    /// Each reporter's reports count at most this many times per window.
    pub const REPORT_CAP: i64 = 3;
    pub const REPORT_WINDOW: i64 = 30 * 24 * 60 * 60;

    pub fn clamp(score: f64) -> f64 {
        score.clamp(LOW, HIGH)
    }

    pub fn after_late_cancel(score: f64) -> f64 {
        clamp(score + LATE_CANCEL_PENALTY)
    }

    pub fn after_no_show(score: f64) -> f64 {
        clamp(score + NO_SHOW_PENALTY)
    }

    pub fn after_rode(score: f64) -> f64 {
        clamp(score + RODE_REWARD)
    }

    /// Where a score below START has drifted after `seconds`: the gap halves every
    /// FADE_HALF_LIFE_DAYS. A score at or above START never fades.
    pub fn faded(score: f64, seconds: f64) -> f64 {
        if score >= START || seconds <= 0.0 {
            return score;
        }
        START - (START - score) * 0.5_f64.powf(seconds / DAY / FADE_HALF_LIFE_DAYS)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn penalties_and_reward_stay_in_range() {
            assert_eq!(after_no_show(10.0), 0.0);
            assert_eq!(after_rode(98.0), 100.0);
            assert_eq!(after_late_cancel(70.0), 60.0);
        }

        #[test]
        fn low_scores_fade_back_halfway_in_a_half_life() {
            assert!((faded(50.0, 30.0 * DAY) - 60.0).abs() < 1e-9);
            assert_eq!(faded(90.0, 300.0 * DAY), 90.0);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Cabshare against a real Postgres: the rules of the standalone app's spec,
    //! end to end through the store. Each test gets its own schema, so they run in
    //! parallel. Set TEST_POSTGRES_URL to run them; without it they pass vacuously,
    //! like the push tests.

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde_json::{Value, json};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool};

    use super::credibility as cred;
    use super::fares;
    use super::places;
    use super::{Cx, Direction, Error, REVEAL_BEFORE, RideIn, Vehicle, iso};

    static SCHEMA: AtomicU32 = AtomicU32::new(0);

    const A: &str = "a@iitgn.ac.in";
    const B: &str = "b@iitgn.ac.in";
    const C: &str = "c@iitgn.ac.in";
    const D: &str = "d@iitgn.ac.in";
    const NOW: i64 = 1_800_000_000;
    const HOUR: i64 = 3600;
    const SLOT: i64 = NOW + 5 * HOUR;
    const LATER: i64 = NOW + 7 * HOUR;

    struct World {
        pool: PgPool,
        now: i64,
    }

    async fn world() -> Option<World> {
        let url = std::env::var("TEST_POSTGRES_URL").ok()?;
        let schema = format!(
            "cab_test_{}_{}",
            std::process::id(),
            SCHEMA.fetch_add(1, Ordering::SeqCst)
        );
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        admin
            .execute(sqlx::AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
            )))
            .await
            .unwrap();
        let path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |conn, _| {
                let sql = format!("SET search_path TO {path}");
                Box::pin(async move {
                    conn.execute(sqlx::AssertSqlSafe(sql)).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        pool.execute(
            "CREATE TABLE push_devices (user_email VARCHAR(255) NOT NULL, device_id VARCHAR(255) NOT NULL,
             token TEXT NOT NULL, reminders_enabled BOOLEAN NOT NULL DEFAULT TRUE,
             updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (user_email, device_id))",
        )
        .await
        .unwrap();
        super::initialize_table(&pool).await.unwrap();
        let w = World { pool, now: NOW };
        for (e, n) in [(A, "Asha"), (B, "Bharat"), (C, "Chitra"), (D, "Dev")] {
            w.with(|cx| Box::pin(async move { cx.sign_in(e, n).await }))
                .await
                .unwrap();
        }
        Some(w)
    }

    type Step<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Error>> + Send + 'a>>;

    impl World {
        async fn with<T>(
            &self,
            f: impl for<'c> FnOnce(&'c mut Cx) -> Step<'c, T>,
        ) -> Result<T, Error> {
            let mut cx = Cx::begin(&self.pool, self.now).await?;
            let out = f(&mut cx).await;
            if out.is_ok() {
                cx.commit().await?;
            }
            out
        }

        async fn post(&self, email: &'static str, dest: &str, slots: &[i64]) -> i32 {
            self.try_post(email, ride(dest, slots)).await.unwrap()
        }

        async fn try_post(&self, email: &'static str, input: RideIn) -> Result<i32, Error> {
            self.with(move |cx| Box::pin(async move { cx.post_request(email, &input).await }))
                .await
        }

        async fn card(&self, email: &'static str, rid: i32) -> Value {
            self.with(move |cx| Box::pin(async move { cx.card(email, rid).await }))
                .await
                .unwrap()
        }

        async fn trips(&self, email: &'static str) -> Value {
            self.with(move |cx| Box::pin(async move { cx.my_trips(email, None, None).await }))
                .await
                .unwrap()
        }

        async fn feed(&self, email: &'static str) -> Value {
            self.with(move |cx| {
                Box::pin(async move { cx.feed(email, None, None, None, None).await })
            })
            .await
            .unwrap()
        }

        async fn score(&self, email: &'static str) -> f64 {
            self.with(move |cx| Box::pin(async move { cx.score(email).await }))
                .await
                .unwrap()
        }

        async fn tick(&self) {
            self.with(|cx| Box::pin(async move { cx.tick().await }))
                .await
                .unwrap()
        }
    }

    fn ride(dest: &str, slots: &[i64]) -> RideIn {
        RideIn {
            destination: dest.into(),
            direction: Direction::FromCampus,
            slots: slots.iter().map(|t| json!(iso(*t))).collect(),
            handbags: Some(0),
            suitcases: Some(0),
            ..Default::default()
        }
    }

    fn bad(r: Result<impl std::fmt::Debug, Error>) -> String {
        match r {
            Err(Error::Bad(why)) => why,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn two_riders_at_one_time_share_and_stay_anonymous_until_15_minutes() {
        let Some(mut w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        assert_eq!(w.card(A, a).await["kind"], "request");
        let b = w.post(B, "airport", &[SLOT]).await;
        let card = w.card(A, a).await;
        assert_eq!(card["kind"], "trip");
        assert_eq!(card["people"], 2);
        let other = card["riders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["you"] == false)
            .unwrap()
            .clone();
        assert_eq!(other["label"], "Rider 2");
        assert!(other.get("name").is_none() && other.get("contact").is_none());
        assert!(
            !card.to_string().contains(B),
            "an email address leaked: {card}"
        );
        // 15 minutes before: names and relay handles, still never the address
        w.now = SLOT - REVEAL_BEFORE;
        let card = w.card(B, b).await;
        let other = card["riders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["you"] == false)
            .unwrap()
            .clone();
        assert_eq!(other["name"], "Asha");
        assert!(other["contact"].as_str().unwrap().starts_with("relay-"));
        assert!(!card.to_string().contains(A));
    }

    #[tokio::test]
    async fn riders_on_diverging_roads_never_share_but_stops_on_the_way_do() {
        let Some(w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        let k = w.post(B, "kudasan", &[SLOT]).await;
        assert_eq!(w.card(A, a).await["kind"], "request");
        assert_eq!(w.card(B, k).await["kind"], "request");
        let r = w.post(C, "rakshashakti", &[SLOT]).await;
        let card = w.card(C, r).await;
        assert_eq!(card["kind"], "trip");
        assert_eq!(card["final"], "airport");
        // the rider dropped first pays less; the shares add up to the fare
        let fare_c = card["fare"].clone();
        let fare_a = w.card(A, a).await["fare"].clone();
        assert!(fare_c["your_share"].as_i64() < fare_a["your_share"].as_i64());
        assert_eq!(
            fare_c["your_share"].as_i64().unwrap() + fare_a["your_share"].as_i64().unwrap(),
            fare_a["total"].as_i64().unwrap()
        );
        assert_eq!(
            fare_a["total"].as_i64().unwrap(),
            fares::total(7.8 + 16.1, Vehicle::Cab, 1.0)
        );
    }

    #[tokio::test]
    async fn one_student_never_holds_one_time_twice() {
        let Some(w) = world().await else { return };
        w.post(A, "airport", &[SLOT, LATER]).await;
        let why = bad(w.try_post(A, ride("kudasan", &[LATER])).await);
        assert!(why.contains("already have a trip at that time"), "{why}");
    }

    #[tokio::test]
    async fn bad_input_is_refused_in_words() {
        let Some(w) = world().await else { return };
        assert!(bad(w.try_post(A, ride("mars", &[SLOT])).await).contains("unknown destination"));
        assert!(bad(w.try_post(A, ride("airport", &[NOW - 10])).await).contains("future"));
        assert!(
            bad(w
                .try_post(A, ride("airport", &[NOW + 40 * 24 * HOUR]))
                .await)
            .contains("30 days")
        );
        let mut heavy = ride("airport", &[SLOT]);
        heavy.suitcases = Some(4);
        assert!(bad(w.try_post(A, heavy).await).contains("at most 3 suitcases"));
        let mut many = ride("airport", &[SLOT]);
        many.party = Some(4);
        assert!(bad(w.try_post(A, many).await).contains("1 to 3 people"));
        let mut quote = ride("airport", &[SLOT]);
        quote.quote = Some(5000.0);
        assert!(bad(w.try_post(A, quote).await).contains("looks far off"));
    }

    #[tokio::test]
    async fn cancelling_is_free_early_costs_late_and_is_refused_at_15_minutes() {
        let Some(mut w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        let b = w.post(B, "airport", &[SLOT]).await;
        // early: free, and the partner waits again with a notice
        w.with(move |cx| Box::pin(async move { cx.cancel(A, a).await }))
            .await
            .unwrap();
        assert_eq!(w.score(A).await, cred::START);
        let card = w.card(B, b).await;
        assert_eq!(card["kind"], "request");
        assert_eq!(card["notice"], "partner_left");
        // late: -10
        let c = w.post(C, "airport", &[SLOT]).await;
        assert_eq!(w.card(C, c).await["kind"], "trip");
        w.now = SLOT - HOUR;
        w.with(move |cx| Box::pin(async move { cx.cancel(C, c).await }))
            .await
            .unwrap();
        assert_eq!(w.score(C).await, cred::START - 10.0);
        // inside 15 minutes: refused
        let d = w.post(D, "airport", &[SLOT]).await;
        w.now = SLOT - 10 * 60;
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.cancel(D, d).await }))
            .await);
        assert!(why.contains("too close"), "{why}");
    }

    #[tokio::test]
    async fn a_more_reliable_rider_takes_the_seat_of_the_least_reliable() {
        let Some(w) = world().await else { return };
        w.with(|cx| {
            Box::pin(async move {
                for (e, s) in [(A, 90.0), (B, 80.0), (C, 30.0), (D, 95.0)] {
                    sqlx::query("UPDATE cab_users SET score=$1 WHERE email=$2")
                        .bind(s)
                        .bind(e)
                        .execute(&mut *cx.tx)
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        let a = w.post(A, "airport", &[SLOT]).await;
        let b = w.post(B, "airport", &[SLOT]).await;
        let c = w.post(C, "airport", &[SLOT]).await;
        let mut big = ride("airport", &[SLOT]);
        big.party = Some(2);
        let d = w.try_post(D, big).await.unwrap();
        assert_eq!(w.card(D, d).await["kind"], "trip");
        let c_card = w.card(C, c).await;
        assert_eq!(
            c_card["kind"], "request",
            "the least reliable rider waits again"
        );
        assert_eq!(c_card["notice"], "displaced");
        assert_eq!(w.card(A, a).await["people"], 4);
        assert_eq!(w.card(B, b).await["people"], 4);
    }

    #[tokio::test]
    async fn a_block_keeps_two_students_apart_both_ways() {
        let Some(w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        let b = w.post(B, "airport", &[SLOT]).await;
        assert_eq!(w.card(A, a).await["kind"], "trip");
        // blocking a co-rider moves the blocker out
        w.with(move |cx| Box::pin(async move { cx.block(A, B).await }))
            .await
            .unwrap();
        assert_eq!(w.card(A, a).await["kind"], "request");
        assert_eq!(w.card(B, b).await["kind"], "request");
        // and B never sees A's ride in the feed
        let feed = w.feed(B).await;
        assert!(feed["rides"].as_array().unwrap().is_empty(), "{feed}");
        let why = bad(w
            .with(|cx| Box::pin(async move { cx.block(A, "x@gmail.com").await }))
            .await);
        assert!(why.contains("IITGN"));
        let list = w
            .with(|cx| Box::pin(async move { cx.blocked(A).await }))
            .await
            .unwrap();
        assert_eq!(list, vec![B.to_string()]);
    }

    #[tokio::test]
    async fn the_feed_shows_each_ride_once_and_joining_seats_you() {
        let Some(w) = world().await else { return };
        w.post(A, "airport", &[SLOT, LATER]).await;
        let feed = w.feed(B).await;
        let rides = feed["rides"].as_array().unwrap();
        assert_eq!(
            rides.len(),
            1,
            "a cab request never shows as an auto ride: {feed}"
        );
        let cab = &rides[0];
        assert_eq!(
            cab["times"].as_array().unwrap().len(),
            2,
            "one rider with two times is one ride"
        );
        assert_eq!(cab["going"], 1);
        let passes: Vec<&str> = cab["passes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["place"].as_str().unwrap())
            .collect();
        assert_eq!(passes, vec!["dholakuva_metro", "rakshashakti"]);
        assert!(!feed.to_string().contains(A));
        // the poster never sees their own ride
        assert!(w.feed(A).await["rides"].as_array().unwrap().is_empty());
        // join at the later time, getting off on the way
        let mut join = ride("rakshashakti", &[]);
        join.departure = Some(json!(iso(LATER)));
        let card = w
            .with(move |cx| {
                Box::pin(async move {
                    let rid = cx.join_time(B, &join).await?;
                    cx.card(B, rid).await
                })
            })
            .await
            .unwrap();
        assert_eq!(card["kind"], "trip");
        assert_eq!(card["departure"], iso(LATER));
        // a time nobody holds is refused with a reason
        let mut stale = ride("airport", &[]);
        stale.departure = Some(json!(iso(SLOT + 60)));
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.join_time(C, &stale).await }))
            .await);
        assert!(why.contains("Nobody is going"), "{why}");
    }

    #[tokio::test]
    async fn feed_pages_never_skip_or_repeat() {
        let Some(w) = world().await else { return };
        for (i, e) in [A, B, C].into_iter().enumerate() {
            w.post(e, "airport", &[SLOT + i as i64 * HOUR]).await;
        }
        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let a = after.clone();
            let page = w
                .with(move |cx| {
                    Box::pin(async move { cx.feed(D, None, None, Some(2), a.as_deref()).await })
                })
                .await
                .unwrap();
            for r in page["rides"].as_array().unwrap() {
                seen.push(format!("{}{}", r["departure"], r["vehicle"]));
            }
            match page["next"].as_str() {
                Some(n) => after = Some(n.to_string()),
                None => break,
            }
        }
        let all = w.feed(D).await["rides"].as_array().unwrap().len();
        assert_eq!(seen.len(), all);
        assert_eq!(all, 3);
        let unique: std::collections::HashSet<_> = seen.iter().collect();
        assert_eq!(unique.len(), all);
    }

    #[tokio::test]
    async fn the_final_regroup_puts_a_lone_rider_into_a_ride() {
        let Some(mut w) = world().await else { return };
        // A and B share; C posts for a stop off their road, then D for C's road
        let a = w.post(A, "airport", &[SLOT]).await;
        w.post(B, "airport", &[SLOT]).await;
        let c = w.post(C, "rakshashakti", &[SLOT, LATER]).await;
        // C joins A's ride straight away: rakshashakti is on the way
        assert_eq!(w.card(C, c).await["kind"], "trip");
        w.now = SLOT - REVEAL_BEFORE + 1;
        w.tick().await;
        let card = w.card(A, a).await;
        assert_eq!(card["people"], 3);
        assert_eq!(card["revealed"], true);
    }

    #[tokio::test]
    async fn check_in_needs_the_window_and_the_radius() {
        let Some(mut w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        w.post(B, "airport", &[SLOT]).await;
        let (lat, lng) = places::coords(places::CAMPUS);
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.check_in(A, a, lat, lng).await }))
            .await);
        assert!(why.contains("opens 10 minutes"), "{why}");
        w.now = SLOT - 5 * 60;
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.check_in(A, a, lat + 0.01, lng).await }))
            .await);
        assert!(why.contains("from the pickup point"), "{why}");
        w.with(move |cx| Box::pin(async move { cx.check_in(A, a, lat + 0.0005, lng).await }))
            .await
            .unwrap();
        let card = w.card(A, a).await;
        assert_eq!(card["checkin"]["done"], true);
    }

    #[tokio::test]
    async fn a_no_show_loses_points_only_when_reported_and_not_checked_in() {
        let Some(mut w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        let b = w.post(B, "airport", &[SLOT]).await;
        let c = w.post(C, "airport", &[SLOT]).await;
        let (lat, lng) = places::coords(places::CAMPUS);
        w.now = SLOT;
        w.with(move |cx| Box::pin(async move { cx.check_in(A, a, lat, lng).await }))
            .await
            .unwrap();
        w.with(move |cx| Box::pin(async move { cx.confirm_ride(B, b).await }))
            .await
            .unwrap();
        w.now = SLOT + 60;
        let card = w.card(A, a).await;
        let handle_of = |label: &str| {
            card["riders"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["label"] == label)
                .unwrap()["contact"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // A's view: Rider 2 is B, Rider 3 is C (seat order)
        let (hb, hc) = (handle_of("Rider 2"), handle_of("Rider 3"));
        let hc2 = hc.clone();
        w.with(move |cx| Box::pin(async move { cx.report_no_show(A, a, &hc2).await }))
            .await
            .unwrap();
        // one report is not enough in a ride of three
        let ha = {
            let card = w.card(B, b).await;
            card["riders"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["label"] == "Rider 2")
                .unwrap()["contact"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.report_no_show(B, b, &ha).await }))
            .await);
        assert!(why.contains("checked in"), "{why}");
        let _ = hb;
        w.with(move |cx| Box::pin(async move { cx.report_no_show(B, b, &hc).await }))
            .await
            .unwrap();
        w.now = SLOT + 2 * HOUR;
        w.tick().await;
        assert_eq!(w.score(A).await, cred::START + cred::RODE_REWARD);
        assert_eq!(w.score(B).await, cred::START + cred::RODE_REWARD);
        assert_eq!(w.score(C).await, cred::START + cred::NO_SHOW_PENALTY);
        assert_eq!(w.card(C, c).await["settled"], true);
        // and it fades back over a month
        w.now += 30 * 24 * HOUR;
        let faded = w.score(C).await;
        assert!(
            faded > cred::START + cred::NO_SHOW_PENALTY && faded <= cred::START,
            "{faded}"
        );
    }

    #[tokio::test]
    async fn chat_is_for_the_ride_by_handle_and_notifications_collapse() {
        let Some(w) = world().await else { return };
        let a = w.post(A, "airport", &[SLOT]).await;
        let b = w.post(B, "airport", &[SLOT]).await;
        let c = w.post(C, "kudasan", &[SLOT]).await;
        for text in ["hi", "gate 2?", "ok"] {
            w.with(move |cx| Box::pin(async move { cx.send_message(A, a, text).await }))
                .await
                .unwrap();
        }
        let chat = w
            .with(move |cx| Box::pin(async move { cx.messages(B, b, None, None).await }))
            .await
            .unwrap();
        let msgs = chat["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["mine"], false);
        assert!(!chat.to_string().contains(A));
        // someone not in the ride reads nothing
        let theirs = w
            .with(move |cx| Box::pin(async move { cx.messages(C, c, None, None).await }))
            .await
            .unwrap();
        assert!(theirs["messages"].as_array().unwrap().is_empty());
        let why = bad(w
            .with(move |cx| Box::pin(async move { cx.send_message(C, c, "hey").await }))
            .await);
        assert!(why.contains("not in a group"));
        assert!(matches!(
            w.with(move |cx| Box::pin(async move { cx.messages(C, a, None, None).await }))
                .await,
            Err(Error::NotYours)
        ));
        let notes = w
            .with(|cx| Box::pin(async move { cx.notifications(B, None, None).await }))
            .await
            .unwrap();
        let messages: Vec<&Value> = notes["notifications"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|n| n["kind"] == "message")
            .collect();
        assert_eq!(messages.len(), 1, "{notes}");
        assert_eq!(messages[0]["times"], 3);
        // switched off, nothing more arrives
        let off: HashMap<String, bool> = HashMap::from([("messages".into(), false)]);
        w.with(move |cx| Box::pin(async move { cx.set_notify_prefs(C, &off).await }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_quote_to_the_final_stop_prices_the_whole_ride() {
        let Some(w) = world().await else { return };
        let mut q = ride("airport", &[SLOT]);
        let base = fares::total(23.9, Vehicle::Cab, 1.0);
        q.quote = Some((base * 2) as f64);
        let a = w.try_post(A, q).await.unwrap();
        w.post(B, "airport", &[SLOT]).await;
        let card = w.card(A, a).await;
        assert_eq!(card["fare"]["price_source"], "quote_final");
        assert_eq!(card["fare"]["total"], base * 2);
    }

    #[tokio::test]
    async fn my_trips_moves_rides_to_the_past_and_pages_them() {
        let Some(mut w) = world().await else { return };
        for i in 0..3 {
            w.post(A, "airport", &[SLOT + i * HOUR]).await;
            w.post(B, "airport", &[SLOT + i * HOUR]).await;
        }
        w.post(A, "kudasan", &[NOW + 20 * HOUR]).await;
        let mine = w.trips(A).await;
        assert_eq!(mine["trips"].as_array().unwrap().len(), 3);
        assert_eq!(mine["requests"].as_array().unwrap().len(), 1);
        w.now = SLOT + 3 * HOUR;
        let page = w
            .with(|cx| Box::pin(async move { cx.my_trips(A, Some(2), None).await }))
            .await
            .unwrap();
        assert_eq!(page["past"].as_array().unwrap().len(), 2);
        let next = page["past_next"].as_str().unwrap().to_string();
        let rest = w
            .with(move |cx| Box::pin(async move { cx.my_trips(A, Some(2), Some(&next)).await }))
            .await
            .unwrap();
        assert_eq!(rest["past"].as_array().unwrap().len(), 1);
        assert!(rest["past_next"].is_null());
    }
}
