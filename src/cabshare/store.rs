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
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::credibility as cred;
use super::fares;
use super::matching::{self, Req};
use super::places;

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
pub const DIRECTIONS: [&str; 2] = ["from_campus", "to_campus"];
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
pub fn pickup(destination: &str, direction: &str) -> (f64, f64) {
    if direction == "to_campus" {
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
    pub direction: String,
    #[serde(default)]
    pub slots: Vec<Value>,
    #[serde(default)]
    pub departure: Option<Value>,
    pub handbags: Option<i64>,
    pub suitcases: Option<i64>,
    #[serde(default)]
    pub party: Option<i64>,
    #[serde(default)]
    pub vehicle: Option<String>,
    #[serde(default)]
    pub quote: Option<f64>,
    #[serde(default)]
    pub place_note: Option<String>,
}

/// One vehicle: a direction, a departure, its riders.
#[derive(Clone)]
pub struct Trip {
    pub id: i64,
    pub destination: String,
    pub direction: String,
    pub departure: i64,
    pub settled: bool,
    pub members: Vec<Req>,
}

impl Trip {
    pub fn vehicle(&self) -> Option<&str> {
        self.members.first().map(|m| m.vehicle.as_str())
    }
    pub fn riders(&self) -> i64 {
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

fn shape(input: &RideIn) -> R<(i64, String, i64, i64)> {
    let vehicle = input.vehicle.clone().unwrap_or_else(|| "cab".into());
    if !matching::VEHICLES.contains(&vehicle.as_str()) {
        return bad(format!("unknown vehicle: {vehicle}"));
    }
    let cap = matching::capacity(&vehicle);
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

fn place_check(destination: &str, direction: &str) -> R<()> {
    if !places::is_stop(destination) {
        return bad(format!("unknown destination: {destination}"));
    }
    if !DIRECTIONS.contains(&direction) {
        return bad(format!("unknown direction: {direction}"));
    }
    Ok(())
}

fn with(group: &[Req], one: &Req) -> Vec<Req> {
    let mut out = group.to_vec();
    out.push(one.clone());
    out
}

fn stops_json(reqs: &[Req], direction: &str, mine: Option<&str>) -> Value {
    let mut order = places::order_for_drop(reqs.iter().map(|r| r.destination.as_str()));
    if direction == "to_campus" {
        order.reverse();
    }
    Value::Array(
        order
            .iter()
            .map(|p| {
                json!({"place": p, "name": places::name(p), "you": Some(p.as_str()) == mine,
                       "people": reqs.iter().filter(|r| &r.destination == p).map(|r| r.party).sum::<i64>()})
            })
            .collect(),
    )
}

/// Places on the road a ride drives that no rider has picked yet.
fn passes(reqs: &[Req], way: &str) -> Vec<String> {
    let taken: HashSet<&str> = reqs.iter().map(|r| r.destination.as_str()).collect();
    let fin = places::farthest(taken.iter().copied());
    let path = places::path_from_campus(&fin);
    let mut road: Vec<String> = path[1..path.len().saturating_sub(1)]
        .iter()
        .filter(|p| !taken.contains(p.as_str()))
        .cloned()
        .collect();
    if way == "to_campus" {
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
    async fn id(&mut self, q: Q<'_>) -> R<i64> {
        Ok(q.fetch_one(&mut *self.tx).await?.get::<i64, _>(0))
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

    async fn row(&mut self, rid: i64) -> R<Req> {
        let row = self
            .one(sqlx::query("SELECT * FROM cab_requests WHERE id=$1").bind(rid))
            .await?
            .ok_or_else(|| Error::NotFound("no such request".into()))?;
        Ok(req_from_row(&row))
    }

    async fn own(&mut self, email: &str, rid: i64) -> R<Req> {
        let r = self.row(rid).await?;
        if r.email != email {
            return Err(Error::NotYours);
        }
        Ok(r)
    }

    async fn offered(&mut self, rid: i64) -> R<Vec<i64>> {
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
        let cab_ids: Vec<i64> = rows
            .iter()
            .filter(|r| r.is_grouped())
            .filter_map(|r| r.cab_id)
            .collect();
        let departs: HashMap<i64, i64> = self
            .all(
                sqlx::query("SELECT id, departure FROM cab_rides WHERE id = ANY($1)")
                    .bind(&cab_ids),
            )
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("departure")))
            .collect();
        let open_ids: Vec<i64> = rows
            .iter()
            .filter(|r| !r.is_grouped())
            .map(|r| r.id)
            .collect();
        let mut offered: HashMap<i64, Vec<i64>> = HashMap::new();
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

    pub async fn request(&mut self, rid: i64) -> R<Req> {
        let r = self.row(rid).await?;
        Ok(self.load(vec![r], true).await?.remove(0))
    }

    async fn members(&mut self, cab_id: i64, scored: bool) -> R<Vec<Req>> {
        self.reqs(
            sqlx::query(
                "SELECT * FROM cab_requests WHERE cab_id=$1 AND state='grouped' ORDER BY id",
            )
            .bind(cab_id),
            scored,
        )
        .await
    }

    async fn cab(&mut self, cab_id: i64) -> R<Option<(String, String, i64, bool)>> {
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

    pub async fn trip(&mut self, cab_id: i64) -> R<Trip> {
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
    async fn waiting(&mut self, direction: &str, at: Option<i64>, scored: bool) -> R<Vec<Req>> {
        let q = match at {
            Some(at) => sqlx::query(
                "SELECT * FROM cab_requests WHERE state='waiting' AND direction=$1 AND id IN
                 (SELECT request_id FROM cab_request_slots WHERE departure=$2) ORDER BY id",
            )
            .bind(direction.to_string())
            .bind(at),
            None => sqlx::query(
                "SELECT * FROM cab_requests WHERE state='waiting' AND direction=$1 AND id IN
                 (SELECT request_id FROM cab_request_slots WHERE departure>$2) ORDER BY id",
            )
            .bind(direction.to_string())
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
    pub async fn post_request(&mut self, email: &str, input: &RideIn) -> R<i64> {
        self.regroup_due().await?;
        place_check(&input.destination, &input.direction)?;
        let (party, vehicle, handbags, suitcases) = shape(input)?;
        let note = clean_note(input.place_note.as_deref())?;
        let slots = self.slots(input)?;
        let quote = self.check_quote(input.quote, &input.destination, &vehicle)?;
        self.refuse_busy(email, &slots).await?;
        self.insert_and_place(
            email, input, &slots, party, &vehicle, handbags, suitcases, quote, note,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_and_place(
        &mut self,
        email: &str,
        input: &RideIn,
        slots: &[i64],
        party: i64,
        vehicle: &str,
        handbags: i64,
        suitcases: i64,
        quote: Option<i64>,
        note: Option<String>,
    ) -> R<i64> {
        let rid = self
            .id(sqlx::query(
                "INSERT INTO cab_requests(email, destination, direction, handbags, suitcases, party, vehicle,
                 quote, place_note, created) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id",
            )
            .bind(email)
            .bind(&input.destination)
            .bind(&input.direction)
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
    pub async fn join_time(&mut self, email: &str, input: &RideIn) -> R<i64> {
        self.regroup_due().await?;
        place_check(&input.destination, &input.direction)?;
        let Some(departure) = input.departure.as_ref().and_then(parse_time) else {
            return bad("that is not a time");
        };
        let (party, vehicle, handbags, suitcases) = shape(input)?;
        let note = clean_note(input.place_note.as_deref())?;
        let open = self
            .open_slots(
                email,
                &input.destination,
                &input.direction,
                handbags,
                suitcases,
                party,
                &vehicle,
            )
            .await?;
        if !open.iter().any(|o| o.0 == departure) {
            let why = self
                .why_not_open(
                    email,
                    &input.destination,
                    &input.direction,
                    departure,
                    handbags,
                    suitcases,
                    party,
                    &vehicle,
                )
                .await?;
            return bad(why);
        }
        self.refuse_busy(email, &[departure]).await?;
        let quote = self.check_quote(input.quote, &input.destination, &vehicle)?;
        self.insert_and_place(
            email,
            input,
            &[departure],
            party,
            &vehicle,
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
        direction: &str,
        departure: i64,
        handbags: i64,
        suitcases: i64,
        party: i64,
        vehicle: &str,
    ) -> R<String> {
        if self.busy_times(email).await?.contains(&departure) {
            return Ok("You already have a trip at that time. Open it in My trips, or cancel it to join this one.".into());
        }
        if departure - self.now <= REVEAL_BEFORE {
            return Ok("That ride leaves in under 15 minutes, so its group is fixed.".into());
        }
        let avoid = self.avoid(email).await?;
        let ids: Vec<i64> = self
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
            direction: direction.into(),
            handbags,
            suitcases,
            party,
            vehicle: vehicle.into(),
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

    async fn seat(&mut self, rid: i64, cab_id: i64, announce: bool) -> R<()> {
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

    async fn member_rows(&mut self, cab_id: i64) -> R<Vec<(i64, String)>> {
        Ok(self
            .all(sqlx::query("SELECT id, email FROM cab_requests WHERE cab_id=$1 AND state='grouped' ORDER BY id").bind(cab_id))
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("email")))
            .collect())
    }

    /// Back to waiting, offering again only times not since taken elsewhere.
    async fn unseat(&mut self, rid: i64) -> R<()> {
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
            if r.get::<String, _>("state") == "grouped" {
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
    async fn reroute(&mut self, cab_id: i64) -> R<()> {
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
    async fn tell(&mut self, rid: i64, why: &str) -> R<()> {
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

    pub async fn dismiss_notice(&mut self, email: &str, rid: i64) -> R<()> {
        self.own(email, rid).await?;
        self.exec(sqlx::query("UPDATE cab_requests SET notice=NULL WHERE id=$1").bind(rid))
            .await?;
        Ok(())
    }

    /// Put a waiting request into a ride if the rules allow. For each time,
    /// earliest first: join a ride with room; else take the seat of a less
    /// reliable member, who is placed again; else start a ride with the most
    /// reliable waiting requests that fit.
    pub fn place(&mut self, rid: i64) -> Pin<Box<dyn Future<Output = R<bool>> + Send + '_>> {
        Box::pin(async move {
            let me = self.request(rid).await?;
            if !me.is_waiting() {
                return Ok(false);
            }
            for slot in me.slots.clone() {
                if self.fixed(slot) {
                    continue;
                }
                let ids: Vec<i64> = self
                    .all(
                        sqlx::query("SELECT id FROM cab_rides WHERE direction=$1 AND departure=$2 ORDER BY id")
                            .bind(&me.direction)
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
                    matching::by_credibility(self.waiting(&me.direction, Some(slot), true).await?)
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
                            .bind(&me.direction)
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
    async fn refill(&mut self, cab_id: i64) -> R<()> {
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
            self.waiting(&trip.direction.clone(), Some(trip.departure), true)
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
    async fn candidates(&mut self, direction: &str) -> R<(Vec<(i64, Vec<Req>)>, Vec<Req>)> {
        let waiting = self.waiting(direction, None, false).await?;
        let cabs: Vec<(i64, i64)> = self
            .all(
                sqlx::query("SELECT id, departure FROM cab_rides WHERE departure>$1 AND direction=$2 ORDER BY departure, id")
                    .bind(self.now)
                    .bind(direction),
            )
            .await?
            .iter()
            .map(|r| (r.get("id"), r.get("departure")))
            .collect();
        let ids: Vec<i64> = cabs.iter().map(|c| c.0).collect();
        let mut members: HashMap<i64, Vec<Req>> = HashMap::new();
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
        direction: &str,
        handbags: i64,
        suitcases: i64,
        party: i64,
        vehicle: &str,
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
                direction: direction.into(),
                handbags,
                suitcases,
                party,
                vehicle: vehicle.into(),
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
        direction: &str,
        handbags: i64,
        suitcases: i64,
        party: i64,
        vehicle: &str,
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
        direction: &str,
        handbags: i64,
        suitcases: i64,
        party: i64,
        vehicle: &str,
    ) -> R<Value> {
        self.regroup_due().await?;
        place_check(destination, direction)?;
        let input = RideIn {
            destination: destination.into(),
            direction: direction.into(),
            handbags: Some(handbags),
            suitcases: Some(suitcases),
            party: Some(party),
            vehicle: Some(vehicle.into()),
            ..Default::default()
        };
        let (party, vehicle, handbags, suitcases) = shape(&input)?;
        let cap = matching::capacity(&vehicle);
        let mut out = Vec::new();
        for (d, members) in self
            .open_slots(
                email,
                destination,
                direction,
                handbags,
                suitcases,
                party,
                &vehicle,
            )
            .await?
        {
            let me = Req {
                destination: destination.into(),
                direction: direction.into(),
                handbags,
                suitcases,
                party,
                vehicle: vehicle.clone(),
                ..Default::default()
            };
            let all = with(&members, &me);
            out.push(json!({
                "departure": iso(d), "riders": matching::people(&members), "vehicle": vehicle,
                "seats_left": cap.seats - matching::people(&members) - party,
                "suitcases_left": cap.suitcases - members.iter().map(|m| m.suitcases).sum::<i64>() - suitcases,
                "handbags_left": cap.handbags - members.iter().map(|m| m.handbags).sum::<i64>() - handbags,
                "stops": stops_json(&all, direction, Some(destination)),
                "fare": self.trip_fare(&members, &me, direction).await?,
            }));
        }
        Ok(Value::Array(out))
    }

    async fn ride(
        &mut self,
        way: &str,
        vehicle: &str,
        departure: i64,
        stop: &str,
        members: &[Req],
    ) -> R<Value> {
        let going = matching::people(members);
        let cap = matching::capacity(vehicle);
        let me = Req {
            destination: stop.into(),
            direction: way.into(),
            vehicle: vehicle.into(),
            party: 1,
            ..Default::default()
        };
        let all = with(members, &me);
        let fin = places::farthest(all.iter().map(|m| m.destination.as_str()));
        Ok(json!({
            "destination": stop, "destination_name": places::name(stop),
            "direction": way, "vehicle": vehicle, "departure": iso(departure),
            "going": going, "seats": cap.seats, "seats_free": cap.seats - going,
            "suitcases_free": cap.suitcases - members.iter().map(|m| m.suitcases).sum::<i64>(),
            "handbags_free": cap.handbags - members.iter().map(|m| m.handbags).sum::<i64>(),
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
        direction: Option<&str>,
        to: Option<&str>,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> R<Value> {
        self.regroup_due().await?;
        if let Some(d) = direction
            && !DIRECTIONS.contains(&d)
        {
            return bad(format!("unknown direction: {d}"));
        }
        if let Some(t) = to
            && !places::is_stop(t)
        {
            return bad(format!("unknown destination: {t}"));
        }
        let limit = limit.unwrap_or(FEED_PAGE).clamp(1, FEED_PAGE_MAX);
        let busy = self.busy_times(email).await?;
        let mut rides: Vec<(i64, String, usize, usize, String, Value)> = Vec::new();
        let ways: Vec<&str> = match direction {
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

    pub async fn send_message(&mut self, email: &str, rid: i64, body: &str) -> R<()> {
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
        rid: i64,
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
                .map(|r| format!("{}~{}", r.get::<i64, _>("created"), r.get::<i64, _>("id")))
        } else {
            None
        };
        Ok(json!({
            "messages": rows.iter().map(|m| {
                let sender: String = m.get("sender");
                json!({"id": m.get::<i64, _>("id"), "from": sender, "mine": sender == mine,
                       "body": m.get::<String, _>("body"), "at": iso(m.get("created"))})
            }).collect::<Vec<_>>(),
            "older": older,
        }))
    }

    // --- after the ride -------------------------------------------------------------

    async fn departed(&mut self, cab_id: i64) -> R<bool> {
        Ok(self.cab(cab_id).await?.is_some_and(|c| self.now >= c.2))
    }

    pub async fn confirm_ride(&mut self, email: &str, rid: i64) -> R<()> {
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
    pub async fn check_in(&mut self, email: &str, rid: i64, lat: f64, lng: f64) -> R<()> {
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
        let away = places::straight_m(pickup(&row.destination, &row.direction), (lat, lng));
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
    pub async fn report_no_show(&mut self, email: &str, rid: i64, handle: &str) -> R<()> {
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
    pub async fn settle(&mut self, cab_id: i64) -> R<bool> {
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
        let ids: Vec<i64> = self
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
        let due: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        let mut done = 0;
        for (direction, departure) in due {
            done += usize::from(self.regroup(&direction, departure).await?);
        }
        Ok(done)
    }

    async fn regroup(&mut self, direction: &str, departure: i64) -> R<bool> {
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
        let cab_ids: Vec<i64> = self
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
        let before: HashMap<i64, i64> = seated.iter().map(|m| (m.id, m.cab_id.unwrap())).collect();
        let (mut groups, _) = matching::regroup(riders.clone());
        groups.sort_by_key(|g| std::cmp::Reverse(g.len()));
        let mut free = cab_ids.clone();
        let mut placed: HashMap<i64, i64> = HashMap::new();
        for g in &groups {
            let ids: Vec<i64> = g.iter().map(|r| r.id).collect();
            let overlap = |c: i64| ids.iter().filter(|i| before.get(i) == Some(&c)).count();
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
                    let mates: HashSet<i64> = placed
                        .iter()
                        .filter(|(_, c)| **c == cab)
                        .map(|(i, _)| *i)
                        .collect();
                    let mates_before: HashSet<i64> = before
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
    pub async fn cancel(&mut self, email: &str, rid: i64) -> R<()> {
        let row = self.own(email, rid).await?;
        match row.state.as_str() {
            "cancelled" => return bad("already cancelled"),
            "waiting" => {
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
    fn check_quote(&self, quote: Option<f64>, destination: &str, vehicle: &str) -> R<Option<i64>> {
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

    pub async fn set_quote(&mut self, email: &str, rid: i64, quote: Option<f64>) -> R<Option<i64>> {
        let row = self.own(email, rid).await?;
        if row.state == "cancelled" {
            return bad("that trip is cancelled");
        }
        let q = self.check_quote(quote, &row.destination, &row.vehicle)?;
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
        vehicle: &str,
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
        let ids: Vec<i64> = reqs.iter().map(|r| r.id).filter(|i| *i > 0).collect();
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
    async fn trip_fare(&mut self, members: &[Req], mine: &Req, direction: &str) -> R<Value> {
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
        let vehicle = if mine.vehicle.is_empty() {
            "cab"
        } else {
            mine.vehicle.as_str()
        };
        let (factor, price, n) = self
            .price_factor(&riders, vehicle, Some(kms.iter().sum()))
            .await?;
        let seats: Vec<(usize, i64)> = last
            .iter()
            .zip(&riders)
            .map(|(l, r)| (*l, r.party.max(1)))
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
                if direction == "to_campus" {
                    std::mem::swap(&mut a, &mut b);
                }
                let part = if j <= mine_last { (cost * riders[me].party.max(1) as f64 / *aboard as f64).round() } else { 0.0 };
                json!({"from": a, "from_name": places::name(&a), "to": b, "to_name": places::name(&b),
                       "km": km, "cost": cost.round(), "people": aboard, "you": j <= mine_last, "your_part": part})
            })
            .collect();
        if direction == "to_campus" {
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
            let (rid, cab): (i64, i64) = (r.get(0), r.get(1));
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

    async fn where_(&mut self, cab_id: i64) -> R<String> {
        Ok(self
            .cab(cab_id)
            .await?
            .map(|c| places::name(&c.0).to_string())
            .unwrap_or_default())
    }

    async fn request_card(&mut self, req: &Req) -> R<Value> {
        let km = places::km_from_campus(&req.destination);
        let (factor, price, n) = self
            .price_factor(std::slice::from_ref(req), &req.vehicle, None)
            .await?;
        let total = fares::total(km, &req.vehicle, factor);
        let r = fares::rate(&req.vehicle);
        let seats = matching::capacity(&req.vehicle).seats as usize;
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
        let point = pickup(&mine.destination, &trip.direction);
        let me = trip
            .members
            .iter()
            .find(|m| m.id == mine.id)
            .cloned()
            .unwrap_or_else(|| mine.clone());
        Ok(json!({
            "kind": "trip", "trip": trip.id, "request": mine.id,
            "fare": self.trip_fare(&trip.members, &me, &trip.direction).await?,
            "stops": stops_json(&trip.members, &trip.direction, Some(&mine.destination)),
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
            "seats_left": matching::capacity(trip.vehicle().unwrap_or("cab")).seats - trip.riders(),
            "handbags": trip.members.iter().map(|m| m.handbags).sum::<i64>(),
            "suitcases": trip.members.iter().map(|m| m.suitcases).sum::<i64>(),
            "fixed": revealed, "revealed": revealed, "departed": departed,
            "rode": mine.rode, "settled": trip.settled, "notice": mine.notice,
            "can_cancel": !revealed, "late_cancel": !revealed && trip.departure - self.now <= FREE_CANCEL_BEFORE,
            "quote": mine.quote, "place_note": mine.place_note,
        }))
    }

    /// One of my requests, as a trip card once grouped or a request card.
    pub async fn card(&mut self, email: &str, rid: i64) -> R<Value> {
        self.tick().await?;
        self.own(email, rid).await?;
        let req = self.request(rid).await?;
        match req.state.as_str() {
            "grouped" => self.trip_card(email, &req).await,
            "waiting" => self.request_card(&req).await,
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
                it.next().and_then(|x| x.parse::<i64>().ok()),
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
        trip: Option<i64>,
        request: Option<i64>,
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
                let n = o.get::<i64, _>("times") + 1;
                let where_ = self.where_(t).await?;
                self.exec(
                    sqlx::query("UPDATE cab_notifications SET title=$1, body=$2, created=$3, times=$4 WHERE id=$5")
                        .bind(format!("{n} new messages · {where_} ride"))
                        .bind(&body)
                        .bind(self.now)
                        .bind(n)
                        .bind(o.get::<i64, _>("id")),
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
        cab: i64,
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
        let cabs: Vec<(i64, i64)> = self
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
            let (rid, email, dest): (i64, String, String) = (r.get(0), r.get(1), r.get(2));
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
                json!({"id": r.get::<i64, _>("id"), "category": category(&kind), "kind": kind,
                       "title": r.get::<String, _>("title"), "body": r.get::<String, _>("body"),
                       "trip": r.get::<Option<i64>, _>("trip"), "request": r.get::<Option<i64>, _>("request"),
                       "at": iso(r.get("created")), "read": r.get::<bool, _>("read"), "times": r.get::<i64, _>("times")})
            }).collect::<Vec<_>>(),
            "unread": unread,
            "older": if more { rows.last().map(|r| r.get::<i64, _>("id")) } else { None },
        }))
    }

    pub async fn read_notifications(&mut self, email: &str, ids: Option<Vec<i64>>) -> R<i64> {
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
            let c = matching::capacity(v);
            let r = fares::rate(v);
            json!({"id": v, "seats": c.seats, "suitcases": c.suitcases, "handbags": c.handbags,
                   "base": r.minimum, "included_km": r.included_km, "per_km": r.per_km})
        }).collect::<Vec<_>>(),
        "limits": {"max_slots": MAX_SLOTS, "max_days_ahead": MAX_AHEAD / 86_400, "reveal_minutes": REVEAL_BEFORE / 60,
                   "free_cancel_hours": FREE_CANCEL_BEFORE / 3600, "message_max": MESSAGE_MAX, "note_max": NOTE_MAX},
    })
}
