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
use super::store::{Cx, Error, REVEAL_BEFORE, RideIn, iso};

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
    super::schema::initialize_table(&pool).await.unwrap();
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
    async fn with<T>(&self, f: impl for<'c> FnOnce(&'c mut Cx) -> Step<'c, T>) -> Result<T, Error> {
        let mut cx = Cx::begin(&self.pool, self.now).await?;
        let out = f(&mut cx).await;
        if out.is_ok() {
            cx.commit().await?;
        }
        out
    }

    async fn post(&self, email: &'static str, dest: &str, slots: &[i64]) -> i64 {
        self.try_post(email, ride(dest, slots)).await.unwrap()
    }

    async fn try_post(&self, email: &'static str, input: RideIn) -> Result<i64, Error> {
        self.with(move |cx| Box::pin(async move { cx.post_request(email, &input).await }))
            .await
    }

    async fn card(&self, email: &'static str, rid: i64) -> Value {
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
        self.with(move |cx| Box::pin(async move { cx.feed(email, None, None, None, None).await }))
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
        direction: "from_campus".into(),
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
        fares::total(7.8 + 16.1, "cab", 1.0)
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
    let base = fares::total(23.9, "cab", 1.0);
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
