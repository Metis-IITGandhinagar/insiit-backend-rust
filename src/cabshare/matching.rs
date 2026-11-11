//! Who may share one vehicle, kept free of storage and time.
//!
//! Requests share only when they go the same direction at a common time and
//! their stops lie on one route from campus (no detour). A group must fit in
//! one vehicle. Credibility orders candidates so reliable riders end up
//! together, and decides who keeps a seat; it never decides whether someone rides.

use std::collections::HashSet;

use super::places;

pub struct Capacity {
    pub seats: i64,
    pub suitcases: i64,
    pub handbags: i64,
}

pub const VEHICLES: [&str; 2] = ["cab", "auto"];

pub fn capacity(vehicle: &str) -> Capacity {
    match vehicle {
        "auto" => Capacity {
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
    pub id: i64,
    pub email: String,
    pub destination: String,
    pub direction: String,
    /// Every time offered; once grouped, only the ride's time.
    pub slots: Vec<i64>,
    pub handbags: i64,
    pub suitcases: i64,
    /// waiting | grouped | cancelled
    pub state: String,
    pub cab_id: Option<i64>,
    pub rode: bool,
    pub checked_in: Option<i64>,
    pub notice: Option<String>,
    pub created: i64,
    pub score: f64,
    /// Emails never to share a vehicle with, both ways.
    pub avoid: HashSet<String>,
    /// People on this request, poster included.
    pub party: i64,
    pub vehicle: String,
    pub quote: Option<i64>,
    pub place_note: Option<String>,
}

impl Req {
    pub fn is_waiting(&self) -> bool {
        self.state == "waiting"
    }
    pub fn is_grouped(&self) -> bool {
        self.state == "grouped"
    }
}

/// Seats these requests take: a rider with a friend takes two.
pub fn people(reqs: &[Req]) -> i64 {
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
    let mut kinds = reqs
        .iter()
        .map(|r| r.vehicle.as_str())
        .collect::<HashSet<_>>();
    if kinds.len() > 1 {
        return false;
    }
    let cap = capacity(kinds.drain().next().unwrap_or("cab"));
    people(reqs) <= cap.seats
        && reqs.iter().map(|r| r.suitcases).sum::<i64>() <= cap.suitcases
        && reqs.iter().map(|r| r.handbags).sum::<i64>() <= cap.handbags
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

    fn req(id: i64, dest: &str, score: f64) -> Req {
        Req {
            id,
            email: format!("s{id}@iitgn.ac.in"),
            destination: dest.into(),
            direction: "from_campus".into(),
            slots: vec![1000],
            state: "waiting".into(),
            score,
            party: 1,
            vehicle: "cab".into(),
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
        b.vehicle = "auto".into();
        assert!(!fits(&[a, b]));
    }

    #[test]
    fn regroup_seats_everyone_and_keeps_similar_scores_together() {
        let scores = [95.0, 90.0, 85.0, 80.0, 40.0, 35.0, 30.0];
        let reqs: Vec<Req> = scores
            .iter()
            .enumerate()
            .map(|(i, s)| req(i as i64 + 1, "airport", *s))
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
