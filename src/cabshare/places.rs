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
