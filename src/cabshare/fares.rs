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

pub fn rate(vehicle: &str) -> Rate {
    match vehicle {
        "auto" => Rate {
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
pub fn raw(km: f64, vehicle: &str) -> f64 {
    let r = rate(vehicle);
    r.minimum + r.per_km * (km - r.included_km).max(0.0)
}

/// Whole-rupee fare for one vehicle over `km`, scaled by `factor`.
pub fn total(km: f64, vehicle: &str, factor: f64) -> i64 {
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
    vehicle: &str,
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
        assert_eq!(total(10.0, "cab", 1.0), 225);
        assert_eq!(total(1.0, "auto", 1.0), 25);
        assert_eq!(total(2.2, "auto", 1.0), 45);
    }

    #[test]
    fn split_adds_up_and_differs_by_at_most_one() {
        let s = split(101, 3);
        assert_eq!(s, vec![34, 34, 33]);
        assert_eq!(s.iter().sum::<i64>(), 101);
    }

    #[test]
    fn one_stop_is_an_even_split() {
        let (fare, shares, _) = by_legs(&[10.0], &[(0, 1), (0, 1), (0, 1)], "cab", 1.0);
        assert_eq!(fare, 225);
        assert_eq!(shares, split(225, 3));
    }

    #[test]
    fn the_rider_dropped_first_pays_less_and_shares_add_up() {
        // campus -> rakshashakti (7.8) -> airport (16.1)
        let (fare, shares, legs) = by_legs(&[7.8, 16.1], &[(0, 1), (1, 1)], "cab", 1.0);
        assert_eq!(fare, total(23.9, "cab", 1.0));
        assert_eq!(shares.iter().sum::<i64>(), fare);
        assert!(shares[0] < shares[1]);
        assert_eq!(legs[0].1, 2);
        assert_eq!(legs[1].1, 1);
    }

    #[test]
    fn a_party_pays_a_share_per_person() {
        let (fare, shares, _) = by_legs(&[10.0], &[(0, 2), (0, 1)], "cab", 1.0);
        assert_eq!(shares.iter().sum::<i64>(), fare);
        assert_eq!(shares[0], 150);
        assert_eq!(shares[1], 75);
    }

    #[test]
    fn a_factor_scales_the_fare() {
        assert_eq!(total(10.0, "cab", 2.0), 450);
    }
}
