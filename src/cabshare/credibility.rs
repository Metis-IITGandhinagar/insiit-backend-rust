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
