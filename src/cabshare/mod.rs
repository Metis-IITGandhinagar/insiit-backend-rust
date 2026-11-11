//! Cabshare: students sharing cabs and autos from IITGN, ported from the
//! standalone app (github.com/GellaNagaSaiKrishna/cs437-9-hack).
//!
//! - `places`: the stops and the route tree rooted at campus.
//! - `matching`: who may share a vehicle, and the final regroup at 15 minutes.
//! - `fares`: rate cards and the leg-by-leg fare split (estimates; no payments).
//! - `credibility`: the reliability score that orders who rides with whom.
//! - `store`: everything stateful, one locked transaction per call.
//! - `routes`: the `/cabshare/...` endpoints.

pub mod credibility;
pub mod fares;
pub mod matching;
pub mod places;
pub mod routes;
pub mod schema;
pub mod store;

#[cfg(test)]
mod tests;
