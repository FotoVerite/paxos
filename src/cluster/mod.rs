//! Cluster-level orchestration.
//!
//! `runtime` stays generic. Protocol-specific orchestration lives under
//! `classic`, `pmmc`, and `vertical`.

pub mod classic;
pub mod pmmc;
pub mod runtime;
pub mod vertical;
