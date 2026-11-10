//! Phase 2 §5 shadow execution.
//!
//! Answers the replication question: can a follower with <$50, acting only
//! on public data, economically replicate Tokor's observable fills?
//!
//! Cardinal rule: **submitted ≠ executed.** Every simulator separates the
//! quantity submitted from the quantity evidenced as filled; fills are
//! credited only under the rules of [`fill_credit`].
//!
//! Module map:
//! * [`queue`] — queue-ahead estimation under three scenarios
//!   (Optimistic / Baseline / Conservative).
//! * [`decision`] — the shared decision vocabulary.
//! * [`copy_sim`] — exact-copy and intelligent-copy policies.
//! * [`fill_credit`] — what counts as an executed simulated fill.
//! * [`portfolio`] — the $49 portfolio simulator.
//! * [`markout`] — signal-decay measurement from the public tape.
//!
//! Every function here is a pure function over recorded data: same inputs →
//! same outputs. No authentication, no order routes, no keys (§12).

pub mod copy_sim;
pub mod decision;
pub mod fill_credit;
pub mod markout;
pub mod portfolio;
pub mod queue;
