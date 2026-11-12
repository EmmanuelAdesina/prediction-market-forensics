//! Canonical domain types for the Tokor engine.
//!
//! Everything flows through this crate: raw wallet activity, market metadata,
//! ingestion cursors, reconstructed positions. Normalization must never discard
//! information - raw JSON is always retained alongside normalized values.
//!
//! Money and prices use `rust_decimal::Decimal` everywhere in the domain layer.
//! Floating point is only permitted at the JSON-decoding boundary, where external
//! APIs emit numbers; even there we convert immediately and never round-trip.

pub mod activity;
pub mod activity_events;
pub mod classification;
pub mod dip;
pub mod error;
pub mod fees;
pub mod market;
pub mod position;

pub use activity::{
    event_identity, ActivityFilters, ActivityType, IngestionBatch, Outcome, OutcomeToken, PageMeta,
    Side, SourceFill, SourceFillRaw, WalletCursor,
};
pub use activity_events::{
    activity_event_from_raw, activity_event_identity, parse_activity_batch, ActivityBatch,
    ActivityEvent, ActivityEventRaw,
};
pub use classification::{ClassifierEvidence, CryptoVerdict};
pub use dip::{
    Asset, BarrierSemantics, DipMarket, DipNormalization, FeeInfo, MarketStatus, RejectionReason,
    ResolutionStatus, ResolutionVerification,
};
pub use error::IngestionError;
pub use market::MarketRef;
pub use position::{Lot, PositionBook, PositionState};

#[cfg(test)]
mod accounting_tests;
#[cfg(test)]
mod activity_event_tests;
#[cfg(test)]
mod fee_tests;
#[cfg(test)]
mod tests;
