//! `tokor-application` library surface: CLI command implementations,
//! configuration, and the dip-market registry sync engine.
//!
//! The binary target (`tokor`, src/main.rs) is a thin wrapper around this
//! library so integration tests (tests/) can exercise the same code paths
//! the operator CLI runs.

pub mod commands;
pub mod config;
pub mod markets;
pub mod registry;
