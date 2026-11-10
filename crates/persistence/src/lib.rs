//! Durable persistence: SQLite (WAL) with explicit migrations.
//!
//! Invariants implemented here:
//! - Migrations run transactionally, versioned, idempotent.
//! - Cursor advancement happens **in the same transaction** as the page's
//!   fills — a crash can never advance the cursor without the data, and can
//!   never commit fills without the cursor (all-or-nothing per page).
//! - `PRAGMA foreign_keys=ON`, `journal_mode=WAL`, `synchronous=FULL`.
//! - Raw API pages are immutable forensic records: insert-only.
//! - Fill identity deduplication lives in the database (`PRIMARY KEY event_key`)
//!   and is enforced with `INSERT OR IGNORE`; the return value reports how
//!   many rows were actually new.

pub mod activity;
pub mod ingest;
pub mod market_data;
pub mod markets;
pub mod recon;
pub mod report;

pub use activity::{ActivityCheckpoint, ActivityRepository, FeeRow, PositionSnapshotRow};
pub use ingest::{IngestRepository, PageCheckpoint};
pub use report::{PositionCounts, ReportRepository, RunSummary};

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use rusqlite::Connection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PersistError {
    #[error("sqlite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("migration failure: {0}")]
    Migration(String),

    #[error("storage misuse: {0}")]
    Misuse(String),
}

pub type Result<T> = std::result::Result<T, PersistError>;

// Embedded migrations keep ordering authority in one place. New migrations
// are appended; never reordered or edited after release.
pub const MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_wallet_activity",
        include_str!("../../../migrations/0001_wallet_activity.sql"),
    ),
    (
        "0002_markets_positions",
        include_str!("../../../migrations/0002_markets_positions.sql"),
    ),
    (
        "0003_activity_fees_positions",
        include_str!("../../../migrations/0003_activity_fees_positions.sql"),
    ),
    (
        "0004_market_data",
        include_str!("../../../migrations/0004_market_data.sql"),
    ),
];

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    /// Open (creating or reusing) a WAL-backed database and run migrations.
    pub fn open(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Self::configure(conn)
    }

    /// In-memory database for tests.
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(conn)
    }

    fn configure(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_millis(5000))?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.run_migrations()?;
        Ok(db)
    }

    /// Run pending migrations, one transaction per version.
    pub fn run_migrations(&self) -> Result<()> {
        {
            let conn = self.conn();
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )",
                [],
            )?;
        }
        let mut conn = self.conn();
        for (name, sql) in MIGRATIONS {
            let already = conn
                .query_row(
                    "SELECT 1 FROM schema_migrations WHERE version = ?1",
                    [name],
                    |_| Ok(()),
                )
                .is_ok();
            if already {
                continue;
            }
            let tx = conn.transaction()?;
            tx.execute_batch(sql)
                .map_err(|e| PersistError::Migration(format!("{name}: {e}")))?;
            tx.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                rusqlite::params![name, Utc::now().to_rfc3339()],
            )?;
            tx.commit()?;
            tracing::info!(migration = %name, "applied migration");
        }
        Ok(())
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("connection poisoned")
    }

    /// Crate-internal connection guard for repository modules.
    pub(crate) fn inner_w(&self) -> MutexGuard<'_, Connection> {
        self.conn()
    }

    /// Sanity check used after restart: WAL + FK pragmas still set.
    pub fn health_check(&self) -> Result<bool> {
        let conn = self.conn();
        let wal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        let fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        Ok(wal.eq_ignore_ascii_case("wal") && fk == 1)
    }

    /// Count of source fills recorded (deduplicated).
    pub fn fills_count(&self) -> Result<u64> {
        let conn = self.conn();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM source_fills", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Count of persisted raw forensic pages.
    pub fn ingest_raw_page_count(&self) -> Result<u64> {
        let conn = self.conn();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM raw_api_pages", [], |r| r.get(0))?;
        Ok(n as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_from_clean_db_create_all_tables() {
        let db = Database::open_memory().unwrap();
        let conn = db.conn();
        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN
                ('raw_api_pages','source_fills','acquisition_runs','wallet_cursor_state',
                 'markets','market_tokens','positions_reconstructed','position_lots_remaining',
                 'position_reconciliations','schema_migrations',
                 'activity_events','fee_reconciliation','positions_snapshot',
                 'market_subscriptions','raw_market_messages','order_book_snapshots',
                 'order_book_deltas','normalized_book_states','public_trade_events',
                 'market_data_health','source_observations','execution_simulations')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 22);
        let versions: Vec<String> = conn
            .prepare("SELECT version FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<String>, _>>()
            .unwrap();
        assert_eq!(
            versions,
            vec![
                "0001_wallet_activity",
                "0002_markets_positions",
                "0003_activity_fees_positions",
                "0004_market_data",
            ]
        );
    }

    #[test]
    fn migrations_idempotent_and_parseable() {
        let db = Database::open_memory().unwrap();
        db.run_migrations().unwrap();
        db.run_migrations().unwrap();
        for (name, sql) in MIGRATIONS {
            let scratch = Connection::open_in_memory().unwrap();
            assert!(
                scratch.execute_batch(sql).is_ok(),
                "migration {name} not applicable"
            );
        }
    }

    #[test]
    fn file_db_wal_health_and_recovery() {
        let dir = std::env::temp_dir().join("tokor-mig-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        let _ = std::fs::remove_file(&path);
        let db = Database::open(&path).unwrap();
        assert!(db.health_check().unwrap());
        drop(db);
        // Reopen: migrations are idempotent across restarts.
        let db2 = Database::open(&path).unwrap();
        assert!(db2.health_check().unwrap());
        db2.run_migrations().unwrap();
        drop(db2);
        let _ = std::fs::remove_file(&path);
    }
}
