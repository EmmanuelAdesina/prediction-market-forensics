//! Operator configuration (TOML).
//!
//! Credentials are never stored here. Phase 1 touches only public, keyless
//! endpoints; any future authenticated phase must introduce a separate,
//! secret-managed configuration mechanism.

use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub wallet_address: String,
    pub data_api_base: String,
    pub clob_base: String,
    pub gamma_base: String,
    pub db_path: PathBuf,
    pub page_limit: u64,
    pub page_pause_ms: u64,
    pub positions_page_limit: u64,
    pub max_retries: u32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            wallet_address: String::new(),
            data_api_base: "https://data-api.polymarket.com/v2".into(),
            clob_base: "https://clob.polymarket.com".into(),
            gamma_base: "https://gamma-api.polymarket.com".into(),
            db_path: PathBuf::from("data/tokor.db"),
            page_limit: 500,
            page_pause_ms: 1200,
            positions_page_limit: 500,
            max_retries: 5,
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path)
            .map_err(|e| format!("cannot read config {}: {e}", path.display()))?;
        let value: toml::Value = toml::from_str(&text)
            .map_err(|e| format!("invalid TOML in {}: {e}", path.display()))?;
        let get = |section: &str, key: &str| -> Option<String> {
            value
                .get(section)
                .and_then(|s| s.get(key))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        };

        let mut cfg = AppConfig::default();
        if let Some(w) = get("wallet", "address") {
            cfg.wallet_address = w;
        }
        if let Some(v) = get("data_api", "base_url_v2") {
            cfg.data_api_base = v;
        }
        if let Some(v) = get("clob", "base_url") {
            cfg.clob_base = v;
        }
        if let Some(v) = get("gamma", "base_url") {
            cfg.gamma_base = v;
        }
        if let Some(v) = get("storage", "db_path") {
            cfg.db_path = PathBuf::from(v);
        }
        let get_u64 = |section: &str, key: &str| -> Option<u64> {
            value
                .get(section)
                .and_then(|s| s.get(key))
                .and_then(|v| v.as_integer())
                .map(|v| v.max(0) as u64)
        };
        if let Some(v) = get_u64("data_api", "page_limit") {
            cfg.page_limit = v;
        }
        if let Some(v) = get_u64("data_api", "page_pause_ms") {
            cfg.page_pause_ms = v;
        }
        if let Some(v) = get_u64("data_api", "positions_page_limit") {
            cfg.positions_page_limit = v;
        }
        if let Some(v) = get_u64("data_api", "max_retries") {
            cfg.max_retries = v.clamp(0, 20) as u32;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Hard validation with actionable errors. Never silently accept a broken
    /// configuration.
    pub fn validate(&self) -> Result<(), String> {
        if !tokor_domain::activity::is_valid_address(&self.wallet_address) {
            return Err(format!(
                "wallet.address {:?} is not a valid 0x-prefixed 20-byte address",
                self.wallet_address
            ));
        }
        for (name, url) in [
            ("data_api.base_url_v2", &self.data_api_base),
            ("clob.base_url", &self.clob_base),
            ("gamma.base_url", &self.gamma_base),
        ] {
            if url.is_empty() {
                return Err(format!("{name} is empty"));
            }
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(format!("{name} must start with http(s):// — got {url:?}"));
            }
        }
        if self.page_limit < 1 || self.page_limit > 1000 {
            // Data API hard-caps the activity limit; keep an operator-visible bound.
            return Err(format!(
                "data_api.page_limit {} outside 1..=1000",
                self.page_limit
            ));
        }
        if self.page_pause_ms < 100 {
            return Err(format!(
                "data_api.page_pause_ms {} is impolite (<100ms); raise it",
                self.page_pause_ms
            ));
        }
        Ok(())
    }
}
