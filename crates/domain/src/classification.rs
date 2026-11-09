//! Deterministic crypto-scope classification with recorded evidence.

use serde::{Deserialize, Serialize};

/// Classifier output: conservative by construction. Ambiguous → UNKNOWN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CryptoVerdict {
    /// Genuinely within cryptocurrency scope (initial engine scope).
    Crypto,
    /// Definitively *not* crypto (commodities, FX, equities, sports, politics…).
    NonCrypto,
    /// Insufficient evidence to decide — must be treated as NOT in scope
    /// for trading and queued for manual review.
    Unknown,
}

impl CryptoVerdict {
    /// Only CRYPTO is in-scope. UNKNOWN is never silently treated as crypto.
    pub fn in_scope(self) -> bool {
        matches!(self, CryptoVerdict::Crypto)
    }
}

/// One piece of supporting evidence for the verdict, carrying where it came from.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum EvidenceSignal {
    /// Naive keyword match on slug/title.
    SlugTerm { term: String },
    /// Hard exclusion term (commodity/eq/other).
    SlugHardTerm { term: String },
    /// Crypto icon marker (e.g. "BTC", "ETH" in the icon filename).
    IconToken { token: String },
    /// Gamma tag on the event or market.
    Tags { tag: String },
    /// Description contained an asset term.
    DescriptionTerm { term: String },
}

impl EvidenceSignal {
    /// Stable discriminator key for dedup/ordering.
    pub fn sort_key(&self) -> String {
        match self {
            EvidenceSignal::SlugTerm { term } => format!("slug:{term}"),
            EvidenceSignal::SlugHardTerm { term } => format!("hard:{term}"),
            EvidenceSignal::IconToken { token } => format!("icon:{token}"),
            EvidenceSignal::Tags { tag } => format!("tag:{tag}"),
            EvidenceSignal::DescriptionTerm { term } => format!("desc:{term}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClassifierEvidence {
    pub verdict: CryptoVerdict,
    pub signals: Vec<EvidenceSignal>,
    pub negated_signals: Vec<EvidenceSignal>,
    /// Stable version string recorded with every classification decision.
    pub classifier_version: String,
}

/// Version of the rules implemented in this crate.
/// Bump on any rule set change.
pub const CLASSIFIER_VERSION: &str = "crypto-cls-v0.2.0";

/// A pre-constructed verdict with evidence, ready for persistence.
pub type ClassifiedVerdict = ClassifierEvidence;

impl ClassifierEvidence {
    pub fn new(verdict: CryptoVerdict) -> Self {
        Self {
            verdict,
            signals: Vec::new(),
            negated_signals: Vec::new(),
            classifier_version: CLASSIFIER_VERSION.to_string(),
        }
    }
}
