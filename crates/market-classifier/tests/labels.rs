//! Manually-labelled classification ground truth.
//!
//! Labels derived from reading the market *questions* — deliberately
//! independent from ruleset internals. `icon_marker` indicates which icon
//! file name the activity record carried ("" = no icon).

use tokor_domain::classification::CryptoVerdict;
use tokor_domain::market::MarketRef;

pub struct LabelledCase {
    pub name: &'static str,
    pub condition_id: &'static str,
    pub title: &'static str,
    pub market_slug: &'static str,
    pub event_slug: &'static str,
    pub icon_marker: &'static str,
    pub expected: CryptoVerdict,
}

impl LabelledCase {
    pub fn all() -> Vec<LabelledCase> {
        vec![
            // --- crypto positives (observed in the Phase-0 Tokor sample) ---
            LabelledCase {
                name: "btc_dip_threshold",
                condition_id: "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
                title: "Will Bitcoin dip to $73,000 on November 9?",
                market_slug: "will-bitcoin-dip-to-73k-on-november-9-2026",
                event_slug: "what-price-will-bitcoin-hit-on-november-9-2026",
                icon_marker: "BTC",
                expected: CryptoVerdict::Crypto,
            },
            LabelledCase {
                name: "eth_monthly_range",
                condition_id: "0xeth1",
                title: "What price will Ethereum hit in November 2026?",
                market_slug: "what-price-will-ethereum-hit-in-november-2026-w28",
                event_slug: "what-price-will-ethereum-hit-in-november-2026",
                icon_marker: "ETH",
                expected: CryptoVerdict::Crypto,
            },
            LabelledCase {
                name: "btc_up_or_down",
                condition_id: "0xbtcud",
                title: "Bitcoin Up or Down on November 3, 2026?",
                market_slug: "bitcoin-up-or-down-on-november-3-2026",
                event_slug: "bitcoin-up-or-down-on-november-3-2026",
                icon_marker: "BTC",
                expected: CryptoVerdict::Crypto,
            },
            LabelledCase {
                name: "solana_price",
                condition_id: "0xsol1",
                title: "What price will Solana hit in November 2026?",
                market_slug: "what-price-will-solana-hit-in-november-2026-w1",
                event_slug: "what-price-will-solana-hit-in-november-2026",
                icon_marker: "SOL",
                expected: CryptoVerdict::Crypto,
            },
            LabelledCase {
                name: "hyperliquid_before_2027",
                condition_id: "0xhype1",
                title: "What price will Hyperliquid hit before 2027?",
                market_slug: "what-price-will-hyperliquid-hit-before-2027-w1",
                event_slug: "what-price-will-hyperliquid-hit-before-2027",
                icon_marker: "HYPE",
                expected: CryptoVerdict::Crypto,
            },
            // --- hard exclusions (shared threshold language) ---
            LabelledCase {
                name: "wti_commodity",
                condition_id: "0xwti1",
                title: "What price will WTI hit in November 2026?",
                market_slug: "what-price-will-wti-hit-in-november-2026-w2",
                event_slug: "what-price-will-wti-hit-in-november-2026",
                icon_marker: "",
                expected: CryptoVerdict::NonCrypto,
            },
            LabelledCase {
                name: "oil_week",
                condition_id: "0xwti2",
                title: "Will WTI hit $70 during the week of November 2, 2026?",
                market_slug: "will-wti-hit-week-of-november-2-2026",
                event_slug: "will-wti-hit-week-of-november-2-2026",
                icon_marker: "",
                expected: CryptoVerdict::NonCrypto,
            },
            LabelledCase {
                name: "gold_price",
                condition_id: "0xgold1",
                title: "What price will Gold hit in December 2026?",
                market_slug: "what-price-will-gold-hit-in-december-2026-w1",
                event_slug: "what-price-will-gold-hit-in-december-2026",
                icon_marker: "",
                expected: CryptoVerdict::NonCrypto,
            },
            LabelledCase {
                name: "nvidia_stock",
                condition_id: "0xnvda",
                title: "What price will NVIDIA stock hit in December 2026?",
                market_slug: "what-price-will-nvidia-stock-hit-in-december-2026-w1",
                event_slug: "what-price-will-nvidia-hit-in-december-2026",
                icon_marker: "",
                expected: CryptoVerdict::NonCrypto,
            },
            // --- ambiguity → must be UNKNOWN, never guessed CRYPTO ---
            LabelledCase {
                name: "ambiguous_no_assets",
                condition_id: "0xamb1",
                title: "Will the market close higher in 2026?",
                market_slug: "will-the-market-close-higher-in-2026",
                event_slug: "will-the-market-close-higher-in-2026",
                icon_marker: "",
                expected: CryptoVerdict::Unknown,
            },
            LabelledCase {
                name: "tricky_eth_substring_title",
                condition_id: "0xamb2",
                title: "Will Wethersfield build the new school by 2027?",
                market_slug: "wethersfield-school-by-2027",
                event_slug: "wethersfield-school-by-2027",
                icon_marker: "",
                expected: CryptoVerdict::Unknown,
            },
        ]
    }
}

impl LabelledCase {
    pub fn clone_case(&self, icon_marker: &'static str) -> LabelledCase {
        LabelledCase {
            name: self.name,
            condition_id: self.condition_id,
            title: self.title,
            market_slug: self.market_slug,
            event_slug: self.event_slug,
            icon_marker,
            expected: self.expected,
        }
    }

    pub fn market_ref(&self, icon: String) -> MarketRef {
        MarketRef {
            condition_id: self.condition_id.to_string(),
            title: Some(self.title.to_string()),
            market_slug: Some(self.market_slug.to_string()),
            event_slug: Some(self.event_slug.to_string()),
            icon: Some(icon),
            tags: None,
            description: None,
            tokens: None,
            active: None,
            closed: None,
            minimum_tick_size: None,
            minimum_order_size: None,
            neg_risk: None,
        }
    }
}
