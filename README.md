# tokor-engine

Rust-native, event-driven quant engine for evaluating the trading activity of the Polymarket
wallet `0xa19cbababc312f9df185e49d7004c249ed1ade6b` ("Tokor") as an information source.

**Status: Phase 0 (reconnaissance) — see [docs/00-phase0-recon-and-assessment.md](docs/00-phase0-recon-and-assessment.md).**

## Guiding principles

- Tokor is an information source, **not** an authority. Every detected trade is independently
  evaluated before any capital is committed.
- Two parallel strategy paths always run: an **exact-copy benchmark** (control group) and the
  **independent intelligent-copy strategy** (commands live execution).
- Capital preservation first: hard limits, independent risk-engine veto, explicit operator
  controls. Live trading is never the default.
- Every trade, refusal, and unit of risk must be explainable and reproducible from the durable
  journal.

## Layout

```text
docs/        Engineering assessments, architecture, threat model, operations
config/      TOML configuration (capital limits, risk limits, markets) — committed, no secrets
crates/      Rust workspace crates (populated from Phase 1)
data/        Raw forensic captures of API responses (gitignored with exceptions)
migrations/  SQLite schema migrations
```

## Rust toolchain note

Source lives on `/mnt/c` (persistence); build artifacts are directed to the Linux-side
`~/.cargo-target` via `CARGO_TARGET_DIR` to avoid the WSL drvfs performance penalty.
See `scripts/setup-env.sh`.
