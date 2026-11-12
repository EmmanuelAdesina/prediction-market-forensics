#!/usr/bin/env python3
"""Phase 2 §2 - on-chain liquidity-role validation via Polygon receipts.

Read-only. For every distinct transaction hash in `source_fills`, fetch the
transaction receipt from a public Polygon RPC (no key), decode fill events
from the Polymarket exchange contracts, and classify the wallet's role per
fill.

Contract generations seen in the wallet's corpus (verified 2026-11-10):

  * v2 fee-era exchange  0xe111180000d2663c0091e4f400237545b87b996b
      fill event topic0 0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
      (signature name unresolved - topic0 pinned empirically; 3 indexed
       [orderHash, maker, taker] + 7 data words:
       [0] side of the event maker's order (0=BUY, 1=SELL - validated
           against the feed across the sample),
       [1] tokenId,
       [2] makerAmountFilled, [3] takerAmountFilled,
       [4] fee charged to the EVENT MAKER in USDC (1e6),
       [5..6] zero in samples; recorded raw).
  * v1 CTF exchange      0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
  * v1 negRisk exchange  0xC5d563A36AE78145C45a50134d48A1215220f80a
      OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
      topic0 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
      (fee denominated in the order's takerAssetId, deducted from proceeds).

Role semantics (identical for both generations, per Trading.sol):
  * The event MAKER slot is always the ORDER OWNER.
  * When an operator matches orders, the aggressor's (taker) order emits with
    taker = the exchange itself; each matched maker order emits with
    taker = the taker-order owner.
  Therefore:
    CONFIRMED_MAKER   wallet in maker slot with taker != exchange (rested)
    CONFIRMED_TAKER   wallet in taker slot, or wallet in maker slot with
                      taker == exchange (aggressive order)
  Fee ground truth (v2): the wallet's own-order event fee vs the verified
  model fee = size*rate*p*(1-p); resting-maker events must carry fee 0.

Outputs:
    data/raw/onchain_receipts/<tx>.json   verbatim receipts (evidence layer)
    data/derived/onchain_role_validation.json
    data/derived/liquidity_role_validation.parquet
"""

import json
import sys
import time
import random
import sqlite3
from pathlib import Path

import requests

DB = Path("data/tokor.db")
RECEIPT_DIR = Path("data/raw/onchain_receipts")
OUT_JSON = Path("data/derived/onchain_role_validation.json")
OUT_PARQUET = Path("data/derived/liquidity_role_validation.parquet")

WALLET = "0xa19cbababc312f9df185e49d7004c249ed1ade6b"  # Tokor proxy (lowercase)

V2_EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"
CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
EXCHANGES = {
    CTF_EXCHANGE: "ctf_exchange_v1",
    NEG_RISK_EXCHANGE: "neg_risk_exchange_v1",
    V2_EXCHANGE: "exchange_v2",
}

# Fill-event topic0s per generation.
V1_ORDER_FILLED = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
V2_ORDER_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
FILL_TOPICS = {V1_ORDER_FILLED, V2_ORDER_FILLED}

# Keyless public RPCs, tried in rotation.
RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
    "https://1rpc.io/matic",
    "https://polygon.llamarpc.com",
]

BATCH = 10
BATCH_PAUSE = 1.0
SAMPLE = None  # None = full corpus; int = first N distinct txs


class Rpc:
    def __init__(self, urls):
        self.urls = urls
        self.i = 0
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "tokor-research/0.1 (read-only)"

    def batch_receipts(self, hashes):
        calls = [
            {"jsonrpc": "2.0", "id": h, "method": "eth_getTransactionReceipt", "params": [h]}
            for h in hashes
        ]
        last_err = None
        for k in range(len(self.urls)):
            url = self.urls[(self.i + k) % len(self.urls)]
            try:
                r = self.session.post(url, json=calls, timeout=30)
                r.raise_for_status()
                resp = r.json()
                out = {}
                for item in resp if isinstance(resp, list) else [resp]:
                    h = item.get("id")
                    if h is None:
                        continue
                    out[h] = None if "error" in item else item.get("result")
                self.i = (self.i + k + 1) % len(self.urls)
                return out
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"all RPC endpoints failed; last: {last_err}")


def word(data_hex, idx):
    body = data_hex[2:] if data_hex.startswith("0x") else data_hex
    off = idx * 64
    return int(body[off : off + 64], 16)


def addr(topic_hex):
    return "0x" + topic_hex[-40:]


def decode_fill(log):
    """Decode one fill event (v1 or v2) into a normalized dict."""
    t0 = log["topics"][0].lower()
    maker, taker = addr(log["topics"][1 + 1]), addr(log["topics"][2 + 1])
    if t0 == V2_ORDER_FILLED:
        ws = [word(log["data"], i) for i in range(7)]
        return {
            "contract": "exchange_v2",
            "maker": maker,
            "taker": taker,
            "maker_side_flag": ws[0],  # 0=BUY, 1=SELL (validated vs feed)
            "token_id": str(ws[1]),
            "maker_amount": ws[2],
            "taker_amount": ws[3],
            "fee": ws[4],
            "fee_units": "usdc_1e6",
            "extra": [ws[5], ws[6]],
            "log_index": int(log["logIndex"], 16),
        }
    # v1 OrderFilled
    ws = [word(log["data"], i) for i in range(5)]
    maker_asset, taker_asset = ws[0], ws[1]
    side = "BUY" if maker_asset == 0 and taker_asset != 0 else "SELL"
    return {
        "contract": EXCHANGES.get(log["address"].lower(), "v1"),
        "maker": maker,
        "taker": taker,
        "maker_side_flag": 0 if side == "BUY" else 1,
        "token_id": str(taker_asset if side == "BUY" else maker_asset),
        "maker_amount": ws[2],
        "taker_amount": ws[3],
        "fee": ws[4],
        "fee_units": "taker_asset_units",
        "extra": [],
        "log_index": int(log["logIndex"], 16),
    }


def classify(events):
    """Role of the wallet within one tx's decoded fill events."""
    rested = [e for e in events if e["maker"] == WALLET and e["taker"] != V2_EXCHANGE]
    own_aggressive = [e for e in events if e["maker"] == WALLET and e["taker"] == V2_EXCHANGE]
    as_taker_slot = [e for e in events if e["taker"] == WALLET]
    if rested and not own_aggressive and not as_taker_slot:
        return "CONFIRMED_MAKER", rested
    if own_aggressive or as_taker_slot:
        return "CONFIRMED_TAKER", (own_aggressive + as_taker_slot) or as_taker_slot
    if rested:
        return "CONFIRMED_MAKER", rested  # both patterns present: report both, maker first
    return "ROLE_UNKNOWN", []


def main():
    conn = sqlite3.connect(DB)
    rows = list(
        conn.execute(
            """
            SELECT transaction_hash, side, price, size, usdc_size, token_id
            FROM source_fills
            ORDER BY timestamp, transaction_hash
            """
        )
    )
    conn.close()
    print(f"source fills: {len(rows)}", flush=True)

    txs = sorted({r[0].lower() for r in rows})
    if SAMPLE:
        txs = txs[:SAMPLE]
    print(f"distinct txs to fetch: {len(txs)}", flush=True)

    fills_by_tx = {}
    for r in rows:
        fills_by_tx.setdefault(r[0].lower(), []).append(r)

    rpc = Rpc(RPCS)
    done = events_total = 0
    role_counts = {"CONFIRMED_MAKER": 0, "CONFIRMED_TAKER": 0, "ROLE_UNKNOWN": 0}
    side_flag_ok = side_flag_bad = 0
    fee_checks = []
    role_rows = []
    t0 = time.time()

    for i in range(0, len(txs), BATCH):
        batch = txs[i : i + BATCH]
        need = [h for h in batch if not (RECEIPT_DIR / f"{h}.json").exists()]
        if need:
            got = None
            for attempt in range(5):
                try:
                    got = rpc.batch_receipts(need)
                    break
                except RuntimeError as e:
                    print(f"  batch error ({e}); pausing", flush=True)
                    time.sleep(5 * (attempt + 1))
            if got is None:
                print("  skipping batch after retries", flush=True)
                continue
            for h, receipt in got.items():
                if receipt is not None:
                    (RECEIPT_DIR / f"{h}.json").write_text(json.dumps(receipt))
                else:
                    (RECEIPT_DIR / f"{h}.missing").write_text("{}")
            time.sleep(BATCH_PAUSE)

        for h in batch:
            path = RECEIPT_DIR / f"{h}.json"
            if not path.exists():
                role_rows.append({"transaction_hash": h, "status": "RECEIPT_MISSING"})
                continue
            receipt = json.loads(path.read_text())
            if receipt.get("status") != "0x1":
                role_rows.append({"transaction_hash": h, "status": "TX_FAILED"})
                continue
            evs = []
            for log in receipt.get("logs", []):
                if (
                    log["address"].lower() in EXCHANGES
                    and log["topics"]
                    and log["topics"][0].lower() in FILL_TOPICS
                ):
                    try:
                        evs.append(decode_fill(log))
                    except Exception as e:  # noqa: BLE001
                        print(f"  decode failed {h}: {e}", flush=True)
            events_total += len(evs)

            role, matched = classify(evs)
            own_events = [e for e in evs if e["maker"] == WALLET]
            other_side_events = [e for e in evs if e["taker"] == WALLET and e["maker"] != WALLET]

            for f in fills_by_tx.get(h, []):
                side, price, size, usdc, token_id = f[1], f[2], f[3], f[4], str(f[5])
                own = next((e for e in own_events if e["token_id"] == token_id), None)
                counter = next((e for e in other_side_events if e["token_id"] == token_id), None)
                role_counts[role] = role_counts.get(role, 0) + 1
                ev_side_flag = None
                if own is not None:
                    ev_side_flag = own["maker_side_flag"]
                    expect = 0 if side == "BUY" else 1
                    if ev_side_flag == expect:
                        side_flag_ok += 1
                    else:
                        side_flag_bad += 1
                row = {
                    "transaction_hash": h,
                    "status": "OK",
                    "token_id": token_id,
                    "feed_side": side,
                    "feed_price": price,
                    "feed_size": size,
                    "feed_usdc": usdc,
                    "role": role,
                    "own_event_contract": own["contract"] if own else None,
                    "own_event_side_flag": ev_side_flag,
                    "own_event_fee": own["fee"] if own else None,
                    "own_event_maker_amount": own["maker_amount"] if own else None,
                    "own_event_taker_amount": own["taker_amount"] if own else None,
                    "counterparty_fee": counter["fee"] if counter else None,
                    "n_fill_events": len(evs),
                }
                role_rows.append(row)
                if own is not None:
                    fee_checks.append(
                        {
                            "tx": h,
                            "contract": own["contract"],
                            "feed_side": side,
                            "feed_price": price,
                            "feed_size": size,
                            "onchain_fee_raw": own["fee"],
                        }
                    )

        done += len(batch)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        eta = (len(txs) - done) / rate if rate > 0 else 0
        print(
            f"[{done}/{len(txs)}] events={events_total} "
            f"maker={role_counts['CONFIRMED_MAKER']} taker={role_counts['CONFIRMED_TAKER']} "
            f"unknown={role_counts['ROLE_UNKNOWN']} side_ok={side_flag_ok} "
            f"side_bad={side_flag_bad} eta={eta/60:.1f}min",
            flush=True,
        )

    summary = {
        "wallet": WALLET,
        "topic0s": {"v1": V1_ORDER_FILLED, "v2": V2_ORDER_FILLED},
        "exchanges": EXCHANGES,
        "txs_fetched": len(txs),
        "events_total": events_total,
        "wallet_fills_classified": sum(role_counts.values()),
        "role_counts": role_counts,
        "side_flag_ok": side_flag_ok,
        "side_flag_bad": side_flag_bad,
        "elapsed_min": round((time.time() - t0) / 60, 2),
        "fee_checks_sample": fee_checks[:20],
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "fee_checks_sample"}, indent=2))

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Union of keys across rows: role_rows[0] may be a RECEIPT_MISSING stub
    # (2 keys) while OK rows carry the full per-fill schema. Writing from the
    # first row's keys alone silently discards every per-fill column.
    seen_cols: set = set()
    cols: list = []
    for r in role_rows:
        for k in r.keys():
            if k not in seen_cols:
                seen_cols.add(k)
                cols.append(k)
    table = pa.table({c: pa.array([r.get(c) for r in role_rows]) for c in cols})
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT_PARQUET)
    print(f"wrote {OUT_PARQUET} ({len(role_rows)} rows)", flush=True)


if __name__ == "__main__":
    if "--sample" in sys.argv:
        SAMPLE = 200
    main()
