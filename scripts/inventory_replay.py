#!/usr/bin/env python3
"""Inventory replay reconciliation.

Replays the FULL unfiltered activity feed (all event types) into per-token
position ledgers and explains each discrepancy against the /v2/positions
census (OPEN + CLOSED + REDEEMABLE, all pages, status-filtered walks).

Outputs an interim JSON (data/derived/inventory_replay.json) for the parquet
writer and prints the explanatory breakdown for every OPEN discrepancy.
"""
import glob
import json
import os
from decimal import Decimal as D, ROUND_FLOOR
from collections import defaultdict, Counter

Q5 = D("0.00001")
CATS = json.load(open("data/derived/event_fee_categories.json")) \
    if os.path.exists("data/derived/event_fee_categories.json") else {}


def fee_for(row):
    p = D(str(row["price"]))
    s = D(str(row["size"]))
    rate = CATS.get(row.get("event_slug") or "", {}).get("fee_rate")
    if rate is None or rate == 0:
        return D(0)
    f = s * D(str(rate)) * p * (1 - p)
    if f < Q5:
        return D(0)
    return f.quantize(Q5, rounding=ROUND_FLOOR)


def load_events():
    events = defaultdict(lambda: {"buy_size": D(0), "buy_cash": D(0), "buy_gross": D(0),
                                  "buy_fee": D(0), "buy_count": 0,
                                  "redeem_size": D(0), "redeem_count": 0,
                                  "merge_size": D(0), "merge_cash": D(0), "merge_count": 0,
                                  "sell_size": D(0), "sell_cash": D(0), "sell_count": 0,
                                  "other_types": Counter()})
    all_rows = defaultdict(Counter)
    for path in sorted(glob.glob("data/raw/activity_pages/page_*.json")):
        for t in json.load(open(path))["body"].get("data", []):
            ty = t.get("type")
            tok = t.get("token_id") or ""
            cid = t.get("condition_id") or ""
            all_rows[cid][ty] += 1  # condition-level census too
            if ty == "TRADE":
                key = (t["transaction_hash"], t["timestamp"], t["condition_id"], t["token_id"],
                       t["side"], repr(t["price"]), repr(t["size"]))
            else:
                key = (t["transaction_hash"] or t.get("type") + str(t["timestamp"]),
                       t["timestamp"], cid, tok, ty, repr(t.get("price")), repr(t.get("size")))
            bucket = events[tok]
            if ty == "TRADE":
                s = D(str(t["size"]))
                p = D(str(t["price"]))
                fee = fee_for(t)
                gross = p * s
                if t["side"] == "BUY":
                    bucket["buy_size"] += s
                    bucket["buy_cash"] += D(str(t["usdc_size"]))
                    bucket["buy_gross"] += gross
                    bucket["buy_fee"] += fee
                    bucket["buy_count"] += 1
                else:
                    bucket["sell_size"] += s
                    bucket["sell_cash"] += D(str(t["usdc_size"]))
                    bucket["sell_count"] += 1
            elif ty == "REDEEM":
                bucket["redeem_size"] += D(str(t["size"]))
                bucket["redeem_count"] += 1
            elif ty == "MERGE":
                bucket["merge_size"] += D(str(t["size"]))
                bucket["merge_cash"] += D(str(t["usdc_size"]))
                bucket["merge_count"] += 1
            else:
                bucket["other_types"][ty] += 1
    return events


def main():
    events = load_events()
    print("tokens touched by events:", len(events))

    # positions census legs
    legs = {}
    for status in ("open", "closed", "redeemable"):
        for path in glob.glob(f"data/raw/positions_pages/{status}/page_*.json"):
            for r in json.load(open(path))["body"].get("data", []):
                tok = r.get("asset") or r.get("token_id") or ""
                legs[tok] = {"status": r.get("status"), "condition": r.get("condition_id"),
                             "outcome": r.get("outcome"), "size": r.get("size"),
                             "current_size": r.get("current_size"),
                             "avg_price": r.get("avg_price"),
                             "entry_cost": r.get("entry_cost_usdc"),
                             "entry_fees": r.get("entry_fees_usdc"),
                             "realized_pnl": r.get("realized_pnl")}
    print("positions legs:", len(legs))

    # replay open inventory per token: buys - redeems - merges
    explained = []
    diffs = Counter()
    report = []
    for tok, eg in legs.items():
        if not tok:
            continue
        e = events.get(tok, {"buy_size": D(0), "redeem_size": D(0), "merge_size": D(0),
                             "buy_count": 0, "sell_count": 0, "sell_size": D(0),
                             "buy_gross": D(0), "buy_fee": D(0), "merge_cash": D(0),
                             "merge_count": 0, "redeem_count": 0, "buy_cash": D(0)})
        replay_open = e["buy_size"] - e["redeem_size"] - e["merge_size"]
        api_open = D(str(eg["current_size"] or 0))
        delta = replay_open - api_open
        entry = []
        if e["redeem_size"] > 0:
            entry.append(f"redeemed {e['redeem_size']}")
        if e["merge_size"] > 0:
            entry.append(f"merged {e['merge_size']} (${e['merge_cash']})")
        if e["sell_size"] > 0:
            entry.append(f"sold {e['sell_size']}")
        if delta == 0:
            diffs["exact"] += 1
        elif abs(delta) <= D("0.0001"):
            diffs["exact_~1e-4"] += 1
        else:
            diffs["delta"] += 1
            entry.append(f"RESIDUAL_delta={delta}")
        report.append({
            "token_id": tok, "condition": eg["condition"], "status": eg["status"],
            "outcome": eg["outcome"],
            "api_current_size": str(api_open), "api_avg": str(eg["avg_price"]),
            "api_entry_cost": str(eg["entry_cost"]), "api_entry_fees": str(eg["entry_fees"]),
            "api_realized": str(eg["realized_pnl"]),
            "replay_buys_shares": str(e["buy_size"]), "replay_buys_gross": str(e["buy_gross"]),
            "replay_buys_cash": str(e["buy_cash"]), "replay_buys_fee": str(e["buy_fee"]),
            "replay_buys_n": e["buy_count"],
            "replay_redeemed_shares": str(e["redeem_size"]), "replay_redeem_events": e["redeem_count"],
            "replay_merged_shares": str(e["merge_size"]), "replay_merge_cash": str(e["merge_cash"]),
            "replay_merge_events": e["merge_count"],
            "sell_shares": str(e["sell_size"]), "sell_events": e["sell_count"],
            "delta_open": str(delta),
            "explanation": "; ".join(entry) or "no disposals",
        })

    print("delta distribution:", dict(diffs))
    open_legs = [r for r in report if r["status"] == "OPEN"]
    open_deltas = [D(r["delta_open"]) for r in open_legs]
    zero = sum(1 for d in open_deltas if abs(d) <= D("0.0001"))
    print(f"OPEN legs: {len(open_legs)} explained-exactly (delta=0): {zero}")
    print("OPEN legs with residual delta:")
    shown = 0
    for r in open_legs:
        d = D(r["delta_open"])
        if abs(d) > D("0.0001") and shown < 20:
            shown += 1
            print(f"  {r['token_id'][:12]} {r['outcome']:>4} delta={d} buys={r['replay_buys_shares']} redeemed={r['replay_redeemed_shares']} merged={r['replay_merged_shares']} sold={r['sell_shares']} api={r['api_current_size']} | {r['explanation']}")

    # condition-level census for merge accounting
    os.makedirs("data/derived", exist_ok=True)
    json.dump({"legs": report}, open("data/derived/inventory_replay.json", "w"))
    print("wrote data/derived/inventory_replay.json")


if __name__ == "__main__":
    main()
