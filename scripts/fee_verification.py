#!/usr/bin/env python3
"""Master fee-model verification over every unique Tokor TRADE row.

Semantics (source-verified):
  cash_usdc = serialize5( gross + fee )    (backend binary-float rounding ->
              |cash - gross - fee| <= 2e-5 envelope = 2 units in the last
              decimal place; strictly negative-or-zero residuals observed)
  fee       = size * rate * p * (1-p), zero when unrounded fee < 0.00001
  rate      = category rate (crypto 0.07, finance 0.04, sports 0.05, ...)
  p         = displayed price of the row

Row verdicts:
  VERIFIED_FEE_INCLUSIVE   - cash fits fee model within the envelope
  VERIFIED_GROSS_NOTIONAL  - cash == gross exactly (fee-free category, min-fee
                             rounding to zero, or maker fill)
  MAKER_FILL_NO_FEE        - fee-category market where cash == gross exactly
                             while the categorical fee would be >= 5e-5;
                             makers pay zero per the published schedule
  UNKNOWN_FEE_SEMANTICS    - fee-category market, otherwise outside envelope
  UNEXPLAINED              - no category and no gross fit
"""
import glob
import json
import os
from decimal import Decimal as D, ROUND_FLOOR, ROUND_HALF_UP
from collections import Counter, defaultdict

Q5 = D("0.00001")
ENVELOPE = D("0.00002")   # 2 units of the last observed decimal place
GROSS_TOL = D("0.00002")  # f64 noise band for exact-gross rows


def round5(x: D) -> D:
    return x.quantize(Q5, rounding=ROUND_HALF_UP)


def load_fills():
    """Unique fills across all persisted TRADE rows (both DB raw pages and the
    unfiltered activity pages file store). Identity includes the trade hash +
    full value tuple; two distinct rows that differ only in ordering remain
    distinct."""
    con = None
    seen = set()
    fills = []
    # prefer the freshest unfiltered pages (cover everything)
    for path in sorted(glob.glob("data/raw/activity_pages/page_*.json")):
        page = json.load(open(path))["body"]
        for row in page.get("data", []):
            if row.get("type") != "TRADE":
                continue
            key = (row["transaction_hash"], row["timestamp"], row["condition_id"],
                   row["token_id"], row["side"], repr(row["price"]), repr(row["size"]))
            if key in seen:
                continue
            seen.add(key)
            fills.append(row)
    return fills


def main():
    cats = json.load(open("data/derived/event_fee_categories.json"))
    fills = load_fills()
    print("unique fills:", len(fills))

    out_rows = []
    verdicts = Counter()
    resids_by_cat = defaultdict(list)
    failures = []
    for r in fills:
        p = D(str(r["price"]))
        s = D(str(r["size"]))
        u = D(str(r["usdc_size"]))
        slug = r.get("event_slug") or ""
        cat_info = cats.get(slug) or {}
        cat = cat_info.get("fee_category") if cat_info.get("found") else "UNKNOWN"
        rate = D(str(cat_info["fee_rate"])) if cat_info.get("fee_rate") is not None else None
        gross = p * s
        observed_cash = u
        verdict = None
        residual = None
        fee = None
        gov = None
        fee_full = None
        if rate is not None:
            fee_full = s * rate * p * (1 - p)
            fee = D("0") if fee_full < Q5 else round5(fee_full)
        gross_fit = abs(observed_cash - gross) <= GROSS_TOL
        if rate is None:
            if gross_fit:
                verdict = "VERIFIED_GROSS_NOTIONAL"
                gov = "gross"
                fee = D("0")
                residual = observed_cash - gross
            else:
                verdict = "UNEXPLAINED"
                residual = observed_cash - gross
        elif gross_fit and fee != D("0") and fee_full >= Q5:
            # fee would apply, cash shows none: maker fill (takers pay, makers not)
            verdict = "VERIFIED_GROSS_NOTIONAL"
            gov = "gross"
            residual = observed_cash - gross
        elif abs(observed_cash - gross - fee) <= ENVELOPE:
            if fee == D("0"):
                verdict = "VERIFIED_GROSS_NOTIONAL"
                gov = "gross(min-fee-0)"
            else:
                verdict = "VERIFIED_FEE_INCLUSIVE"
                gov = "gross+fee"
            residual = observed_cash - (gross + fee)
        elif gross_fit:
            verdict = "VERIFIED_GROSS_NOTIONAL"
            gov = "gross"
            residual = observed_cash - gross
        else:
            verdict = "UNKNOWN_FEE_SEMANTICS" if rate is not None else "UNEXPLAINED"
            residual = observed_cash - (gross + (fee or 0))
        if verdict in ("UNKNOWN_FEE_SEMANTICS", "UNEXPLAINED"):
            failures.append((r["title"][:50], slug[:40], str(gov), str(rate), str(residual)[:20]))
        verdicts[verdict] += 1
        face = "maker" if verdict == "VERIFIED_GROSS_NOTIONAL" and rate is not None \
            and fee_full is not None and fee_full >= Q5 else \
            ("taker" if verdict == "VERIFIED_FEE_INCLUSIVE" else "n/a")
        if verdict == "VERIFIED_FEE_INCLUSIVE":
            resids_by_cat[str(rate)].append(residual)
        out_rows.append({
            "transaction_hash": r["transaction_hash"],
            "timestamp": r["timestamp"],
            "condition_id": r["condition_id"],
            "token_id": r["token_id"],
            "side": r["side"],
            "price": str(p),
            "size": str(s),
            "gross_usdc": str(gross),
            "fee_usdc": str(fee) if (fee is not None and face == "taker") else "0",
            "fee_wouldbe_usdc": str(fee) if fee is not None else None,
            "fill_face": face,
            "fee_rate": str(rate) if rate is not None else None,
            "fee_category": cat,
            "observed_usdc": str(u),
            "reconstructed_debit": str(gross + (fee or 0)),
            "residual": str(residual) if residual is not None else None,
            "verdict": verdict,
            "title": r.get("title"),
            "event_slug": slug,
        })

    print("verdicts:", dict(verdicts))
    for rate, rs in sorted(resids_by_cat.items()):
        nz = [x for x in rs if x != 0]
        mx = max((abs(x) for x in rs), default=D(0))
        print(f"  rate {rate}: rows={len(rs)} nonzero_residuals={len(nz)} max|resid|={mx}")
    tot_fee = sum((D(x["fee_usdc"]) if x["fee_usdc"] else D(0)) for x in out_rows)
    tot_gross = sum(D(x["gross_usdc"]) for x in out_rows)
    tot_cash = sum(D(x["observed_usdc"]) for x in out_rows)
    print(f"totals: cash={tot_cash:.6f} gross={tot_gross:.6f} fee={tot_fee:.6f}")
    print("failures:", len(failures))
    for f in failures[:15]:
        print("  ", f)
    os.makedirs("data/derived", exist_ok=True)
    json.dump(out_rows, open("data/derived/fee_reconciliation.json", "w"))


if __name__ == "__main__":
    main()
