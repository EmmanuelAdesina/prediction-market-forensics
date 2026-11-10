#!/usr/bin/env python3
"""Resolve Gamma event metadata (tags -> fee category) for all event slugs
touched by Tokor fills. Saves the derived category map to
data/derived/event_fee_categories.json. Idempotent and resumable.
"""
import json
import os
import time
import urllib.request

UA = {"User-Agent": "tokor-engine/0.1"}
GAMMA = "https://gamma-api.polymarket.com/events?slugs="

# Official fee schedule by category (help.polymarket.com "Trading Fees", 2026-07-10)
RATE_BY_TAG = {
    "crypto": 0.07,
    "sports": 0.05,
    "finance": 0.04,
    "politics": 0.04,
    "economics": 0.05,
    "culture": 0.05,
    "weather": 0.05,
    "mentions": 0.04,
    "tech": 0.04,
    "geopolitics": 0.0,
    "other/general/other": 0.05,  # placeholder, resolved below by slug taxonomy
}


def classify(tags):
    tags = set(tags)
    if "geopolitics" in tags:
        return "geopolitics", 0.0
    if "crypto" in tags:
        return "crypto", 0.07
    if "sports" in tags:
        return "sports", 0.05
    if "finance" in tags or "stocks" in tags or "commodities" in tags:
        return "finance", 0.04
    if "politics" in tags:
        return "politics", 0.04
    if "weather" in tags:
        return "weather", 0.05
    if "economics" in tags:
        return "economics", 0.05
    if "culture" in tags:
        return "culture", 0.05
    if "tech" in tags or "science" in tags:
        return "tech", 0.04
    if "mentions" in tags:
        return "mentions", 0.04
    return "other", None


def main():
    census = json.load(open("data/raw/event_slug_census.json"))
    slugs = sorted(census)
    out_path = "data/derived/event_fee_categories.json"
    os.makedirs("data/derived", exist_ok=True)
    out = {}
    if os.path.exists(out_path):
        out = json.load(open(out_path))
    pending = [s for s in slugs if s not in out]
    print(f"slugs total={len(slugs)} cached={len(slugs) - len(pending)} pending={len(pending)}")
    for i, s in enumerate(pending):
        url = GAMMA.replace("events?slugs=", "events?slug=") + s
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    events = json.loads(resp.read().decode())
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 4:
                    raise
                time.sleep(1.5 * (attempt + 1))
        slug_used = s
        e = events[0] if isinstance(events, list) and events else None
        if not e:
            out[slug_used] = {"found": False}
            continue
        tags = [t.get("slug") for t in (e.get("tags") or [])]
        series = [x.get("slug") for x in (e.get("series") or [])]
        cat, rate = classify(tags)
        if rate is None:
            # 'other/general' bucket
            cat, rate = "other", 0.05
        out[slug_used] = {"found": True, "event_id": e.get("id"), "tags": tags,
                          "series": series, "fee_category": cat, "fee_rate": rate}
        os.makedirs("data/derived", exist_ok=True)
        json.dump(out, open(out_path, "w"), indent=0)
        time.sleep(0.25)
    # census by category
    from collections import Counter
    cats = Counter((v.get("fee_category") if v.get("found") else "NOT_FOUND") for v in out.values())
    print("categories:", dict(cats))
    miss = [k for k, v in out.items() if not v.get("found")]
    print("missing:", len(miss), miss[:10])


if __name__ == "__main__":
    main()
