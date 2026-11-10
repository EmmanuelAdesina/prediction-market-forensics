#!/usr/bin/env python3
"""Resumable, polite walk of the unfiltered /v2/activity feed for the Tokor
wallet. Saves every raw page to data/raw/activity_pages/page_NNNN.json with the
request URL for forensics. Idempotent on re-run: resumes after the last saved
page by re-using its stored cursor chain (walk is cursor-ordered, so pages are
deterministic; identical URLs skip the fetch and stop when all pages exist).
"""
import glob
import json
import os
import sys
import time
import urllib.parse
import urllib.request

BASE = "https://data-api.polymarket.com/v2/activity"
USER = "0xa19cbababc312f9df185e49d7004c249ed1ade6b"
OUT_DIR = "data/raw/activity_pages"
UA = "tokor-engine/0.1"


def fetch(url: str, attempts: int = 5):
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return 200, resp.read().decode()
        except Exception as e:  # noqa: BLE001 - investigation script
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"fetch failed after {attempts}: {last}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    saved = sorted(glob.glob(f"{OUT_DIR}/page_*.json"))
    resume_from = None
    page_no = 0
    if saved:
        last = json.load(open(saved[-1]))
        page_no = len(saved)
        resume_from = last["body"].get("next_cursor")
        # if the last page already completed the walk, nothing to do
        pag = last["body"].get("pagination") or {}
        if pag.get("has_more") is False or not last["body"].get("data"):
            print("already complete:", len(saved), "pages")
            return
        cursor = pag.get("next_cursor")
        if cursor in (None, "LTE="):
            print("already complete (cursor terminal):", len(saved), "pages")
            return
        print(f"resuming from page {page_no} (cursor present)")
        page_no = len(saved)
    cursor = resume_from
    while True:
        params = {"user": USER, "limit": "500"}
        if cursor:
            params["cursor"] = cursor
        url = BASE + "?" + urllib.parse.urlencode(params)
        status, body = fetch(url)
        page = json.loads(body)
        rows = page.get("data", [])
        path = f"{OUT_DIR}/page_{page_no:04d}.json"
        tmp = path + ".tmp"
        json.dump({"page_no": page_no, "url": url, "http_status": status, "body": page}, open(tmp, "w"))
        os.replace(tmp, path)
        page_no += 1
        pag = page.get("pagination") or {}
        nc = pag.get("next_cursor")
        hm = pag.get("has_more")
        if not rows or hm is False or nc in (None, "LTE="):
            print(f"terminal: pages={page_no} last_rows={len(rows)} has_more={hm} next_cursor={nc}")
            break
        cursor = nc
        time.sleep(0.25)
    # summary across all saved pages
    from collections import Counter
    types = Counter()
    rows_total = 0
    ts_min, ts_max = 10**12, 0
    seen_urls = set()
    for p in sorted(glob.glob(f"{OUT_DIR}/page_*.json")):
        meta = json.load(open(p))
        body = meta["body"]
        seen_urls.add(meta["url"])
        for r in body.get("data", []):
            types[r.get("type")] += 1
            rows_total += 1
            ts = r.get("timestamp") or 0
            ts_min = min(ts_min, ts)
            ts_max = max(ts_max, ts)
    import datetime
    print("pages on disk:", page_no, "distinct urls:", len(seen_urls))
    print("rows:", rows_total)
    print("types:", dict(types))
    print("ts window:", datetime.datetime.fromtimestamp(ts_min, datetime.UTC).date(),
          "->", datetime.datetime.fromtimestamp(ts_max, datetime.UTC).date())


if __name__ == "__main__":
    main()
