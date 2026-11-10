#!/usr/bin/env python3
"""Resumable walk of /v2/positions for the Tokor wallet across statuses
(OPEN / CLOSED / REDEEMABLE). Raw pages saved per status for forensics.
Idempotent on re-run (resumes from the last saved pagination cursor).
"""
import glob
import json
import os
import time
import urllib.parse
import urllib.request

BASE = "https://data-api.polymarket.com/v2/positions"
USER = "0xa19cbababc312f9df185e49d7004c249ed1ade6b"
OUT_DIR = "data/raw/positions_pages"
UA = "tokor-engine/0.1"


def fetch(url: str, attempts: int = 5):
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return 200, resp.read().decode()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"fetch failed after {attempts}: {last}")


def walk(status: str):
    os.makedirs(f"{OUT_DIR}/{status.lower()}", exist_ok=True)
    saved = sorted(glob.glob(f"{OUT_DIR}/{status.lower()}/page_*.json"))
    page_no = len(saved)
    cursor = None
    if saved:
        pag = json.load(open(saved[-1]))["body"].get("pagination") or {}
        if pag.get("has_more") is False or not json.load(open(saved[-1]))["body"].get("data"):
            print(f"[{status}] already complete: {len(saved)} pages")
            return
        cursor = pag.get("next_cursor")
        if cursor in (None, "None", "LTE="):
            print(f"[{status}] terminal cursor present (complete)")
            return
        print(f"[{status}] resuming at page {page_no}")
    while True:
        params = {"user": USER, "status": status, "limit": "500"}
        if cursor:
            params["cursor"] = cursor
        url = BASE + "?" + urllib.parse.urlencode(params)
        st, body = fetch(url)
        page = json.loads(body)
        rows = page.get("data", [])
        path = f"{OUT_DIR}/{status.lower()}/page_{page_no:04d}.json"
        tmp = path + ".tmp"
        json.dump({"page_no": page_no, "url": url, "http_status": st, "body": page}, open(tmp, "w"))
        os.replace(tmp, path)
        page_no += 1
        pag = page.get("pagination") or {}
        nc = pag.get("next_cursor")
        hm = pag.get("has_more")
        if not rows or hm is False or nc in (None, "LTE="):
            print(f"[{status}] terminal: pages={page_no} rows_last={len(rows)} has_more={hm}")
            return
        cursor = nc
        time.sleep(0.25)


if __name__ == "__main__":
    for s in ("OPEN", "CLOSED", "REDEEMABLE"):
        walk(s)
    for s in ("OPEN", "CLOSED", "REDEEMABLE"):
        n = 0
        conds = set()
        legs = set()
        for p in sorted(glob.glob(f"{OUT_DIR}/{s.lower()}/page_*.json")):
            for r in json.load(open(p))["body"].get("data", []):
                n += 1
                conds.add(r.get("condition_id"))
                legs.add((r.get("condition_id"), r.get("outcome")))
        print(f"census {s}: rows={n} distinct_conditions={len(conds)} distinct_legs={len(legs)}")
