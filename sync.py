#!/usr/bin/env python3
"""
Fetch quote success rates from Loki in UTC-aligned 6h buckets, merge them
into a local CSV cache (deduplicated by window_end), and publish the full
cache to Dune.

Buckets are 00:00-06:00, 06:00-12:00, 12:00-18:00, 18:00-24:00 UTC.
Guaranteed no overlap and no gaps: the [6h] range equals the 6h step, and
both ends of the query are snapped to epoch multiples of 21600s.
"""

import os
import csv
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Already filled in for your stack. Secrets come from the environment.
# ---------------------------------------------------------------------------
LOKI_URL = "https://logs-prod-042.grafana.net"
LOKI_INSTANCE_ID = "1660827"

DUNE_TABLE_NAME = "haze_quote_success_6h"
DUNE_IS_PRIVATE = True   # requires a Dune Enterprise plan; silently
                         # stays public on lower tiers

CACHE_PATH = Path("data/buckets.csv")

STEP = 6 * 3600  # 21600s divides evenly into 86400, so epoch multiples
                 # land exactly on 00:00 / 06:00 / 12:00 / 18:00 UTC
# ---------------------------------------------------------------------------

BASE = (
    '{container_name="haze-aggregator-api"} '
    '|= `"app_id":"120"` '
    "!= `userPublicKey=11111111111111111111111111111111` "
    '| json | fields_fields_app_id="120"'
)

SERIES = [
    ("success_0_60",    "fields_fields_sec_until_close >= 0 | fields_fields_sec_until_close <= 60",    True),
    ("total_0_60",      "fields_fields_sec_until_close >= 0 | fields_fields_sec_until_close <= 60",    False),
    ("success_180_300", "fields_fields_sec_until_close >= 180 | fields_fields_sec_until_close <= 300", True),
    ("total_180_300",   "fields_fields_sec_until_close >= 180 | fields_fields_sec_until_close <= 300", False),
]

COLUMNS = [
    "window_start", "window_end",
    "success_0_60", "total_0_60", "pct_0_60",
    "success_180_300", "total_180_300", "pct_180_300",
]


def build_expr():
    return "\nor\n".join(
        'label_replace(sum(count_over_time({} | {}{} [6h])), "series", "{}", "", "")'.format(
            BASE, window, ' | fields_status="200"' if success else "", name
        )
        for name, window, success in SERIES
    )


def fmt(ts):
    """Dune parses this shape as a timestamp reliably."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def rfc3339(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(lookback_buckets, token):
    now = int(time.time())
    end_ts = now - (now % STEP)                          # last CLOSED boundary
    start_ts = end_ts - (lookback_buckets - 1) * STEP    # first eval point

    print(f"querying {rfc3339(start_ts)} -> {rfc3339(end_ts)} "
          f"({lookback_buckets} buckets, step 6h)")

    resp = requests.get(
        f"{LOKI_URL.rstrip('/')}/loki/api/v1/query_range",
        params={
            "query": build_expr(),
            "start": rfc3339(start_ts),
            "end": rfc3339(end_ts),
            "step": "6h",        # must equal the [6h] range
        },
        auth=(LOKI_INSTANCE_ID, token),
        timeout=300,
    )
    if resp.status_code != 200:
        sys.exit(f"loki {resp.status_code}: {resp.text[:500]}")

    body = resp.json()
    if body.get("status") != "success":
        sys.exit(f"loki returned: {body}")

    raw = {}
    for stream in body["data"]["result"]:
        name = stream["metric"].get("series", "unlabeled")
        for ts, val in stream["values"]:
            raw.setdefault(int(float(ts)), {})[name] = int(float(val))

    unaligned = [t for t in raw if t % STEP != 0]
    if unaligned:
        sys.exit(f"timestamps not on 6h boundaries: {[rfc3339(t) for t in unaligned]}")

    expected = list(range(start_ts, end_ts + 1, STEP))
    empty = [t for t in expected if t not in raw]
    if empty:
        print(f"note: {len(empty)} bucket(s) returned no samples: "
              f"{[rfc3339(t) for t in empty]}")

    rows = []
    for ts in expected:
        v = raw.get(ts, {})
        row = {
            "window_start": fmt(ts - STEP),
            "window_end": fmt(ts),
        }
        for name, _, _ in SERIES:
            row[name] = v.get(name, 0)
        for tag in ("0_60", "180_300"):
            s, t = row[f"success_{tag}"], row[f"total_{tag}"]
            row[f"pct_{tag}"] = round(s / t * 100, 4) if t else ""
        rows.append(row)

    print(f"fetched {len(rows)} buckets")
    return rows


def load_cache():
    if not CACHE_PATH.exists():
        return {}
    with CACHE_PATH.open(newline="") as fh:
        return {r["window_end"]: r for r in csv.DictReader(fh)}


def merge(cache, fresh):
    """Key on window_end. Refetched buckets overwrite, so late-arriving
    logs correct earlier counts instead of creating a duplicate row."""
    added, updated = 0, 0
    for row in fresh:
        key = row["window_end"]
        if key not in cache:
            added += 1
        elif any(str(cache[key].get(c, "")) != str(row[c]) for c in COLUMNS):
            updated += 1
        cache[key] = row
    print(f"cache: {added} new, {updated} corrected, {len(cache)} total rows")
    return added + updated


def write_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(cache.values(), key=lambda r: r["window_end"])
    with CACHE_PATH.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        for row in ordered:
            w.writerow({c: row.get(c, "") for c in COLUMNS})
    return ordered


def upload_to_dune(api_key):
    """Full-table replace. Idempotent by construction: Dune always ends up
    holding exactly what is in the CSV, so re-running never duplicates."""
    payload = {
        "table_name": DUNE_TABLE_NAME,
        "description": "Quote success rate by 6h UTC bucket (haze-aggregator-api, app_id 120)",
        "data": CACHE_PATH.read_text(),
        "is_private": DUNE_IS_PRIVATE,
    }
    resp = requests.post(
        "https://api.dune.com/api/v1/uploads/csv",
        headers={"X-Dune-Api-Key": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=180,
    )
    if resp.status_code != 200:
        sys.exit(f"dune {resp.status_code}: {resp.text[:500]}")
    print(f"dune: uploaded -> {resp.json()}")

    # Confirm what Dune actually stored. On non-Enterprise plans an
    # is_private=True request still results in a public table.
    check = requests.get(
        "https://api.dune.com/api/v1/uploads",
        headers={"X-Dune-Api-Key": api_key},
        params={"limit": 50},
        timeout=60,
    )
    if check.status_code == 200:
        for t in check.json().get("tables", []):
            if DUNE_TABLE_NAME in t.get("full_name", ""):
                state = "PRIVATE" if t.get("is_private") else "PUBLIC"
                print(f"dune: {t['full_name']} is {state}")
                if DUNE_IS_PRIVATE and not t.get("is_private"):
                    print("warning: requested private but table is public - "
                          "private uploads require a Dune Enterprise plan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int,
                    default=int(os.environ.get("LOOKBACK_BUCKETS", "3")),
                    help="how many closed 6h buckets to re-query each run")
    ap.add_argument("--no-upload", action="store_true",
                    help="refresh the CSV cache but skip Dune")
    args, _ = ap.parse_known_args()

    loki_token = os.environ.get("GLC_TOKEN", "")
    dune_key = os.environ.get("DUNE_API_KEY", "")
    if not loki_token:
        sys.exit("GLC_TOKEN is not set")
    if not dune_key and not args.no_upload:
        sys.exit("DUNE_API_KEY is not set")

    fresh = fetch(args.lookback, loki_token)
    cache = load_cache()
    changes = merge(cache, fresh)
    ordered = write_cache(cache)

    print(f"coverage: {ordered[0]['window_start']} -> {ordered[-1]['window_end']} UTC")

    if args.no_upload:
        print("skipping dune upload (--no-upload)")
        return
    if changes == 0:
        print("no changes, skipping dune upload")
        return
    upload_to_dune(dune_key)


if __name__ == "__main__":
    main()
