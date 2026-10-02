#!/usr/bin/env python3
"""
Fetch quote success rates from Loki for the last 7 days in UTC-aligned 6h
buckets and push them to Dune, replacing the table on every run.

Buckets are 00:00-06:00, 06:00-12:00, 12:00-18:00, 18:00-24:00 UTC.
Only CLOSED buckets are included. A run at 13:00 UTC covers 12:00 UTC seven
days ago through 12:00 UTC today (28 buckets). The 12:00-18:00 bucket that
is still in progress is left out.

Nothing is stored locally. Each run rebuilds the full 7-day window from Loki
and overwrites the Dune table with it.
"""

import io
import os
import csv
import sys
import time
import argparse
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

STEP = 6 * 3600          # 21600s divides evenly into 86400, so epoch multiples
                         # land exactly on 00:00 / 06:00 / 12:00 / 18:00 UTC
LOOKBACK_DAYS = 7
BUCKETS = LOOKBACK_DAYS * 86400 // STEP   # 28 closed buckets
CHUNK_BUCKETS = 4        # one day per Loki request, so no single query has
                         # to scan the whole week and risk a timeout
# ---------------------------------------------------------------------------

# Quotes from these user public keys are excluded from every series.
EXCLUDED_USER_KEYS = [
    "11111111111111111111111111111111",
    "4kh9wtXijAgHQefb5rEyWMWBd1kCThPJnyk7xGJz4d6F",
    "EZa8LZhmz4jCcQqxtVz2zW2AUUkGQ1EkYyR4a5muQL3h",
    "EDHhLVHm7Cjyj8BSP3sVASkuZ1kAd5LMPD4USRk2RsnT",
    "Jg4adAQQ4txGwHFZrCUUUPp8MkLqcMcEN3T6wgzSbPy",
    "4FSfK1bnE66hRF2KJZ7dZQndBZVVziQkrrdXDf853Wds",
    "42uMDScBpaXjr8o1yEiurGxNWVGvEWDudfgG8Y7khpRQ",
    "Eps3ZgQxmaynGJpWZcJ8xGyWxtabo83rys6ubHKniKZk",
    "GJsPEgv1ZQSUvZWBnWAzqiK1vfg8JkVWhhQUCxbhLkcM",
    "FkHxUC6PN8gEaJmtqQmmgTkUfFsj9T6GaoYepmH8R7Y8",
]

BASE = (
    '{container_name="haze-aggregator-api"} '
    '|= `"app_id":"120"` '
    + "".join(f"!= `userPublicKey={k}` " for k in EXCLUDED_USER_KEYS)
    + '| json | fields_fields_app_id="120"'
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


def query_chunk(start_ts, end_ts, token):
    """One query_range call. Evaluation points are start_ts, start_ts+6h, ...
    end_ts, each counting the 6h that end at that point."""
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
    return raw


def fetch(token):
    now = int(time.time())
    end_ts = now - (now % STEP)                 # last CLOSED boundary
    first_ts = end_ts - (BUCKETS - 1) * STEP    # end of the oldest bucket
    expected = list(range(first_ts, end_ts + 1, STEP))

    print(f"window {rfc3339(first_ts - STEP)} -> {rfc3339(end_ts)} "
          f"({len(expected)} closed 6h buckets)")

    raw = {}
    for i in range(0, len(expected), CHUNK_BUCKETS):
        chunk = expected[i:i + CHUNK_BUCKETS]
        print(f"  querying {rfc3339(chunk[0] - STEP)} -> {rfc3339(chunk[-1])}")
        raw.update(query_chunk(chunk[0], chunk[-1], token))

    unaligned = [t for t in raw if t % STEP != 0]
    if unaligned:
        sys.exit(f"timestamps not on 6h boundaries: {[rfc3339(t) for t in unaligned]}")

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


def to_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def upload_to_dune(csv_text, api_key):
    """Full-table replace: Dune ends up holding exactly the last 7 days."""
    payload = {
        "table_name": DUNE_TABLE_NAME,
        "description": ("Quote success rate by 6h UTC bucket, rolling last "
                        f"{LOOKBACK_DAYS} days (haze-aggregator-api, app_id 120)"),
        "data": csv_text,
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
    ap.add_argument("--dry-run", action="store_true",
                    help="print the CSV instead of uploading it to Dune")
    args, _ = ap.parse_known_args()

    loki_token = os.environ.get("GLC_TOKEN", "")
    dune_key = os.environ.get("DUNE_API_KEY", "")
    if not loki_token:
        sys.exit("GLC_TOKEN is not set")
    if not dune_key and not args.dry_run:
        sys.exit("DUNE_API_KEY is not set")

    rows = fetch(loki_token)
    csv_text = to_csv(rows)
    print(f"coverage: {rows[0]['window_start']} -> {rows[-1]['window_end']} UTC")

    if args.dry_run:
        print("dry run, not uploading:\n")
        sys.stdout.write(csv_text)
        return
    upload_to_dune(csv_text, dune_key)


if __name__ == "__main__":
    main()
