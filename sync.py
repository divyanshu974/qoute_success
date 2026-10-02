#!/usr/bin/env python3
"""
Quote success rates for 5-minute prediction markets, from Loki, in
UTC-aligned 6h buckets over the last 7 days, pushed to Dune.

The logs have no reliable timeframe field (tf=5m only shows up inside
native_pm on failed requests), so 5m quotes are identified by mint:

  1. A saved Dune query (MINTS_QUERY_ID) lists every outcome mint of every
     5m market, with the market's close time.
  2. For each 6h bucket the script keeps only the mints whose market
     closes inside that bucket (plus a margin), and adds them to the Loki
     query as a line filter. A log line is counted only if it mentions one
     of those mints, as input or output mint.

Buckets are 00:00-06:00, 06:00-12:00, 12:00-18:00, 18:00-24:00 UTC, and
only CLOSED buckets are included: a run at 13:00 UTC covers 12:00 UTC
seven days ago through 12:00 UTC today (28 buckets). Nothing is stored
locally; every run overwrites the Dune table with the full window.
"""

import io
import os
import re
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

DUNE_API = "https://api.dune.com/api/v1"
DUNE_TABLE_NAME = "haze_quote_success_6h"
DUNE_IS_PRIVATE = True   # requires a Dune Enterprise plan; silently
                         # stays public on lower tiers

# Saved Dune query listing the 5m markets. It must return one row per
# outcome mint, covering at least the last 8 days, with these columns:
#   mint        varchar    base58 SPL mint address
#   close_time  timestamp  market close time, UTC
#               (or close_ts: bigint unix seconds, if you prefer)
MINTS_QUERY_ID = 8885536

STEP = 6 * 3600          # 21600s divides evenly into 86400, so epoch multiples
                         # land exactly on 00:00 / 06:00 / 12:00 / 18:00 UTC
LOOKBACK_DAYS = 7
BUCKETS = LOOKBACK_DAYS * 86400 // STEP   # 28 closed buckets

MAX_SEC_UNTIL_CLOSE = 300  # widest window counted below (180-300s)
MINT_MARGIN = 600          # slack, in seconds, when matching markets to a
                           # bucket. Extra 5m mints are harmless; missing
                           # ones would drop real quotes.
# ---------------------------------------------------------------------------

MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")   # base58

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
]

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


def fmt(ts):
    """Dune parses this shape as a timestamp reliably."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def rfc3339(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- Dune: 5m mints

def run_dune_query(query_id, api_key, wait_s=600):
    """Execute a saved Dune query and return all result rows."""
    h = {"X-Dune-Api-Key": api_key}
    r = requests.post(f"{DUNE_API}/query/{query_id}/execute", headers=h, json={}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"dune execute {r.status_code}: {r.text[:500]}")
    exec_id = r.json()["execution_id"]

    deadline = time.time() + wait_s
    while True:
        s = requests.get(f"{DUNE_API}/execution/{exec_id}/status", headers=h, timeout=60)
        if s.status_code != 200:
            sys.exit(f"dune status {s.status_code}: {s.text[:500]}")
        state = s.json().get("state", "")
        if state == "QUERY_STATE_COMPLETED":
            break
        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELED", "QUERY_STATE_CANCELLED",
                     "QUERY_STATE_EXPIRED", "QUERY_STATE_COMPLETED_PARTIAL"):
            # COMPLETED_PARTIAL means truncated, i.e. some mints would be missing.
            sys.exit(f"dune query {query_id} ended in {state}: {s.text[:500]}")
        if time.time() > deadline:
            sys.exit(f"dune query {query_id} still {state} after {wait_s}s")
        time.sleep(5)

    rows, offset = [], 0
    while True:
        res = requests.get(f"{DUNE_API}/execution/{exec_id}/results", headers=h,
                           params={"limit": 10000, "offset": offset}, timeout=120)
        if res.status_code != 200:
            sys.exit(f"dune results {res.status_code}: {res.text[:500]}")
        body = res.json()
        rows.extend(body["result"]["rows"])
        if body.get("next_offset") is None:
            return rows
        offset = body["next_offset"]


def parse_close(r):
    """Unix seconds from close_ts, or from a close_time timestamp. The Dune
    API returns timestamps like '2026-10-02 20:35:00.000 UTC'."""
    if r.get("close_ts") is not None:
        return int(float(r["close_ts"]))
    s = str(r.get("close_time") or "").strip().removesuffix(" UTC")
    dt = datetime.fromisoformat(s)          # raises ValueError if empty/odd
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # Dune timestamps are UTC
    return int(dt.timestamp())


def load_5m_markets(api_key):
    """[(close_ts, mint), ...] sorted by close time."""
    rows = run_dune_query(MINTS_QUERY_ID, api_key)
    markets, bad = set(), 0
    for r in rows:
        mint = str(r.get("mint") or "").strip()
        try:
            close_ts = parse_close(r)
        except (TypeError, ValueError):
            bad += 1
            continue
        if not MINT_RE.match(mint):
            bad += 1
            continue
        markets.add((close_ts, mint))
    if bad:
        print(f"warning: skipped {bad} Dune row(s) without a valid mint / close time")
    if not markets:
        cols = sorted(rows[0].keys()) if rows else []
        sys.exit(f"Dune query {MINTS_QUERY_ID} returned no usable rows (columns seen: "
                 f"{cols}); it must return `mint` (base58) and `close_time` (timestamp)")
    print(f"dune: {len(markets)} 5m-market mints from query {MINTS_QUERY_ID}")
    return sorted(markets)


def mints_for_bucket(markets, start_ts, end_ts):
    """Mints that can be quoted inside (start_ts, end_ts] with
    sec_until_close <= MAX_SEC_UNTIL_CLOSE, i.e. markets closing in
    (start_ts, end_ts + MAX_SEC_UNTIL_CLOSE], widened by MINT_MARGIN."""
    lo = start_ts - MINT_MARGIN
    hi = end_ts + MAX_SEC_UNTIL_CLOSE + MINT_MARGIN
    return sorted({m for c, m in markets if lo < c <= hi})


# --------------------------------------------------------------------------- Loki

def build_expr(mints):
    # Mints are plain base58, so the alternation needs no escaping. Loki
    # turns an alternation of literals into fast substring checks.
    base = (
        '{container_name="haze-aggregator-api"} '
        '|= `"app_id":"120"` '
        f'|~ `{"|".join(mints)}` '
        + "".join(f"!= `userPublicKey={k}` " for k in EXCLUDED_USER_KEYS)
        + '| json | fields_fields_app_id="120"'
    )
    return "\nor\n".join(
        'label_replace(sum(count_over_time({} | {}{} [6h])), "series", "{}", "", "")'.format(
            base, window, ' | fields_status="200"' if success else "", name
        )
        for name, window, success in SERIES
    )


def query_bucket(end_ts, mints, token):
    """Counts for the single 6h bucket ending at end_ts. Sent as a POST
    form body: with ~150 mints the query is far too long for a URL."""
    resp = requests.post(
        f"{LOKI_URL.rstrip('/')}/loki/api/v1/query_range",
        data={
            "query": build_expr(mints),
            "start": rfc3339(end_ts),
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


def fetch(token, markets):
    now = int(time.time())
    end_ts = now - (now % STEP)                 # last CLOSED boundary
    first_ts = end_ts - (BUCKETS - 1) * STEP    # end of the oldest bucket
    expected = list(range(first_ts, end_ts + 1, STEP))

    print(f"window {rfc3339(first_ts - STEP)} -> {rfc3339(end_ts)} "
          f"({len(expected)} closed 6h buckets)")

    raw, no_markets = {}, []
    for ts in expected:
        mints = mints_for_bucket(markets, ts - STEP, ts)
        if not mints:
            # An empty alternation would match every line, so never query.
            no_markets.append(ts)
            continue
        print(f"  querying {rfc3339(ts - STEP)} -> {rfc3339(ts)} ({len(mints)} mints)")
        raw.update(query_bucket(ts, mints, token))

    if no_markets:
        print(f"warning: {len(no_markets)} bucket(s) have no 5m markets in the Dune "
              f"query and were left at 0: {[rfc3339(t) for t in no_markets]}")

    unaligned = [t for t in raw if t % STEP != 0]
    if unaligned:
        sys.exit(f"timestamps not on 6h boundaries: {[rfc3339(t) for t in unaligned]}")

    empty = [t for t in expected if t not in raw and t not in no_markets]
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


# --------------------------------------------------------------------------- output

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
        "description": ("Quote success rate for 5m markets by 6h UTC bucket, rolling "
                        f"last {LOOKBACK_DAYS} days (haze-aggregator-api, app_id 120)"),
        "data": csv_text,
        "is_private": DUNE_IS_PRIVATE,
    }
    resp = requests.post(
        f"{DUNE_API}/uploads/csv",
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
        f"{DUNE_API}/uploads",
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
    if not dune_key:
        sys.exit("DUNE_API_KEY is not set (needed for the 5m mint list, even in --dry-run)")
    if not MINTS_QUERY_ID:
        sys.exit("MINTS_QUERY_ID is not set")

    markets = load_5m_markets(dune_key)
    rows = fetch(loki_token, markets)
    csv_text = to_csv(rows)
    print(f"coverage: {rows[0]['window_start']} -> {rows[-1]['window_end']} UTC")

    if args.dry_run:
        print("dry run, not uploading:\n")
        sys.stdout.write(csv_text)
        return

    # The upload replaces the whole table, so an all-zero week (usually a
    # filter that matches nothing) would wipe good data. Refuse instead.
    if not any(r["total_0_60"] or r["total_180_300"] for r in rows):
        sys.exit("every bucket is empty - check the Loki filters and the mint list. "
                 "Not uploading, so the Dune table keeps its last good data.")
    # Same for a week with traffic but zero successes: that is a success
    # filter matching nothing, and it would show up in Dune as a 0% outage.
    if not any(r["success_0_60"] or r["success_180_300"] for r in rows):
        sys.exit("every bucket has 0 successes - check the success filter. "
                 "Not uploading, so the Dune table keeps its last good data.")
    upload_to_dune(csv_text, dune_key)


if __name__ == "__main__":
    main()
