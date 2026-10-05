#!/usr/bin/env python3
"""
Top 100 userPublicKeys by row count over the last 2 days of closed 6h UTC
buckets, 5m markets only, with the same filters as the Dune sync.
Prints a table; uploads nothing. Self-contained: needs only `requests`.

Env: GLC_TOKEN (Loki), DUNE_API_KEY (to fetch the 5m-market mints).
"""

import os
import re
import sys
import time
import argparse
from datetime import datetime, timezone

import requests

LOKI_URL = "https://logs-prod-042.grafana.net"
LOKI_INSTANCE_ID = "1660827"

DUNE_API = "https://api.dune.com/api/v1"
MINTS_QUERY_ID = 8885536     # 5m markets: columns mint, close_time

STEP = 6 * 3600              # 6h buckets aligned to 00/06/12/18 UTC
MAX_SEC_UNTIL_CLOSE = 300    # widest window counted (180-300s)
MINT_MARGIN = 600            # slack when matching markets to a time slice
MIN_SLICE = 900              # never split a slice below 15 min

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

WINDOWS = {
    "0_60":    "fields_fields_sec_until_close >= 0 | fields_fields_sec_until_close <= 60",
    "180_300": "fields_fields_sec_until_close >= 180 | fields_fields_sec_until_close <= 300",
}

MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
USER_RE = r"userPublicKey=(?P<user_key>[1-9A-HJ-NP-Za-km-z]+)"
NO_KEY = "(no userPublicKey in log line)"


def rfc3339(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------- Dune: 5m mints

def dune_markets(api_key):
    """Run the Dune query fresh; return [(close_ts, mint), ...]."""
    h = {"X-Dune-Api-Key": api_key}
    r = requests.post(f"{DUNE_API}/query/{MINTS_QUERY_ID}/execute", headers=h, json={}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"dune execute {r.status_code}: {r.text[:500]}")
    exec_id = r.json()["execution_id"]

    deadline = time.time() + 600
    while True:
        s = requests.get(f"{DUNE_API}/execution/{exec_id}/status", headers=h, timeout=60)
        state = s.json().get("state", "") if s.status_code == 200 else f"HTTP {s.status_code}"
        if state == "QUERY_STATE_COMPLETED":
            break
        if state not in ("QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING") or time.time() > deadline:
            sys.exit(f"dune query {MINTS_QUERY_ID} ended in {state}: {s.text[:500]}")
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
            break
        offset = body["next_offset"]

    markets = set()
    for row in rows:
        mint = str(row.get("mint") or "").strip()
        try:
            dt = datetime.fromisoformat(str(row["close_time"]).strip().removesuffix(" UTC"))
        except (KeyError, ValueError):
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)      # Dune timestamps are UTC
        if MINT_RE.match(mint):
            markets.add((int(dt.timestamp()), mint))
    if not markets:
        sys.exit(f"dune query {MINTS_QUERY_ID} returned no usable mint / close_time rows")
    print(f"dune: {len(markets)} 5m-market mints")
    return sorted(markets)


def mints_for(markets, start, end):
    """Mints of markets that can be quoted in (start, end] with
    sec_until_close <= 300, widened by MINT_MARGIN on both sides."""
    lo, hi = start - MINT_MARGIN, end + MAX_SEC_UNTIL_CLOSE + MINT_MARGIN
    return sorted({m for c, m in markets if lo < c <= hi})


# ----------------------------------------------------------------- Loki

class SeriesLimit(Exception):
    pass


def build_expr(mints, window, dur):
    base = (
        '{container_name="haze-aggregator-api"} '
        '|= `"app_id":"120"` '
        f'|~ `{"|".join(mints)}` '
        + "".join(f"!= `userPublicKey={k}` " for k in EXCLUDED_USER_KEYS)
        + '| json | fields_fields_app_id="120"'
    )
    return (f"sum by (user_key, fields_status) (count_over_time("
            f"{base} | {window} | regexp `{USER_RE}` [{dur}s]))")


def loki_instant(query, at_ts, token):
    r = requests.post(                     # POST: the mint list is too long for a URL
        f"{LOKI_URL}/loki/api/v1/query",
        data={"query": query, "time": rfc3339(at_ts)},
        auth=(LOKI_INSTANCE_ID, token),
        timeout=300,
    )
    if r.status_code != 200:
        if "maximum number of series" in r.text:
            raise SeriesLimit()
        sys.exit(f"loki {r.status_code}: {r.text[:500]}")
    body = r.json()
    if body.get("status") != "success":
        sys.exit(f"loki returned: {body}")
    return body["data"]["result"]


def count_slice(start, end, markets, token, counts):
    """Add per-user counts for (start, end]. If Loki hits its series limit
    (too many distinct users), split the slice in half and retry."""
    mints = mints_for(markets, start, end)
    if not mints:
        return                             # an empty mint filter would match everything
    try:
        results = {tag: loki_instant(build_expr(mints, w, end - start), end, token)
                   for tag, w in WINDOWS.items()}
    except SeriesLimit:
        if end - start <= MIN_SLICE:
            sys.exit(f"too many users even in {rfc3339(start)} -> {rfc3339(end)}")
        mid = start + (end - start) // 2
        print(f"    too many users for one query, splitting {rfc3339(start)} -> {rfc3339(end)}")
        count_slice(start, mid, markets, token, counts)
        count_slice(mid, end, markets, token, counts)
        return

    for tag, series in results.items():
        for s in series:
            user = s["metric"].get("user_key") or NO_KEY
            n = int(float(s["value"][1]))
            c = counts.setdefault(user, {"total_0_60": 0, "success_0_60": 0,
                                         "total_180_300": 0, "success_180_300": 0})
            c[f"total_{tag}"] += n
            if s["metric"].get("fields_status") == "200":
                c[f"success_{tag}"] += n


# ----------------------------------------------------------------- output

def pct(part, whole):
    return f"{part / whole * 100:.1f}%" if whole else "-"


def report(counts, top):
    no_key = counts.pop(NO_KEY, None)
    rows = {u: c["total_0_60"] + c["total_180_300"] for u, c in counts.items()}
    no_key_rows = no_key["total_0_60"] + no_key["total_180_300"] if no_key else 0
    grand = sum(rows.values()) + no_key_rows
    ranked = sorted(counts, key=lambda u: (-rows[u], u))

    print(f"\n{'#':>4}  {'userPublicKey':<44}  {'rows':>8}  {'share':>6}  "
          f"{'tot_0_60':>8}  {'ok_0_60':>7}  {'tot_180_300':>11}  {'ok_180_300':>10}")
    for i, u in enumerate(ranked[:top], 1):
        c = counts[u]
        print(f"{i:>4}  {u:<44}  {rows[u]:>8}  {pct(rows[u], grand):>6}  "
              f"{c['total_0_60']:>8}  {pct(c['success_0_60'], c['total_0_60']):>7}  "
              f"{c['total_180_300']:>11}  {pct(c['success_180_300'], c['total_180_300']):>10}")

    print(f"\n{len(ranked)} distinct users, {grand} rows in total")
    print(f"top 10 users: {pct(sum(rows[u] for u in ranked[:10]), grand)} of rows; "
          f"top {min(top, len(ranked))}: {pct(sum(rows[u] for u in ranked[:top]), grand)}")
    if no_key_rows:
        print(f"{no_key_rows} rows ({pct(no_key_rows, grand)}) have no userPublicKey= in the log line")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="days back (max 7)")
    ap.add_argument("--top", type=int, default=100, help="how many users to print")
    args, _ = ap.parse_known_args()
    if not 1 <= args.days <= 7 or args.top < 1:
        sys.exit("--days must be 1-7 and --top at least 1")

    token, dune_key = os.environ.get("GLC_TOKEN", ""), os.environ.get("DUNE_API_KEY", "")
    if not token or not dune_key:
        sys.exit("GLC_TOKEN and DUNE_API_KEY must both be set")

    markets = dune_markets(dune_key)
    now = int(time.time())
    end = now - (now % STEP)                   # last closed 6h boundary
    start = end - args.days * 86400
    print(f"window {rfc3339(start)} -> {rfc3339(end)} UTC, 5m markets only")

    counts = {}
    for s in range(start, end, STEP):
        print(f"  querying {rfc3339(s)} -> {rfc3339(s + STEP)}")
        count_slice(s, s + STEP, markets, token, counts)

    if not counts:
        sys.exit("no rows matched in this window")
    report(counts, args.top)


if __name__ == "__main__":
    main()
