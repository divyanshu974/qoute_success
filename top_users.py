#!/usr/bin/env python3
"""
Top userPublicKeys by row count over the last N days (default 2) of closed
6h UTC buckets. Prints the top 100; nothing is uploaded anywhere.

Rows are counted with exactly the same filters as sync.py (it imports them):
app_id 120, the excluded user keys, the 5m-market mints from the Dune query,
and the 0-60s / 180-300s sec_until_close windows. So "rows" here are the
same log lines that make up total_0_60 + total_180_300 in the Dune table.

Needs sync.py (the version with base_selector) in the same folder.
"""

import os
import sys
import time
import argparse

import requests

import sync

USER_RE = r"userPublicKey=(?P<user_key>[1-9A-HJ-NP-Za-km-z]+)"
NO_KEY = "(no userPublicKey in log line)"
MIN_SLICE = 900   # never split a time slice below 15 minutes

# {"0_60": <sec_until_close filter>, "180_300": ...}, taken from sync.SERIES
WINDOWS = {name.removeprefix("total_"): window
           for name, window, success in sync.SERIES if not success}


class SeriesLimit(Exception):
    """Loki refused the query: more distinct users than its series limit."""


def build_expr(mints, window, dur):
    return (
        "sum by (user_key, fields_status) (count_over_time("
        f"{sync.base_selector(mints)} | {window} | regexp `{USER_RE}` [{dur}s]))"
    )


def loki_instant(query, at_ts, token):
    # POST: with the mint list the query is far too long for a URL.
    r = requests.post(
        f"{sync.LOKI_URL.rstrip('/')}/loki/api/v1/query",
        data={"query": query, "time": sync.rfc3339(at_ts)},
        auth=(sync.LOKI_INSTANCE_ID, token),
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
    """Add per-user counts for (start, end] into counts. If Loki hits its
    series limit, split the slice in half and retry; the halves add up to
    exactly the same counts."""
    if markets is None:
        mints = None
    else:
        mints = sync.mints_for_bucket(markets, start, end)
        if not mints:
            return  # no 5m markets here; an empty mint filter would match everything

    try:
        results = {tag: loki_instant(build_expr(mints, window, end - start), end, token)
                   for tag, window in WINDOWS.items()}
    except SeriesLimit:
        if end - start <= MIN_SLICE:
            sys.exit(f"still too many users in {sync.rfc3339(start)} -> {sync.rfc3339(end)}; "
                     "Loki's series limit is too low for a per-user breakdown")
        mid = start + (end - start) // 2
        print(f"    too many users for one query, splitting "
              f"{sync.rfc3339(start)} -> {sync.rfc3339(end)} in half")
        count_slice(start, mid, markets, token, counts)
        count_slice(mid, end, markets, token, counts)
        return

    for tag, series in results.items():
        for s in series:
            user = s["metric"].get("user_key") or NO_KEY
            n = int(float(s["value"][1]))
            c = counts.setdefault(user, {f"{k}_{t}": 0 for t in WINDOWS
                                         for k in ("total", "success")})
            c[f"total_{tag}"] += n
            if s["metric"].get("fields_status") == "200":
                c[f"success_{tag}"] += n


def pct(part, whole):
    return f"{part / whole * 100:.1f}%" if whole else "-"


def report(counts, top):
    no_key = counts.pop(NO_KEY, None)
    rows = {u: c["total_0_60"] + c["total_180_300"] for u, c in counts.items()}
    grand = sum(rows.values()) + (no_key["total_0_60"] + no_key["total_180_300"] if no_key else 0)
    ranked = sorted(counts, key=lambda u: (-rows[u], u))

    print(f"\n{'#':>4}  {'userPublicKey':<44}  {'rows':>8}  {'share':>6}  "
          f"{'tot_0_60':>8}  {'ok_0_60':>7}  {'tot_180_300':>11}  {'ok_180_300':>10}")
    for i, u in enumerate(ranked[:top], 1):
        c = counts[u]
        print(f"{i:>4}  {u:<44}  {rows[u]:>8}  {pct(rows[u], grand):>6}  "
              f"{c['total_0_60']:>8}  {pct(c['success_0_60'], c['total_0_60']):>7}  "
              f"{c['total_180_300']:>11}  {pct(c['success_180_300'], c['total_180_300']):>10}")

    shown = sum(rows[u] for u in ranked[:top])
    top10 = sum(rows[u] for u in ranked[:10])
    print(f"\n{len(ranked)} distinct users, {grand} rows in total "
          f"(ok_ columns are each user's success rate in that window)")
    print(f"top 10 users: {pct(top10, grand)} of all rows; "
          f"top {min(top, len(ranked))}: {pct(shown, grand)}")
    if no_key:
        n = no_key["total_0_60"] + no_key["total_180_300"]
        print(f"{n} rows ({pct(n, grand)}) have no userPublicKey= in the log line; "
              "the user exclusions cannot apply to those")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="days back, in closed 6h buckets")
    ap.add_argument("--top", type=int, default=100, help="how many users to print")
    ap.add_argument("--all-markets", action="store_true",
                    help="count every market, not only 5m (skips the Dune mint list)")
    args, _ = ap.parse_known_args()

    token = os.environ.get("GLC_TOKEN", "")
    dune_key = os.environ.get("DUNE_API_KEY", "")
    if not token:
        sys.exit("GLC_TOKEN is not set")
    if args.days < 1 or args.top < 1:
        sys.exit("--days and --top must be at least 1")

    if args.all_markets:
        markets = None
    else:
        if not dune_key:
            sys.exit("DUNE_API_KEY is not set (needed for the 5m mint list)")
        if args.days > 7:
            sys.exit("the Dune mint query only covers the last 8 days; use --days 7 or less")
        markets = sync.load_5m_markets(dune_key)

    now = int(time.time())
    end = now - (now % sync.STEP)            # last CLOSED 6h boundary
    start = end - args.days * 86400
    print(f"window {sync.rfc3339(start)} -> {sync.rfc3339(end)} UTC, "
          f"{'all markets' if markets is None else '5m markets only'}")

    counts = {}
    for s in range(start, end, sync.STEP):
        print(f"  querying {sync.rfc3339(s)} -> {sync.rfc3339(s + sync.STEP)}")
        count_slice(s, s + sync.STEP, markets, token, counts)

    if not counts:
        sys.exit("no rows matched in this window")
    report(counts, args.top)


if __name__ == "__main__":
    main()
