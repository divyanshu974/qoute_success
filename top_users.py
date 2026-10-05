#!/usr/bin/env python3
"""
Fetch ALL log lines matching the Grafana Logs Drilldown view (not capped at
Grafana's 5,000 line limit) and save them raw to out/logs.txt: one log per
row, newest first like Grafana, as "<UTC timestamp><TAB><raw line>". The raw
line is written exactly as Loki stores it.

Filters (same as the Drilldown screenshot):
  container_name = haze-aggregator-api
  include  "app_id":"120"
  exclude  userPublicKey=11111111
  include  tf=5m
  exclude  "status":"200"
  field    fields_fields_sec_until_close > 0

Env: GLC_TOKEN. Needs only `requests`.
"""

import os
import sys
import argparse
from datetime import datetime, timezone

import requests

LOKI_URL = "https://logs-prod-042.grafana.net"
LOKI_INSTANCE_ID = "1660827"

QUERY = (
    '{container_name="haze-aggregator-api"} '
    '|= `"app_id":"120"` '
    '!= `userPublicKey=11111111` '
    '|= `tf=5m` '
    '!= `"status":"200"` '
    '| json '
    '| fields_fields_sec_until_close > 0'
)

PAGE = 5000          # Grafana Cloud's default max lines per request
OUT_DIR = "out"


def to_ns(s):
    dt = datetime.fromisoformat(s.strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 10**9 + dt.microsecond * 1000


def ns_to_str(ns):
    dt = datetime.fromtimestamp(ns // 10**9, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ns % 10**9:09d}"


def fetch_all(start_ns, end_ns, token):
    """Page forward through [start, end). Each page starts at the newest
    timestamp of the previous one, and lines already fetched at exactly that
    timestamp are skipped, so nothing is lost or duplicated at page edges."""
    entries, seen_at_cursor, cursor = [], set(), start_ns
    while True:
        r = requests.get(
            f"{LOKI_URL}/loki/api/v1/query_range",
            params={"query": QUERY, "start": str(cursor), "end": str(end_ns),
                    "limit": PAGE, "direction": "forward"},
            auth=(LOKI_INSTANCE_ID, token),
            timeout=300,
        )
        if r.status_code != 200:
            sys.exit(f"loki {r.status_code}: {r.text[:500]}")
        body = r.json()
        if body.get("status") != "success":
            sys.exit(f"loki returned: {str(body)[:500]}")

        page = sorted((int(ts), line)
                      for stream in body["data"]["result"]
                      for ts, line in stream["values"])
        new = [e for e in page if e not in seen_at_cursor]
        entries.extend(new)
        print(f"  fetched {len(entries)} lines (up to {ns_to_str(page[-1][0]) if page else '-'})")

        if len(page) < PAGE:
            return entries
        last_ts = page[-1][0]
        at_last = {e for e in page if e[0] == last_ts}
        if not new:
            # More than PAGE lines share one nanosecond; step past it.
            print(f"warning: over {PAGE} lines at {ns_to_str(last_ts)}, some may be skipped")
            cursor, seen_at_cursor = last_ts + 1, set()
        elif last_ts == cursor:
            seen_at_cursor |= at_last
        else:
            cursor, seen_at_cursor = last_ts, at_last


def save(entries):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(f"{OUT_DIR}/logs.txt", "w", encoding="utf-8", newline="\n") as fh:
        for ts, line in sorted(entries, reverse=True):      # newest first
            fh.write(f"{ns_to_str(ts)}\t{line}\n")
    print(f"saved {len(entries)} raw log lines to {OUT_DIR}/logs.txt")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-01T20:21:00Z", help="UTC, inclusive")
    ap.add_argument("--end", default="2026-10-02T18:38:00Z", help="UTC, exclusive")
    args, _ = ap.parse_known_args()

    token = os.environ.get("GLC_TOKEN", "")
    if not token:
        sys.exit("GLC_TOKEN is not set")
    start_ns, end_ns = to_ns(args.start), to_ns(args.end)
    if start_ns >= end_ns:
        sys.exit("--start must be before --end")

    print(f"query: {QUERY}")
    print(f"range: {ns_to_str(start_ns)} -> {ns_to_str(end_ns)} UTC")
    entries = fetch_all(start_ns, end_ns, token)
    if not entries:
        sys.exit("no lines matched")
    save(entries)


if __name__ == "__main__":
    main()
