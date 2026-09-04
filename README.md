# haze quote success → Dune

Pulls quote success rates from Grafana Cloud Loki in UTC-aligned 6-hour
buckets, keeps them in a deduplicated CSV cache, and publishes to Dune.

Runs automatically at **00:10, 06:10, 12:10 and 18:10 UTC**.

## Setup

Two repository secrets. Nothing else needs changing.

`Settings → Secrets and variables → Actions → New repository secret`

| Secret | Where to get it |
|---|---|
| `GLC_TOKEN` | grafana.com → Access Policies → token with `logs:read` (starts `glc_`) |
| `DUNE_API_KEY` | dune.com → Settings → API keys |

Then `Actions → Sync Loki buckets to Dune → Run workflow` to verify.

Everything else (Loki host `logs-prod-042.grafana.net`, instance id
`1660827`, table name) is already set in `sync.py`.

## What it produces

`data/buckets.csv`, one row per 6h window, committed back to the repo:

| column | meaning |
|---|---|
| `window_start`, `window_end` | UTC bounds of the bucket |
| `success_0_60`, `total_0_60`, `pct_0_60` | quotes closing within 0–60s |
| `success_180_300`, `total_180_300`, `pct_180_300` | quotes closing within 180–300s |

Dune receives the same table as `dune.<your_username>.dataset_haze_quote_success_6h`.

## No overlap, no gaps, no duplicates

Three separate guarantees:

**Windows tile exactly.** The LogQL range `[6h]` equals the query step
`6h`, so each evaluation begins where the previous ended. A step larger
than the range would skip data; a smaller one would double-count it.

**Boundaries are real UTC boundaries.** 21600 divides evenly into 86400,
so `now - (now % 21600)` always lands on 00:00 / 06:00 / 12:00 / 18:00.
`end` is the last *closed* boundary, so partial buckets are never queried.

**Rows are keyed on `window_end`.** Re-fetching a bucket overwrites the
existing row rather than appending. The Dune upload replaces the whole
table each time, so duplicates cannot accumulate on either side.

A point stamped `06:00` covers `00:00–06:00`. `window_start` is written
out explicitly so nothing has to be inferred.

## Missed or delayed runs

`LOOKBACK_BUCKETS` defaults to 3, meaning every run re-queries the last
three closed buckets and merges them. Two consecutive failed runs
therefore self-heal on the next success. It also picks up logs that
arrived late and corrects earlier counts.

GitHub cron is best-effort and can be delayed under load. The overlapping
lookback is what makes that harmless.

To backfill, run the workflow manually with a larger `lookback`
(4 = 1 day, 28 = 7 days, 120 = 30 days).

## Cost

Roughly 4 GiB scanned per bucket across the four series, so a default run
is around 12 GiB and a day is around 48 GiB. Grafana Cloud bills on
queried bytes — check your plan before raising `LOOKBACK_BUCKETS` or the
schedule frequency. A 120-bucket backfill is a ~480 GiB query.

## Local run

```bash
pip install -r requirements.txt
export GLC_TOKEN="glc_..."
export DUNE_API_KEY="..."

python sync.py --lookback 8 --no-upload   # refresh CSV only
python sync.py --lookback 8               # and publish to Dune
```

`--no-upload` is the safe way to check output without touching Dune.

## Note on the Dune endpoint

Upload uses `POST /api/v1/uploads/csv` (the current path; the older
`/api/v1/table/upload/csv` is legacy). It replaces the table on every
call, so re-running never duplicates.

`DUNE_IS_PRIVATE = True` is set in `sync.py`, but **private uploads
require a Dune Enterprise plan**. On lower tiers the request succeeds and
the table stays public. After each upload the script reads back
`GET /api/v1/uploads` and prints whether Dune actually stored the table
as PRIVATE or PUBLIC, warning if it doesn't match what was requested.

If you're not on Enterprise and the data must not be public, don't
publish it here — keep `data/buckets.csv` in a private GitHub repo and
run with `--no-upload`.

Privacy can also be flipped after upload in the web app:
Settings → Data → three dots next to the dataset → "make table private"
(same Enterprise requirement applies).
