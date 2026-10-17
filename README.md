# QueensLab

Queens business and demographic research. **A standalone project** — its own repo,
its own Render service, its own store. It has no connection to BizGenome.

## Why it is separate

BizGenome's rule is *"scraping never touches the model."* Keeping this in a
different repo and a different service makes that **structural** rather than a
habit: there is no shared database, no shared environment, and
**deliberately no `DATABASE_URL`** in `render.yaml`. Nothing here can reach
BizGenome's production data even by mistake.

When something researched here earns its way into the product, it gets a
proper licensed collector over there. It is never copied across.

## Run it

```bash
pip install -r requirements.txt

python fetch.py --list          # 16 sources available
python fetch.py --core          # the 6 that matter most
python fetch.py --all           # everything (~1.7M Queens rows)

python analyze.py               # terminal analyses
uvicorn app:app --reload        # the page, at localhost:8000
```

## Deploy

Render blueprint is in `render.yaml` — free plan, Python, `uvicorn app:app`.

## Storage

```
gs://queenslab-research-yourteam-2026/raw/*.parquet   ← the store (GCP project: queens-data)
gs://…/raw/_fetch_log.parquet                         ← row counts + when each table was pulled
raw/*.parquet in this repo                            ← fallback if the bucket is unreachable
queens.duckdb                                         ← never stored; rebuilt on every boot
```

On boot the app rebuilds its database **from the bucket**. If the bucket is
unreachable — key missing, wrong, or revoked — it logs why and falls back to the
Parquet committed here, so the page never comes up empty.

Every `fetch.py` save also pushes to the bucket, so data fetched on the live
service (`POST /fetch/<key>`) now survives restarts.

### Credentials

DuckDB talks to GCS directly with an **HMAC key** — no Google SDK, no `gcloud`.
Three variables, in `.env` locally (gitignored) and in Render's Environment tab:

```
GCS_BUCKET=queenslab-research-yourteam-2026
GCS_HMAC_KEY_ID=GOOG1E…
GCS_HMAC_SECRET=…
```

The key belongs to a service account scoped to **this one bucket**. It cannot
reach BizGenome's database or anything else in Google Cloud.

### Commands

```bash
python fetch.py --push            # upload everything already pulled
python fetch.py --core            # fetch; each table lands locally AND in the bucket
curl https://queenslab.onrender.com/api/status   # "loaded_from" = where this boot's data really came from
```

## Files

| | |
|---|---|
| `sources.py` | every source declared in one place — one entry adds a source |
| `fetch.py` | pages Socrata, writes Parquet, loads DuckDB, resume-safe |
| `analyze.py` | terminal analyses + `--sql` escape hatch |
| `app.py` | the web page and a small JSON API |
| `SOURCES.md` | **every link** — free sources and scrapers |

`raw/` and `queens.duckdb` are gitignored; both regenerate from `fetch.py`.

## Already corrected once

`sla_liquor.originalissuedate` was described as business age. It is not — it
equals `lastissuedate` on every row and starts in 2021, so it is the current
licence **term**. An analysis claiming 82% of Queens bars were under two years
old is what exposed it. **Check a field's values before trusting its name.**
