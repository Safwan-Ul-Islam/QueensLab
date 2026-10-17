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
python fetch.py --core          # the 5 that matter most
python fetch.py --all           # everything (~1.7M Queens rows)

python fetch.py --jobs          # the collection table, in the terminal
python crawl.py --dry-run       # which business websites Firecrawl would read (FIRECRAWL_API_KEY in .env)
python nta_profile.py           # rebuild the neighbourhood profile
python analyze.py               # terminal analyses
uvicorn app:app --reload        # the page, at localhost:8000
```

## Deploy

Render blueprint is in `render.yaml` — free plan, Python, `uvicorn app:app`.

## Storage

```
gs://queenslab-research-yourteam-2026/raw/*.parquet   ← the store (GCP project: queens-data)
gs://…/raw/_fetch_log.parquet                         ← row counts + when each table was pulled
raw/*.parquet in this repo                            ← the SMALL tables only: fallback if the bucket is unreachable
queens.duckdb                                         ← never stored; rebuilt on every boot
```

On boot the app rebuilds its database **from the bucket**. Small tables are
copied in. The big ones — 311, DOB permits, PLUTO, subway (over 250k rows or
20 MB, judged from each file's own footer) — become **views over their Parquet
in the bucket**: nothing is copied on boot, and a query reads only the columns
it names. If the bucket is unreachable — key
missing, wrong, or revoked — it logs why and falls back to the small tables'
Parquet committed here, so the page never comes up empty.

The big sources are **pulled on a laptop** (`python fetch.py --only nyc_311`);
the free server would run out of memory or fall asleep partway, so its `/fetch`
refuses anything tagged `local` in `sources.py`. 311 and subway come down one calendar month at a time, and a run
that dies picks up where it stopped.

Every `fetch.py` save also pushes to the bucket, so data fetched on the live
service (`POST /fetch/<key>`) now survives restarts.

Fetch dates live in one small object per table (`raw/_log/<key>.parquet`), so
two machines uploading at once can't drop each other's dates. `--where` and
`--cap` are **trials**: saved locally as `<key>__trial`, never uploaded.

## Monitoring — the Collection table

Every pull and every upload is a **job**: queued → running → succeeded or
failed, with rows downloaded, started, last activity, finished, the GCS upload
result and the error. Shown on the page (Collection), at `/api/jobs`, and by
`python fetch.py --jobs`.

- Kept in `jobs.sqlite` (SQLite, so a DuckDB write lock can never hide it) and
  mirrored per machine to `gs://…/jobs/<machine>.parquet`, so the page shows
  laptop pulls and Render's history survives restarts.
- A job that dies silently (kill -9, out of memory) is closed from outside: by
  its runner's exit code, or because its process is gone. One whose machine
  went quiet shows as **stalled**, never as running.
- "Succeeded" means saved; the GCS column says separately whether it reached
  the bucket ("not uploaded" → `python fetch.py --push`).

## Neighbourhoods — `nta_profile.py`

Every source side by side for each of the 82 neighbourhoods (2020 NTAs):
residents and income (ACS 2023 via the city's tract→NTA table), licensed
businesses, restaurants and new ones, food stores, liquor licences, 311,
subway riders, storefront vacancy, permits, sale prices, retail floor area.
Records are placed **by their coordinates** inside the NTA polygons — never by
the NTA codes some sources carry, which mix the 2010 and 2020 schemes. Built on
a laptop (about a million points), saved under `derived/` and the bucket's
`derived/`, and only displayed by the page.

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

`queens.duckdb`, `jobs.sqlite` and the big sources' Parquet are gitignored; all
regenerate from `fetch.py`. `derived/` (the profile, small) is committed.

## Already corrected

`sla_liquor.originalissuedate` was described as business age. It is not — it
equals `lastissuedate` on every row and starts in 2021, so it is the current
licence **term**. An analysis claiming 82% of Queens bars were under two years
old is what exposed it. **Check a field's values before trusting its name.**

**Paging needs a total order.** Sorting pages by a column with ties made tied
rows swap places between requests: 2,682 storefront rows arrived twice and as
many never arrived, while the row count still matched. Every pull now breaks
ties with Socrata's `:id`. (The city's files also contain genuine identical
rows, 59 in DOHMH; those are kept.)

**Column detection must read every row.** Socrata omits empty fields, and
DuckDB looked at only the first 20,480 rows, so a column first seen later was
never created. In PLUTO that cost one column (`zonedist4`, set on 4 lots).
`fetch.py` now scans the whole file. PLUTO's 14 other absent columns are empty
for every Queens lot — the API never sends them, so they are not missing.

**A read-only connection is not a sandbox.** The public SQL box could read files
on the server — including the environment holding the bucket key — and
`SELECT 1; COPY …` wrote files. Now it takes exactly one SELECT, by DuckDB's own
parser, on a connection with no local disk and no web access, capped at 200 MB
and 30 seconds, and refusing `query()`, `query_table()` and the secret
functions wherever they appear in the parse tree. A text filter on
"duckdb_secrets" had been bypassed with `query('… duckdb_' || 'secrets()')`.

**Never render a URL parameter as HTML.** `GET /?result=<script>…` put a
stranger's script on the page, because the page function doubled as the route.
