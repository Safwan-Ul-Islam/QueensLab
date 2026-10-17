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

**Note on the free plan:** the disk is wiped on restart, so data is re-fetched
rather than stored. Trigger a pull with `POST /fetch/core`, or move to a paid
plan with a persistent disk if you want it to survive.

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
