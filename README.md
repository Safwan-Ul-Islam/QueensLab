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

## Storage — how the data survives Render's free plan

The free disk is wiped on every restart, so the data **ships with the code**:

```
raw/*.parquet        ← committed to git (~19 MB, zstd-compressed)
raw/_manifest.json   ← committed: row counts + when each table was fetched
queens.duckdb        ← NOT committed; rebuilt from the Parquet on every boot (~5s)
```

**To refresh the data:**

```bash
python fetch.py --core            # or --only <table>, or --all
git add raw/ && git commit -m "Refresh Queens data" && git push
```

Then redeploy on Render. (The service was created from a public repo URL, which
has no webhook — pushes do **not** auto-deploy. Use *Manual Deploy* in the
dashboard.)

`POST /fetch/<key>` still works on the live service, but anything it pulls is
lost at the next restart. Fetch locally and commit instead.

**When this stops being enough:** GitHub rejects files over 100 MB, and the repo
grows with every refresh. If the data outgrows that, move the Parquet to object
storage (Cloudflare R2 or S3) and read it remotely — DuckDB can query Parquet
over HTTPS directly.

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
