"""QueensLab — a standalone page for Queens research.

Completely separate from BizGenome: its own repo, its own Render service, its
own store. There is NO DATABASE_URL here and there must never be one; the only
data this service holds is a local DuckDB file it fetches for itself.

    uvicorn app:app --reload
"""
from __future__ import annotations

import asyncio
import html
import subprocess
import sys
import threading
import warnings
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

warnings.filterwarnings("ignore")
HERE = Path(__file__).resolve().parent
DB = HERE / "queens.duckdb"
sys.path.insert(0, str(HERE))
import sources as S  # noqa: E402

app = FastAPI(title="QueensLab", docs_url=None, redoc_url=None)
_fetching: set[str] = set()
_lock = threading.Lock()


def con(read_only: bool = True):
    import duckdb
    if not DB.exists():
        return None
    try:
        return duckdb.connect(str(DB), read_only=read_only)
    except Exception:
        return None


def loaded() -> list[tuple]:
    c = con()
    if not c:
        return []
    try:
        return c.execute("""SELECT key, rows, bytes,
                                   strftime(fetched_at,'%d %b %H:%M') t
                            FROM _fetch_log ORDER BY rows DESC""").fetchall()
    except Exception:
        return []
    finally:
        c.close()


def q(sql: str, limit: int = 60):
    c = con()
    if not c:
        return [], []
    try:
        cur = c.execute(sql)
        cols = [d[0] for d in cur.description]
        return cols, cur.fetchmany(limit)
    except Exception as e:
        return ["error"], [(str(e)[:200],)]
    finally:
        c.close()


# ── analyses shown on the page ───────────────────────────────────────────
PANELS = [
    ("Business mix", "dcwp_licences", """
        SELECT business_category AS category, count(*) AS licences
        FROM dcwp_licences WHERE license_status='Active'
        GROUP BY 1 ORDER BY licences DESC LIMIT 12"""),

    ("Density per 10k residents, by neighbourhood", "dcwp_licences,nta_population", """
        WITH pop AS (SELECT nta_code, max(TRY_CAST(population AS BIGINT)) p
                     FROM nta_population GROUP BY 1 HAVING p > 2000),
             biz AS (SELECT nta, count(*) n FROM dcwp_licences
                     WHERE license_status='Active' AND nta IS NOT NULL GROUP BY 1)
        SELECT b.nta AS nta, b.n AS businesses, pop.p AS population,
               round(10000.0*b.n/pop.p,1) AS per_10k
        FROM biz b JOIN pop ON pop.nta_code=b.nta
        ORDER BY per_10k DESC LIMIT 12"""),

    ("Storefront vacancy", "storefront_vacancy", """
        SELECT reporting_year AS year, sum(TRY_CAST(storefronts AS BIGINT)) AS storefronts,
               sum(TRY_CAST(vacant AS BIGINT)) AS vacant,
               round(100.0*sum(TRY_CAST(vacant AS BIGINT))
                     /nullif(sum(TRY_CAST(storefronts AS BIGINT)),0),1) AS pct_vacant
        FROM storefront_vacancy GROUP BY 1 ORDER BY 1"""),

    ("Industry scale (County Business Patterns)", "cbp_industry", """
        SELECT industry, establishments, employment
        FROM cbp_industry WHERE naics <> '------'
        ORDER BY establishments DESC LIMIT 12"""),

    ("Tract demographics (ACS 2023)", "acs_tracts", """
        SELECT count(*) AS tracts,
               round(avg(TRY_CAST(population AS DOUBLE))) AS avg_population,
               round(median(TRY_CAST(median_income AS DOUBLE))) AS median_income,
               round(avg(TRY_CAST(median_age AS DOUBLE)),1) AS avg_median_age
        FROM acs_tracts"""),
]

CSS = """
*{box-sizing:border-box}
body{margin:0;font:15px/1.6 'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
 color:#15171c;background:#fff;-webkit-font-smoothing:antialiased;letter-spacing:-.006em}
.wrap{max-width:1100px;margin:0 auto;padding:0 24px 80px}
header{border-bottom:1px solid #eceef2;margin-bottom:34px}
.hin{max-width:1100px;margin:0 auto;padding:26px 24px}
h1{font-size:26px;font-weight:650;letter-spacing:-.028em;margin:0}
.sub{color:#697180;font-size:14.5px;margin:6px 0 0}
.wall{display:inline-block;margin-top:14px;font-size:12.5px;color:#3c6e4f;
 background:#eaf6ef;border:1px solid #cfe8da;border-radius:999px;padding:5px 13px}
h2{font-size:16px;font-weight:650;letter-spacing:-.02em;margin:34px 0 12px}
table{width:100%;border-collapse:collapse;font-size:13.5px;font-variant-numeric:tabular-nums}
th{text-align:left;font-size:10.5px;font-weight:650;letter-spacing:.06em;text-transform:uppercase;
 color:#8a92a0;border-bottom:1.5px solid #15171c;padding:0 10px 7px 0}
td{padding:7px 10px 7px 0;border-bottom:1px solid #f1f3f6}
td:not(:first-child),th:not(:first-child){text-align:right}
.muted{color:#8a92a0}
.empty{background:#fafbfd;border:1px dashed #dde2ea;border-radius:10px;padding:22px;
 color:#697180;font-size:14px}
code{font:12.5px 'SFMono-Regular',Menlo,monospace;background:#f3f5f9;padding:2px 6px;border-radius:4px}
.chips{display:flex;flex-wrap:wrap;gap:7px;margin:10px 0 0}
.chip{font-size:12px;color:#5d6475;background:#f6f8fb;border:1px solid #e6eaf0;
 border-radius:999px;padding:4px 11px}
.chip.on{color:#2a5d3f;background:#eaf6ef;border-color:#cfe8da}
"""


def table_html(cols, rows) -> str:
    if not rows:
        return '<div class="empty">No rows.</div>'
    th = "".join(f"<th>{html.escape(str(c)).replace('_',' ')}</th>" for c in cols)
    tr = "".join("<tr>" + "".join(
        f"<td>{html.escape('' if v is None else str(v))}</td>" for v in r) + "</tr>"
        for r in rows)
    return f"<table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>"


@app.get("/", response_class=HTMLResponse)
def home():
    have = {k for k, *_ in loaded()}
    total = sum(r[1] for r in loaded())
    chips = "".join(
        f'<span class="chip{" on" if s.key in have else ""}">{html.escape(s.label)}</span>'
        for s in S.ALL)

    body = []
    for title, needs, sql in PANELS:
        if not all(t in have for t in needs.split(",")):
            body.append(f"<h2>{html.escape(title)}</h2>"
                        f'<div class="empty">Not loaded yet — run '
                        f'<code>python fetch.py --only {needs.split(",")[0]}</code></div>')
            continue
        cols, rows = q(sql)
        body.append(f"<h2>{html.escape(title)}</h2>" + table_html(cols, rows))

    state = (f"{len(have)} of {len(S.ALL)} sources · {total:,} Queens rows"
             if have else "No data yet")
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QueensLab</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;650&display=swap" rel="stylesheet">
<style>{CSS}</style></head><body>
<header><div class="hin">
  <h1>QueensLab</h1>
  <p class="sub">Queens business &amp; demographic research — {state}</p>
  <span class="wall">Standalone · no connection to BizGenome</span>
  <div class="chips">{chips}</div>
</div></header>
<div class="wrap">{''.join(body)}
  <h2>Refresh</h2>
  <div class="empty">On Render's free plan the disk is wiped on restart, so data is
  re-fetched rather than stored. Pull with <code>POST /fetch/&lt;key&gt;</code> or run
  <code>python fetch.py --core</code> locally.</div>
</div></body></html>"""


@app.get("/api/status")
def status():
    rows = loaded()
    return {"sources_declared": len(S.ALL),
            "sources_loaded": len(rows),
            "rows": sum(r[1] for r in rows),
            "tables": [{"key": k, "rows": n, "bytes": b, "fetched": t} for k, n, b, t in rows],
            "fetching": sorted(_fetching),
            "database": "local duckdb (no external database)",
            "checked": datetime.now(timezone.utc).isoformat(timespec="seconds")}


@app.post("/fetch/{key}")
async def fetch(key: str):
    if key not in S.BY_KEY and key not in ("core", "all"):
        return JSONResponse({"error": f"unknown source '{key}'"}, status_code=404)
    with _lock:
        if key in _fetching:
            return {"status": "already running", "key": key}
        _fetching.add(key)

    def run():
        arg = f"--{key}" if key in ("core", "all") else f"--only={key}"
        try:
            subprocess.run([sys.executable, str(HERE / "fetch.py"), arg],
                           cwd=HERE, timeout=1800, check=False)
        finally:
            with _lock:
                _fetching.discard(key)

    asyncio.get_running_loop().run_in_executor(None, run)
    return {"status": "started", "key": key, "poll": "/api/status"}
