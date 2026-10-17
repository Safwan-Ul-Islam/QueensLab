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



def source_link(src) -> str:
    """Human-readable page for a source, derived rather than stored."""
    if getattr(src, "kind", None) == "acs":
        return "https://data.census.gov/"
    if getattr(src, "kind", None) == "cbp":
        return "https://www.census.gov/programs-surveys/cbp.html"
    return f"https://{src.domain}/d/{src.dataset}"


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
/* numbers right, prose left — the notes column was inheriting the number rule */
td.l,th.l{text-align:left}
.muted{color:#8a92a0}
.empty{background:#fafbfd;border:1px dashed #dde2ea;border-radius:10px;padding:22px;
 color:#697180;font-size:14px}
code{font:12.5px 'SFMono-Regular',Menlo,monospace;background:#f3f5f9;padding:2px 6px;border-radius:4px}
.note{color:#697180;font-size:13.5px;margin:0 0 12px}
textarea{width:100%;font:13px 'SFMono-Regular',Menlo,monospace;padding:12px 14px;
 border:1px solid #dde2ea;border-radius:9px;resize:vertical;background:#fbfcfe;color:#15171c}
textarea:focus{outline:2px solid #2b52e0;outline-offset:1px;border-color:transparent}
button{margin-top:10px;font:600 14px 'Inter',sans-serif;color:#fff;background:#15171c;
 border:0;border-radius:999px;padding:11px 24px;cursor:pointer}
button:hover{background:#000}
a{color:#2b52e0;text-decoration:none}
a:hover{text-decoration:underline}
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
def home(sql: str = "", result: str = ""):
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

    srows = []
    for src in S.ALL:
        on = src.key in have
        n = f"{src.probed:,}" if src.probed else "—"
        srows.append(
            f'<tr><td>{"✅" if on else "○"}</td>'
            f'<td class="l"><a href="{source_link(src)}" target="_blank" rel="noopener">'
            f'{html.escape(src.label)}</a></td>'
            f'<td><code>{html.escape(src.key)}</code></td><td>{n}</td>'
            f'<td class="muted l">{html.escape((src.note or "")[:120])}</td></tr>')
    src_table = ('<table><thead><tr><th></th><th>Source</th><th>Table</th>'
                 '<th>Queens rows</th><th class="l">Notes</th></tr></thead><tbody>'
                 + "".join(srows) + "</tbody></table>")

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
  <h2>Where this data comes from</h2>
  <p class="note">Every source below is <b>free and public</b>. Nothing is bought, and
  nothing here is used to train BizGenome's model.</p>
  {src_table}

  <h2>Query it yourself</h2>
  <p class="note">Read-only SQL over the tables above. Try
  <code>SELECT business_category, count(*) FROM dcwp_licences GROUP BY 1 ORDER BY 2 DESC</code></p>
  <form method="get" action="/query">
    <textarea name="sql" rows="3" placeholder="SELECT * FROM dcwp_licences LIMIT 20">{sql}</textarea>
    <button type="submit">Run</button>
  </form>
  {result}

  <h2>Refresh</h2>
  <div class="empty">On Render's free plan the disk is wiped on restart, so data is
  re-fetched rather than stored. Pull with <code>POST /fetch/core</code>, or run
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

SAFE = ("select", "with", "describe", "show", "summarize", "pragma_table_info")


@app.get("/query", response_class=HTMLResponse)
def query(sql: str = ""):
    """Read-only SQL over the research tables.

    This page is PUBLIC. The data is all public records, so exposure is not the
    worry — abuse is. The connection is read-only, only read statements are
    accepted, and results are capped.
    """
    sql = (sql or "").strip().rstrip(";")
    if not sql:
        return home()
    first = sql.lstrip("( \n\t").split(None, 1)[0].lower() if sql.split() else ""
    if first not in SAFE:
        return home(sql, '<div class="empty">Only read queries are allowed '
                         '(SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE).</div>')
    cols, rows = q(sql, limit=200)
    if cols == ["error"]:
        return home(sql, f'<div class="empty">{html.escape(str(rows[0][0]))}</div>')
    note = f'<p class="note">{len(rows)} row(s){" · capped at 200" if len(rows) == 200 else ""}</p>'
    return home(sql, note + table_html(cols, rows))
