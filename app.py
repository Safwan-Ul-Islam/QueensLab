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


@app.on_event("startup")
def _restore():
    """Render's free disk is empty on every boot; the Parquet in the repo is
    not. Rebuild from it before the first request is served."""
    global _loaded_from
    if DB.exists():
        _loaded_from = "existing database file on disk"
        return
    import fetch
    import gcs
    # The bucket is the store. The Parquet committed in raw/ is the fallback, so
    # a missing or broken key degrades to slightly-older data, never to nothing.
    reason = "bucket not configured"
    if gcs.enabled():
        try:
            n = fetch.rebuild_from_gcs()
            _loaded_from = f"gs://{gcs.bucket()}"
            print(f"[queenslab] rebuilt {n:,} rows from {_loaded_from}", flush=True)
            return
        except Exception as e:  # noqa: BLE001
            reason = f"bucket failed — {type(e).__name__}: {str(e)[:140]}"
            print(f"[queenslab] {reason} — falling back to committed parquet", flush=True)
            DB.unlink(missing_ok=True)
    n = fetch.rebuild_from_parquet()
    _loaded_from = f"parquet committed in the repo ({reason})"
    print(f"[queenslab] rebuilt {n:,} rows from committed parquet", flush=True)
# Where the data on this boot ACTUALLY came from. `bucket` in /api/status only
# says one is configured — after a silent fallback that would have misreported.
_loaded_from = "not loaded yet"
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

    # Names come from nta_population, NOT the `ntas` table: DCWP and
    # nta_population both use the 2010 NTA scheme (QN61), while `ntas` is the
    # 2020 scheme (QN0101). Joining the two matched exactly zero rows.
    ("Density per 10k residents, by neighbourhood", "dcwp_licences,nta_population", """
        WITH pop AS (SELECT nta_code, max(nta_name) nta_name,
                            max(TRY_CAST(population AS BIGINT)) p
                     FROM nta_population GROUP BY 1 HAVING p > 2000),
             biz AS (SELECT nta, count(*) n FROM dcwp_licences
                     WHERE license_status='Active' AND nta IS NOT NULL GROUP BY 1)
        SELECT pop.nta_name AS neighbourhood, b.nta AS code,
               b.n AS businesses, pop.p AS population,
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
:root{--ink:#15171c;--muted:#697180;--faint:#98a0ac;--line:#eceef2;--soft:#f7f9fc;
      --accent:#2b52e0;--good:#16855a}
body{margin:0;font:15px/1.6 'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
 color:var(--ink);background:#fff;-webkit-font-smoothing:antialiased;letter-spacing:-.006em}
.wrap{max-width:1120px;margin:0 auto;padding:0 28px}

/* header */
header{border-bottom:1px solid var(--line);background:#fff}
.hin{max-width:1120px;margin:0 auto;padding:30px 28px 0}
h1{font-size:27px;font-weight:650;letter-spacing:-.03em;margin:0}
.lede{color:var(--muted);font-size:15px;margin:7px 0 0;max-width:62ch}
.badge{display:inline-flex;align-items:center;gap:6px;margin-top:14px;font-size:12.5px;
 color:#2a5d3f;background:#eaf6ef;border:1px solid #cfe8da;border-radius:999px;padding:5px 13px}
.badge::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--good)}

/* stat cards */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:26px 0 0}
.stat{background:var(--soft);border:1px solid var(--line);border-radius:12px;padding:15px 17px}
.stat b{display:block;font-size:25px;font-weight:650;letter-spacing:-.03em;line-height:1.1;
 font-variant-numeric:tabular-nums}
.stat span{display:block;font-size:11px;font-weight:600;letter-spacing:.05em;
 text-transform:uppercase;color:var(--faint);margin-top:6px}

/* section nav */
nav{display:flex;gap:4px;margin:24px 0 -1px;position:sticky;top:0;background:#fff;z-index:5;
 padding-top:4px}
nav a{font-size:13.5px;font-weight:600;color:var(--muted);text-decoration:none;
 padding:10px 15px;border-bottom:2px solid transparent}
nav a:hover{color:var(--ink);border-bottom-color:var(--line)}

section{padding:38px 0 6px;scroll-margin-top:52px}
h2{font-size:19px;font-weight:650;letter-spacing:-.024em;margin:0 0 4px}
.note{color:var(--muted);font-size:13.5px;margin:0 0 16px;max-width:76ch}

/* cards */
.card{border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin:0 0 14px}
.card h3{font-size:14.5px;font-weight:650;margin:0 0 12px;letter-spacing:-.015em}

table{width:100%;border-collapse:collapse;font-size:13.5px;font-variant-numeric:tabular-nums}
th{text-align:left;font-size:10.5px;font-weight:650;letter-spacing:.06em;text-transform:uppercase;
 color:var(--faint);border-bottom:1.5px solid var(--ink);padding:0 10px 7px 0;white-space:nowrap}
td{padding:7px 10px 7px 0;border-bottom:1px solid #f4f6f9;vertical-align:top}
tr:last-child td{border-bottom:0}
td:not(:first-child),th:not(:first-child){text-align:right}
td.l,th.l{text-align:left}
.muted{color:var(--muted)}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#dde2ea}
.dot.on{background:var(--good)}

.empty{background:var(--soft);border:1px dashed #dde2ea;border-radius:11px;padding:20px;
 color:var(--muted);font-size:14px}
code{font:12.5px 'SFMono-Regular',Menlo,monospace;background:#f1f4f9;padding:2px 6px;
 border-radius:4px;color:#3c4254}
textarea{width:100%;font:13px/1.55 'SFMono-Regular',Menlo,monospace;padding:13px 15px;
 border:1px solid #dde2ea;border-radius:10px;resize:vertical;background:var(--soft);color:var(--ink)}
textarea:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:transparent}
button{margin-top:11px;font:600 14px 'Inter',sans-serif;color:#fff;background:var(--ink);
 border:0;border-radius:999px;padding:11px 25px;cursor:pointer}
button:hover{background:#000}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
footer{margin:46px 0 70px;padding-top:20px;border-top:1px solid var(--line);
 color:var(--faint);font-size:12.5px}
@media (max-width:820px){.stats{grid-template-columns:repeat(2,1fr)}
 nav{overflow-x:auto}.wrap,.hin{padding-left:18px;padding-right:18px}}
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
    rows_log = loaded()
    have = {k for k, *_ in rows_log}
    total = sum(r[1] for r in rows_log)
    last = max((r[3] for r in rows_log), default=None)

    # ── findings, one card each ──────────────────────────────────────────
    cards = []
    for title, needs, q_sql in PANELS:
        if not all(t in have for t in needs.split(",")):
            missing = [t for t in needs.split(",") if t not in have][0]
            cards.append(
                f'<div class="card"><h3>{html.escape(title)}</h3>'
                f'<div class="empty">Needs <code>{html.escape(missing)}</code> — '
                f'pull it from the Sources tab below.</div></div>')
            continue
        cols, rws = q(q_sql)
        cards.append(f'<div class="card"><h3>{html.escape(title)}</h3>'
                     + table_html(cols, rws) + "</div>")

    # ── sources, loaded ones first ───────────────────────────────────────
    ordered = sorted(S.ALL, key=lambda x: (x.key not in have, x.label))
    srows = []
    for src in ordered:
        on = src.key in have
        n = f"{src.probed:,}" if src.probed else "—"
        note = (src.note or "").replace("⚠️", "").strip()
        srows.append(
            f'<tr><td class="l"><span class="dot{" on" if on else ""}"></span></td>'
            f'<td class="l"><a href="{source_link(src)}" target="_blank" rel="noopener">'
            f'{html.escape(src.label)}</a></td>'
            f'<td class="l"><code>{html.escape(src.key)}</code></td><td>{n}</td>'
            f'<td class="muted l" title="{html.escape(note)}">'
            f'{html.escape(note[:88])}{"…" if len(note) > 88 else ""}</td></tr>')
    src_table = ('<table><thead><tr><th class="l"></th><th class="l">Source</th>'
                 '<th class="l">Table</th><th>Queens rows</th>'
                 '<th class="l">Notes</th></tr></thead><tbody>'
                 + "".join(srows) + "</tbody></table>")

    stats = f"""<div class="stats">
      <div class="stat"><b>{len(have)}<small style="font-size:15px;color:#98a0ac;font-weight:500">/{len(S.ALL)}</small></b>
        <span>sources loaded</span></div>
      <div class="stat"><b>{total:,}</b><span>Queens rows</span></div>
      <div class="stat"><b>82</b><span>neighbourhoods</span></div>
      <div class="stat"><b>{html.escape(last or "—")}</b><span>last fetched</span></div>
    </div>"""

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QueensLab — Queens business research</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;650&display=swap" rel="stylesheet">
<style>{CSS}</style></head><body>

<header><div class="hin">
  <h1>QueensLab</h1>
  <p class="lede">Open data on every business, building and neighbourhood in Queens,
     pulled from public records and queryable here.</p>
  <span class="badge">Standalone research — not connected to BizGenome</span>
  {stats}
  <nav>
    <a href="#findings">Findings</a>
    <a href="#sources">Sources</a>
    <a href="#query">Query</a>
    <a href="/api/status">API</a>
  </nav>
</div></header>

<div class="wrap">
  <section id="findings">
    <h2>Findings</h2>
    <p class="note">Computed live from the tables below. Each card re-runs its query on load.</p>
    {''.join(cards)}
  </section>

  <section id="sources">
    <h2>Sources</h2>
    <p class="note">All {len(S.ALL)} are <b>free and public</b> — city, state and federal records.
      Nothing is bought, and nothing here trains BizGenome's model. A green dot means loaded.</p>
    {src_table}
    <p class="note" style="margin-top:14px">Pull more with
      <code>POST /fetch/&lt;table&gt;</code>, or <code>python fetch.py --core</code> locally.</p>
  </section>

  <section id="query">
    <h2>Query</h2>
    <p class="note">Read-only SQL over the loaded tables. Results cap at 200 rows.</p>
    <form method="get" action="/query">
      <textarea name="sql" rows="3"
        placeholder="SELECT business_category, count(*) AS n FROM dcwp_licences GROUP BY 1 ORDER BY n DESC LIMIT 10">{html.escape(sql)}</textarea>
      <button type="submit">Run query</button>
    </form>
    {result}
  </section>

  <footer>Queens open data · DuckDB · no external database ·
    <a href="https://github.com/Safwan-Ul-Islam/QueensLab" target="_blank" rel="noopener">source</a>
  </footer>
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
            "bucket": (f"gs://{__import__('gcs').bucket()}" if __import__('gcs').enabled()
                       else "not configured"),
            "loaded_from": _loaded_from,
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
            # --force: after a rebuild every table already counts as fetched, so
            # without it a live refresh silently did nothing and never reached
            # the bucket.
            subprocess.run([sys.executable, str(HERE / "fetch.py"), arg, "--force"],
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
