"""QueensLab — a standalone page for Queens research.

Completely separate from BizGenome: its own repo, its own Render service, its
own store. There is NO DATABASE_URL here and there must never be one; the only
data this service holds is a local DuckDB file it fetches for itself.

    uvicorn app:app --reload
"""
from __future__ import annotations

import asyncio
import hmac
import html
import os
import subprocess
import sys
import threading
import warnings
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

warnings.filterwarnings("ignore")
HERE = Path(__file__).resolve().parent
DB = HERE / "queens.duckdb"
sys.path.insert(0, str(HERE))
import sources as S  # noqa: E402
import jobs  # noqa: E402

app = FastAPI(title="QueensLab", docs_url=None, redoc_url=None)


@app.on_event("startup")
def _restore():
    """Render's free disk is empty on every boot; the Parquet in the repo is
    not. Rebuild from it before the first request is served."""
    global _loaded_from
    jobs.on_boot()          # job history back from the bucket; never raises
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
_lock = threading.Lock()
# The last figures the page managed to read. While a pull holds the database's
# write lock, the page shows these with a notice, instead of going blank.
_seen: dict = {"loaded": [], "panels": {}}
_tls = threading.local()


def busy() -> bool:
    """Did this request's last connection find the database locked by a writer?"""
    return getattr(_tls, "busy", False)


def con(read_only: bool = True):
    """Every page request, and the public SQL box, runs on one of these.

    ⚠️ read_only stops writes to the DATABASE, not reads of the MACHINE. Until
    2026-10-17 a query could read any file the server can — its environment,
    holding the bucket key, included — and `SELECT 1; COPY ... TO ...` wrote
    files. So: give the connection the bucket key the big tables need (they
    are views over the bucket), then take away the local disk and the open
    web, and lock the setting so no query can undo it. Fails closed."""
    import duckdb
    _tls.busy = False
    if not DB.exists():
        return None
    try:
        c = duckdb.connect(str(DB), read_only=read_only)
    except Exception as e:
        # Another process is writing — a pull saving its table. Say so. Only
        # DuckDB's own lock message counts: matching the bare word "lock" also
        # matched "block" and called a damaged file "busy" forever.
        msg = str(e).lower()
        _tls.busy = "could not set lock" in msg or "conflicting lock" in msg
        return None
    # Connections to one file can share a DuckDB instance; on an instance that
    # is already locked down these steps fail harmlessly, so each is tolerated
    # and the end state is TESTED rather than assumed.
    import gcs
    if gcs.enabled():
        try:
            gcs.attach(c)
        except Exception:
            pass
    # HTTPFileSystem too: otherwise the box fetches any web address FROM the
    # server. The bucket is gs://, a different file system, and still works.
    # Capped so one heavy query in the public box fails on its own instead of
    # taking the whole 512 MB server down with it.
    for stmt in ("SET memory_limit='200MB'", "SET threads=2",
                 "SET disabled_filesystems='LocalFileSystem,HTTPFileSystem'",
                 "SET lock_configuration=true"):
        try:
            c.execute(stmt)
        except Exception:
            pass
    # current_setting('disabled_filesystems') reads '' even when it is on, so
    # test the effect: listing the database's own file must be refused.
    try:
        c.execute("SELECT count(*) FROM glob(?)", [str(DB)]).fetchone()
        safe = False
    except Exception as e:  # noqa: BLE001
        safe = "disabled" in str(e)
    if not safe:
        c.close()
        return None
    return c


def loaded() -> list[tuple]:
    c = con()
    if not c:
        return _seen["loaded"] if busy() else []
    try:
        rows = c.execute("""SELECT key, rows, bytes,
                                   strftime(fetched_at,'%d %b %H:%M') t
                            FROM _fetch_log ORDER BY rows DESC""").fetchall()
        _seen["loaded"] = rows
        return rows
    except Exception:
        return []
    finally:
        c.close()


QUERY_SECONDS = 30


def q(sql: str, limit: int = 60):
    c = con()
    if not c:
        return [], []
    # A runaway query is interrupted rather than left to hold the server.
    timer = threading.Timer(QUERY_SECONDS, c.interrupt)
    timer.start()
    try:
        cur = c.execute(sql)
        cols = [d[0] for d in cur.description]
        return cols, cur.fetchmany(limit)
    except Exception as e:
        msg = str(e)
        if "INTERRUPT" in msg.upper():
            msg = f"Stopped after {QUERY_SECONDS} seconds. Try a narrower query (a WHERE, or a LIMIT)."
        return ["error"], [(msg[:200],)]
    finally:
        timer.cancel()
        c.close()



def source_link(src) -> str:
    """Human-readable page for a source, derived rather than stored."""
    if getattr(src, "kind", None) == "acs":
        return "https://data.census.gov/"
    if getattr(src, "kind", None) == "cbp":
        return "https://www.census.gov/programs-surveys/cbp.html"
    if getattr(src, "kind", None) == "overture":
        return "https://docs.overturemaps.org/guides/places/"
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
    ("DCWP-licensed businesses per 10k residents (2010 neighbourhoods)", "dcwp_licences,nta_population", """
        WITH pop AS (SELECT nta_code, max(nta_name) nta_name,
                            max(TRY_CAST(population AS BIGINT)) p
                     FROM nta_population GROUP BY 1 HAVING p > 2000),
             biz AS (SELECT nta, count(*) n FROM dcwp_licences
                     WHERE license_status='Active' AND nta IS NOT NULL GROUP BY 1)
        SELECT pop.nta_name AS neighbourhood, b.nta AS code,
               b.n AS dcwp_licensed, pop.p AS population_2010,
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

.pill{display:inline-block;font-size:11.5px;font-weight:600;border-radius:999px;padding:2px 9px;
 white-space:nowrap;background:#eef1f5;color:#4a5262}
.pill.ok{background:#e8f5ee;color:var(--good)}
.pill.bad{background:#fdecec;color:#b42318}
.pill.run{background:#e9efff;color:var(--accent)}
.pill.warn{background:#fff4e0;color:#9a5b00}
.sub{display:block;font-size:11.5px;color:var(--faint);white-space:normal}
.err{color:#b42318;font-size:12.5px}
.scroll{overflow-x:auto}
#jobs td{white-space:nowrap}
#jobs td.wrap{white-space:normal;min-width:220px}
.nb th{cursor:pointer}
.nb th[data-dir=desc]::after{content:" ↓"}
.nb th[data-dir=asc]::after{content:" ↑"}
.nb td:first-child,.nb th:first-child{position:sticky;left:0;background:#fff;z-index:1}
.nb td{white-space:nowrap}
.nb tr.nonres td{color:var(--faint)}
.defs{margin-top:12px;font-size:13px;color:var(--muted)}
.defs summary{cursor:pointer;font-weight:600}
.defs ul{padding-left:18px}
.defs li{margin:5px 0}
.panel{margin-top:14px;border:1px solid var(--line);border-radius:12px;padding:12px 16px}
.panel summary{cursor:pointer;font-weight:650}
.panel h3{font-size:14px;margin:20px 0 8px}
.pnl-login,.pnl-row{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:10px 0}
.panel input,.panel select{font:13px 'Inter',sans-serif;padding:7px 9px;border:1px solid #dde2ea;border-radius:8px}
.panel input[type=number]{width:90px}
.panel button{margin-top:0;padding:8px 16px;font-size:13px}
.panel button.ghost{background:#fff;color:var(--ink);border:1px solid #dde2ea}
.panel label{font-size:13px;color:var(--muted)}
.panel .blk{display:block}
.cfg input,.cfg select{width:100%}
.cfg td{padding:4px 6px 4px 0}
#crawl-results td{white-space:normal;min-width:110px;font-size:12.5px}
.credits{display:flex;flex-wrap:wrap;gap:6px 18px;margin:12px 0 0;font-size:13.5px}
.credits b{font-size:15px}
.credits .bad{color:#b42318;font-weight:600}
.live{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:14px 0;padding:12px 14px;
 border:1px solid var(--line);border-radius:10px;background:var(--soft)}
.live .dot{width:10px;height:10px;border-radius:50%;background:#c3c9d3}
.live.running .dot{background:var(--accent);animation:pulse 1.2s ease-in-out infinite}
.live.done .dot{background:var(--good)}
.live.failed .dot{background:#b42318}
.live .bar{flex-basis:100%;height:6px;background:#e6e9ef;border-radius:3px;overflow:hidden}
.live .bar div{height:100%;width:0;background:var(--accent);transition:width .6s}
.live.done .bar div{background:var(--good)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.35;transform:scale(1.35)}}
.fbar{display:grid;grid-template-columns:200px 1fr 90px;gap:10px;align-items:center;font-size:12.5px;margin:3px 0}
.fbar .t{height:8px;background:#eef1f5;border-radius:4px;overflow:hidden}
.fbar .t div{height:100%;background:var(--good)}
.busy{background:#fff8e6;border:1px solid #f3dfae;color:#7a5200;border-radius:11px;
 padding:12px 16px;margin:26px 0 0;font-size:14px}
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


def _t(iso: str | None) -> str:
    """'17 Oct 18:30' (UTC) from an ISO string."""
    if not iso:
        return "—"
    try:
        t = datetime.fromisoformat(iso)
    except ValueError:
        return "—"
    if t.tzinfo is None:          # every time here is UTC; never let one go local
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).strftime("%d %b %H:%M")


def _ago(iso: str | None) -> str:
    a = jobs.age(iso)
    if a is None:
        return ""
    return "just now" if a < 60 else f"{int(a // 60)} min ago" if a < 5400 else f"{a / 3600:.0f} h ago"


_PILL = {"queued": "", "running": "run", "succeeded": "ok", "failed": "bad", "stalled": "warn",
         "uploaded": "ok", "pending": "", "skipped": "warn", "off": ""}
_ORIGIN = {"cli": "command line", "web": "web", "upload": "upload only", "trial": "trial",
           "derived": "built here"}


def collection_html(limit: int = 25) -> str:
    """Source | Status | Rows downloaded | Started | Last activity | Finished |
    GCS upload | Error — every machine's jobs, newest first.

    Never raises: rows can come from another machine's mirror, and one odd row
    must cost at most that row, not the whole page."""
    try:
        rows = jobs.listing(limit)
    except Exception as e:  # noqa: BLE001
        return (f'<div id="jobs" data-active="0"><div class="empty">The collection list could '
                f'not be read just now ({html.escape(type(e).__name__)}). Reload in a minute.'
                '</div></div>')
    active = any(j.get("state") in jobs.ACTIVE for j in rows)
    if not rows:
        body = ('<div class="empty">No collection jobs yet. Each pull appears here the '
                'moment it is queued.</div>')
    else:
        trs = []
        for j in rows:
            try:
                trs.append(_job_row(j))
            except Exception:  # noqa: BLE001 — a malformed row is skipped, not fatal
                continue
        body = ('<div class="scroll"><table><thead><tr><th class="l">Source</th>'
                '<th class="l">Status</th><th>Rows downloaded</th><th>Started</th>'
                '<th>Last activity</th><th>Finished</th><th class="l">GCS upload</th>'
                '<th class="l">Error</th></tr></thead><tbody>'
                + "".join(trs) + "</tbody></table></div>")
    return f'<div id="jobs" data-active="{1 if active else 0}">{body}</div>'


def _job_row(j: dict) -> str:
    e = lambda v: html.escape("" if v is None else str(v))  # noqa: E731
    state = str(j.get("state") or "unknown")
    retries = int(j.get("retries") or 0)
    label = state + (f" · {j['step']}" if state == "running" and j.get("step") else "")
    if retries and state in ("queued", "running", "stalled"):
        label += f" · {retries} retr{'y' if retries == 1 else 'ies'}"
    tip = (f"no sign of life since {_t(j.get('last_activity'))} UTC — the process "
           "has probably stopped, or its machine is asleep" if state == "stalled" else "")
    gcs_state = str(j.get("gcs") or "—")
    gcs_label = {"off": "no bucket", "pending": "waiting",
                 "skipped": "not uploaded"}.get(gcs_state, gcs_state)
    gcs_tip = j.get("gcs_detail") or ("Run python fetch.py --push to upload what is saved locally."
                                      if gcs_state == "skipped" else "")
    err = str(j.get("error") or "")
    warn = str(j.get("warnings") or "")
    # Last activity = the last time it got somewhere (rows, a step, an upload).
    # A job retrying a dead server is alive but not progressing: say both.
    progress = j.get("progress_at") or j.get("last_activity")
    last = _t(progress)
    if state in ("queued", "running", "stalled"):
        last += f'<span class="sub">{e(_ago(progress))}'
        if j.get("last_activity") and j.get("last_activity") != progress:
            last += f' · alive {e(_ago(j.get("last_activity")))}'
        last += "</span>"
    return (f'<tr><td class="l"><code>{e(j.get("source"))}</code>'
            f'<span class="sub">{e(j.get("host"))} · '
            f'{e(_ORIGIN.get(j.get("origin"), j.get("origin")))}</span></td>'
            f'<td class="l"><span class="pill {_PILL.get(state, "")}" title="{e(tip)}">'
            f'{e(label)}</span></td>'
            f'<td>{int(j.get("rows_downloaded") or 0):,}</td>'
            f'<td>{_t(j.get("started"))}</td><td>{last}</td><td>{_t(j.get("finished"))}</td>'
            f'<td class="l"><span class="pill {_PILL.get(gcs_state, "")}" '
            f'title="{e(gcs_tip)}">{e(gcs_label)}</span></td>'
            f'<td class="l wrap"><span class="err" title="{e(err)}">{e(err[:160])}'
            f'{"…" if len(err) > 160 else ""}</span>'
            + (f'<span class="sub">⚠ {e(warn[:160])}</span>' if warn else "")
            + "</td></tr>")


# (column, header, kind) — kind decides formatting and how the column sorts.
NB_COLUMNS = [
    ("name", "Neighbourhood", "text"), ("residents", "Residents", "int"),
    ("median_income", "Median income", "money"),
    ("restaurants_per_10k", "Restaurants /10k", "dec"), ("new_restaurants_12m", "New restaurants (12 mo)", "int"),
    ("food_stores_per_10k", "Food stores /10k", "dec"), ("liquor_per_10k", "Liquor licences /10k", "dec"),
    ("businesses_per_10k", "DCWP businesses /10k", "dec"), ("new_licences_12m", "New DCWP licences (12 mo)", "int"),
    ("complaints_per_1k", "311 per 1k (12 mo)", "dec"), ("top_complaint", "Top 311 complaint", "text"),
    ("subway_weekday_riders", "Subway riders / weekday", "int"),
    ("vacancy_pct", "Storefront vacancy", "pct"), ("permits_12m", "Permits (12 mo)", "int"),
    ("median_sale_price", "Median sale", "money"), ("retail_sqft_per_resident", "Retail sq ft / resident", "dec"),
]


def _fmt(v, kind: str) -> str:
    if v is None:
        return "—"
    if kind == "int":
        return f"{int(round(float(v))):,}"
    if kind == "dec":
        return f"{float(v):,.1f}"
    if kind == "pct":
        return f"{float(v):.1f}%"
    if kind == "money":
        v = float(v)
        return f"${v / 1e6:.2f}M" if v >= 1e6 else f"${v / 1e3:.0f}k"
    return str(v)


def neighbourhoods_html() -> str:
    """The neighbourhood profile. nta_profile.py builds it on a laptop — about
    a million points placed into polygons — and this only shows it."""
    c = con()
    if not c:
        return ('<div class="empty">The database is busy saving a table — the profile '
                'will show again in a minute.</div>' if busy() else
                '<div class="empty">Not loaded yet.</div>')
    try:
        cols = ["kind"] + [k for k, _, _ in NB_COLUMNS]
        rows = c.execute(f"SELECT {', '.join(cols)} FROM nta_profile "
                         "ORDER BY kind = 'residential' DESC, residents DESC NULLS LAST, name").fetchall()
        notes = c.execute("SELECT metric, definition, window_from, window_to, records, placed "
                          "FROM nta_profile_notes").fetchall()
        built = c.execute("SELECT max(built_at) FROM nta_profile").fetchone()[0]
    except Exception:  # noqa: BLE001 — not built yet
        return ('<div class="empty">Not built yet. On a laptop with the sources pulled: '
                '<code>python nta_profile.py</code></div>')
    finally:
        c.close()
    e = html.escape
    th = "".join(f'<th class="{"l" if k == "text" else ""}" title="Sort">{e(h)}</th>'
                 for _, h, k in NB_COLUMNS)
    trs = []
    for r in rows:
        kind, vals = r[0], r[1:]
        tds = []
        for (col, _, k), v in zip(NB_COLUMNS, vals):
            sort_v = "" if v is None else str(v)
            cell = e(_fmt(v, k))
            if col == "name" and kind != "residential":
                cell += f'<span class="sub">{e(kind)}</span>'
            tds.append(f'<td class="{"l" if k == "text" else ""}" data-v="{e(sort_v)}">{cell}</td>')
        trs.append(f'<tr class="{"" if kind == "residential" else "nonres"}">' + "".join(tds) + "</tr>")
    defs = []
    for metric, text, w_from, w_to, records, placed in notes:
        bits = [e(text)]
        if w_to:
            bits.append(f"{e(w_from)} → {e(w_to)}")
        if records:
            bits.append(f"{placed:,} of {records:,} placed ({100 * placed / records:.1f}%)")
        defs.append(f"<li><b>{e(metric)}</b> — {' · '.join(bits)}</li>")
    return (f'<div class="scroll nb"><table id="nbt"><thead><tr>{th}</tr></thead><tbody>'
            + "".join(trs) + "</tbody></table></div>"
            f'<details class="defs"><summary>What each column means, its dates, and how much '
            f'of each source it covers</summary><ul>{"".join(defs)}</ul>'
            f'<p class="note">Built {e(_t(built))} UTC by <code>nta_profile.py</code>. Each record is '
            "placed in a neighbourhood by its own coordinates. Per-resident figures are left blank "
            "where fewer than 1,000 people live (parks, airports, cemeteries).</p></details>")


NB_JS = """<script>
/* Click a column header to sort the neighbourhood table; blanks always last. */
(function () {
  var t = document.getElementById('nbt');
  if (!t) return;
  var ths = t.tHead.rows[0].cells;
  Array.prototype.forEach.call(ths, function (th, i) {
    th.addEventListener('click', function () {
      var asc = th.getAttribute('data-dir') === 'desc';
      Array.prototype.forEach.call(ths, function (h) { h.removeAttribute('data-dir'); });
      th.setAttribute('data-dir', asc ? 'asc' : 'desc');
      var tb = t.tBodies[0];
      var rows = Array.prototype.slice.call(tb.rows);
      rows.sort(function (a, b) {
        var x = a.cells[i].getAttribute('data-v'), y = b.cells[i].getAttribute('data-v');
        if (x === '' && y === '') return 0;
        if (x === '') return 1;
        if (y === '') return -1;
        var nx = parseFloat(x), ny = parseFloat(y);
        var r = (isNaN(nx) || isNaN(ny)) ? x.localeCompare(y) : nx - ny;
        return asc ? r : -r;
      });
      rows.forEach(function (r) { tb.appendChild(r); });
    });
  });
})();
</script>"""


CRAWL_JS = """<script>
/* The website check + Firecrawl panel. Everything scraped is untrusted text:
   it is only ever set with textContent, never as HTML. */
(function () {
  var $ = function (id) { return document.getElementById(id); };
  var token = '';
  try { token = localStorage.getItem('ql_token') || ''; } catch (e) {}
  function el(tag, text, cls) { var e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
  function api(method, path, body) {
    var o = {method: method, headers: {'X-Fetch-Token': token}};
    if (body !== undefined) { o.headers['Content-Type'] = 'application/json'; o.body = JSON.stringify(body); }
    return fetch(path, o).then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || r.status); return j; }); });
  }
  function msg(t) { $('crawl-msg').textContent = t || ''; }
  function stat(box, value, label) { var d = el('div', null, 'stat'); d.appendChild(el('b', value)); d.appendChild(el('span', label)); box.appendChild(d); }

  // public overview
  fetch('/api/crawl/overview').then(function (r) { return r.json(); }).then(function (o) {
    var box = $('crawl-overview'); box.textContent = ''; box.className = 'stats';
    var c = o.site_checks || {};
    var n = function (k) { return (c[k] || 0).toLocaleString(); };
    stat(box, (o.sites_checked || 0).toLocaleString(), 'business sites checked');
    stat(box, ((c.alive || 0) + (c.alive_js || 0) + (c.blocked || 0)).toLocaleString(), 'alive');
    stat(box, n('dead'), 'dead — domain gone');
    stat(box, ((c.parked || 0) + (c.hijacked || 0)).toLocaleString(), 'parked or hijacked by spam');
    stat(box, n('platform'), 'not their own site');
    stat(box, (o.with_facts || 0).toLocaleString(), 'read by Firecrawl');
    var ul = $('crawl-canpull').querySelector('ul');
    (o.can_pull || []).forEach(function (p) { var li = el('li'); li.appendChild(el('b', p[0])); li.appendChild(document.createTextNode(' — ' + p[1])); ul.appendChild(li); });
  }).catch(function () { $('crawl-overview').textContent = 'Not available right now.'; });

  var cfg = null, per = {structured: 5, text: 1}, types = ['text', 'number', 'yes/no', 'list', 'prices'];
  function fieldRow(f) {
    var tr = el('tr');
    var name = el('input'); name.value = f.name || ''; name.placeholder = 'field_name';
    var type = el('select'); types.forEach(function (t) { var o = el('option', t); o.value = t; if (t === f.type) o.selected = true; type.appendChild(o); });
    var desc = el('input'); desc.value = f.description || ''; desc.placeholder = 'Describe exactly what to find';
    var del = el('button', '×', 'ghost'); del.type = 'button'; del.onclick = function () { tr.remove(); };
    [name, type, desc, del].forEach(function (x) { var td = el('td'); td.appendChild(x); tr.appendChild(td); });
    $('cfg-fields').appendChild(tr);
  }
  function estimate() {
    var m = $('cfg-mode').value, k = parseInt($('crawl-limit').value || '0', 10);
    var f = m === 'structured' ? parseInt($('cfg-follow').value || '0', 10) : 0;
    $('crawl-estimate').textContent = 'up to ' + (k * per[m] * (1 + f)).toLocaleString() + ' credits';
  }
  function load() {
    msg('Loading…');
    return api('GET', '/api/crawl/admin').then(function (d) {
      $('crawl-admin').hidden = false; msg('');
      try { localStorage.setItem('ql_token', token); } catch (e) {}
      per = d.credits_per_page || per; cfg = d.config; types = d.field_types || types;
      var s = $('crawl-stats'); s.textContent = '';
      stat(s, d.credits && d.credits.remaining != null ? d.credits.remaining.toLocaleString() : '—', 'Firecrawl credits left');
      stat(s, (d.crawlable_sites || 0).toLocaleString(), 'alive sites to read');
      stat(s, d.credits && d.credits.period_end ? d.credits.period_end.slice(0, 10) : '—', 'credits renew');
      var runs = $('crawl-runs'); runs.textContent = '';
      (d.runs || []).slice().reverse().slice(0, 5).forEach(function (r) {
        runs.appendChild(el('div', r.started.slice(0, 16).replace('T', ' ') + ' UTC · ' + r.n + ' sites · ' + r.mode + ' · ' +
          (r.status || '') + ' ' + (r.completed || 0) + '/' + r.n + ' · ' + (r.credits_used || 0) + ' credits' +
          (r.collected_at ? ' · collected' : (r.expires_at ? ' · results kept until ' + r.expires_at.slice(0, 16).replace('T', ' ') : '')), 'muted'));
      });
      live();
      $('cfg-mode').value = cfg.mode; $('cfg-prompt').value = cfg.prompt;
      $('cfg-recrawl').value = cfg.recrawl_days; $('cfg-conf').value = cfg.filters.min_confidence;
      $('cfg-blocked').checked = !!cfg.include_blocked;
      $('cfg-follow').value = cfg.follow_links || 0; $('cfg-evidence').checked = !!cfg.evidence;
      $('cfg-cats').value = (cfg.filters.categories || []).join(', '); $('cfg-ntas').value = (cfg.filters.ntas || []).join(', ');
      $('cfg-fields').textContent = ''; cfg.fields.forEach(fieldRow);
      $('crawl-limit').value = cfg.default_batch; estimate();
      var res = d.results || {columns: [], rows: []}, box = $('crawl-results'); box.textContent = '';
      if (!res.rows.length) { box.appendChild(el('div', 'No results yet.', 'empty')); return; }
      var t = el('table'), hr = el('tr'); res.columns.forEach(function (c) { hr.appendChild(el('th', c.replace(/_/g, ' '), 'l')); });
      var th = el('thead'); th.appendChild(hr); t.appendChild(th); var tb = el('tbody');
      res.rows.forEach(function (r) { var tr = el('tr'); r.forEach(function (v) { tr.appendChild(el('td', v == null ? '' : v, 'l')); }); tb.appendChild(tr); });
      t.appendChild(tb); box.appendChild(t);
    }).catch(function (e) { msg(e.message); });
  }
  var polling = null, wasRunning = false;
  function live() {
    return api('GET', '/api/crawl/progress').then(function (d) {
      var box = $('crawl-live'), a = d.active, st = d.stats || {}, last = st.last_run;
      var cb = $('crawl-credits'), cr = d.credits || {}, u = d.usage || {};
      cb.textContent = '';
      if (cr.error) {
        cb.appendChild(el('span', 'Credits could not be read: ' + cr.error +
          ' — check FIRECRAWL_API_KEY in Render → Environment.', 'bad'));
      } else {
        var item = function (label, value) { var s = el('span'); s.appendChild(el('b', value)); s.appendChild(document.createTextNode(' ' + label)); cb.appendChild(s); };
        item('credits left', (cr.remaining != null ? cr.remaining.toLocaleString() : '—') + (cr.plan ? ' / ' + cr.plan.toLocaleString() : ''));
        item('used this billing period', (u.this_period || 0).toLocaleString());
        if (u.this_run != null) item('used by this run so far', (u.this_run || 0).toLocaleString());
        item('used all time', (u.all_time || 0).toLocaleString());
        if (cr.period_end) item('renews', cr.period_end.slice(0, 10));
      }
      if (d.running && a) {
        box.className = 'live running';
        var pct = a.n ? Math.round(100 * (a.completed || 0) / a.n) : 0;
        $('live-title').textContent = a.kind === 'follow' ? 'Running — reading linked pages' : 'Running at Firecrawl';
        $('live-detail').textContent = (a.completed || 0) + ' of ' + a.n + ' pages done · ' + (a.credits_used || 0) +
          ' credits used · started ' + (a.started || '').slice(11, 16) + ' UTC · updates every 6 s';
        $('live-bar').style.width = pct + '%';
      } else if (last) {
        box.className = 'live ' + (last.status === 'completed' ? 'done' : (last.status === 'running' ? 'running' : 'failed'));
        $('live-title').textContent = last.status === 'completed' ? 'Finished' : ('Last run: ' + last.status);
        $('live-detail').textContent = (last.finished ? last.finished.slice(0, 16).replace('T', ' ') + ' UTC · ' : '') +
          (last.returned || 0) + ' of ' + (last.sent || 0) + ' sites read · ' + (last.linked_pages || 0) + ' linked pages · ' +
          last.businesses_with_facts + ' businesses with facts · ' + last.facts_filled + ' facts · ' + last.credits_used + ' credits';
        $('live-bar').style.width = '100%';
      } else {
        box.className = 'live idle'; $('live-title').textContent = 'Idle';
        $('live-detail').textContent = 'No runs yet.'; $('live-bar').style.width = '0';
      }
      // statistics
      var t = $('stat-totals'); t.textContent = '';
      stat(t, (st.businesses_read || 0).toLocaleString(), 'businesses read');
      stat(t, (st.pages_read || 0).toLocaleString(), 'pages read');
      stat(t, (st.facts_filled || 0).toLocaleString(), 'facts filled');
      stat(t, st.stored_in_bucket == null ? '—' : st.stored_in_bucket.toLocaleString(), 'pages saved in the bucket');
      $('stat-last').textContent = last ? ('Latest run: ' + last.sent + ' sites sent, ' + last.returned + ' read, ' +
        last.businesses_with_facts + ' businesses with facts, ' + last.avg_fields_per_business + ' fields filled per business on average.') : '';
      var f = $('stat-fields'); f.textContent = '';
      f.appendChild(el('p', 'How many businesses have each field (yes/no fields count only a stated yes):', 'note'));
      (st.field_fill || []).forEach(function (x) {
        var row = el('div', null, 'fbar'); row.appendChild(el('span', x.field.replace(/_/g, ' ')));
        var tr = el('div', null, 't'), fill = el('div'); fill.style.width = x.pct + '%'; tr.appendChild(fill); row.appendChild(tr);
        row.appendChild(el('span', x.filled + ' · ' + x.pct + '%', 'muted')); f.appendChild(row);
      });
      if (wasRunning && !d.running) { msg('Finished — results are saved.'); load(); }
      wasRunning = d.running;
      clearTimeout(polling);
      if (d.running && !document.hidden) polling = setTimeout(live, 6000);
    }).catch(function (e) { $('live-detail').textContent = e.message; });
  }
  document.addEventListener('visibilitychange', function () { if (!document.hidden && wasRunning) live(); });
  function act(p, method, path, body, done) {
    msg(p); api(method, path, body).then(function (r) { msg(done || 'Done.'); load(); }).catch(function (e) { msg(e.message); });
  }
  $('crawl-unlock').onclick = function () { token = $('crawl-token').value || token; load(); };
  if (token) { $('crawl-token').value = token; }
  $('crawl-limit').oninput = estimate; $('cfg-mode').onchange = estimate; $('cfg-follow').oninput = estimate;
  $('crawl-start').onclick = function () {
    var k = parseInt($('crawl-limit').value, 10);
    if (!confirm('Send ' + k + ' sites to Firecrawl? ' + $('crawl-estimate').textContent + '.')) return;
    var box = $('crawl-live'); box.className = 'live running';
    $('live-title').textContent = 'Starting…'; $('live-detail').textContent = 'sending ' + k + ' sites to Firecrawl'; $('live-bar').style.width = '2%';
    act('Starting…', 'POST', '/api/crawl/start', {limit: k}, 'Started — the status above updates by itself.');
    wasRunning = true;
  };
  $('crawl-stop').onclick = function () { if (confirm('Stop the run? What it finished is kept.')) act('Stopping…', 'POST', '/api/crawl/stop'); };
  $('crawl-collect').onclick = function () { act('Checking Firecrawl…', 'POST', '/api/crawl/collect'); };
  $('check-start').onclick = function () { act('Starting the check…', 'POST', '/api/crawl/check', {limit: parseInt($('check-limit').value, 10)}, 'Checking — see Collection below.'); };
  $('cfg-add').onclick = function () { fieldRow({type: 'text'}); };
  $('cfg-save').onclick = function () {
    var fields = [];
    Array.prototype.forEach.call($('cfg-fields').rows, function (tr) {
      var i = tr.querySelectorAll('input,select');
      if (i[0].value.trim()) fields.push({name: i[0].value, type: i[1].value, description: i[2].value});
    });
    var split = function (v) { return v.split(',').map(function (x) { return x.trim(); }).filter(Boolean); };
    act('Saving…', 'POST', '/api/crawl/config', {mode: $('cfg-mode').value, prompt: $('cfg-prompt').value, fields: fields,
      recrawl_days: parseInt($('cfg-recrawl').value, 10), default_batch: parseInt($('crawl-limit').value, 10),
      include_blocked: $('cfg-blocked').checked, evidence: $('cfg-evidence').checked,
      follow_links: parseInt($('cfg-follow').value || '0', 10),
      filters: {categories: split($('cfg-cats').value), ntas: split($('cfg-ntas').value), min_confidence: parseFloat($('cfg-conf').value)}}, 'Saved.');
  };
  $('crawl-csv').onclick = function (ev) {
    ev.preventDefault();
    fetch('/api/crawl/results.csv', {headers: {'X-Fetch-Token': token}}).then(function (r) {
      if (!r.ok) throw new Error('not allowed'); return r.blob(); }).then(function (b) {
      var a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = 'queens_web_facts.csv'; a.click();
    }).catch(function (e) { msg(e.message); });
  };
})();
</script>"""


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
    return render()


def render(sql: str = "", result: str = "") -> str:
    """The page. `result` is TRUSTED HTML built by /query — which is exactly
    why it must never come from the URL. Until 2026-10-17 this function was
    the "/" route itself, so GET /?result=<script>… put a stranger's script
    on the page (reflected XSS, found in review)."""
    rows_log = loaded()
    was_busy = busy()
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
        if busy() and title in _seen["panels"]:
            cols, rws = _seen["panels"][title]
            was_busy = True
        elif cols and cols != ["error"]:
            _seen["panels"][title] = (cols, rws)
        cards.append(f'<div class="card"><h3>{html.escape(title)}</h3>'
                     + table_html(cols, rws) + "</div>")

    # ── sources, loaded ones first ───────────────────────────────────────
    ordered = sorted(S.ALL, key=lambda x: (x.key not in have, x.label))
    got = {k: n for k, n, *_ in rows_log}
    srows = []
    for src in ordered:
        on = src.key in have
        # What is actually loaded; the probe figure only until it is.
        n = f"{got[src.key]:,}" if on else (f"{src.probed:,}" if src.probed else "—")
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

    notice = ""
    if was_busy:
        saving = sorted({j["source"] for j in jobs.active_here() if j.get("step") == "saving"})
        what = (", ".join(f"<code>{html.escape(k)}</code>" for k in saving)
                if saving else "a table")
        notice = (f'<div class="busy">The database is busy saving {what}. '
                  + ("The figures below are from just before; reload in a minute for the new ones."
                     if rows_log else "Its figures will appear when it finishes — reload in a minute.")
                  + "</div>")

    stats = f"""<div class="stats">
      <div class="stat"><b>{len(have)}<small style="font-size:15px;color:#98a0ac;font-weight:500">/{len(S.ALL)}</small></b>
        <span>sources loaded</span></div>
      <div class="stat"><b>{total:,}</b><span>Queens rows</span></div>
      <div class="stat"><b>82</b><span>neighbourhoods</span></div>
      <div class="stat"><b>{html.escape(last or "—")}</b><span>last fetched (UTC)</span></div>
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
    <a href="#neighbourhoods">Neighbourhoods</a>
    <a href="#sources">Sources</a>
    <a href="#websites">Websites</a>
    <a href="#collection">Collection</a>
    <a href="#query">Query</a>
    <a href="/api/status">API</a>
  </nav>
</div></header>

<div class="wrap">
  {notice}
  <section id="findings">
    <h2>Findings</h2>
    <p class="note">Computed live from the tables below. Each card re-runs its query on load.</p>
    {''.join(cards)}
  </section>

  <section id="neighbourhoods">
    <h2>Neighbourhoods</h2>
    <p class="note">Every source side by side, for each of Queens' 82 neighbourhoods (2020
      boundaries). Click a column to sort. Counts are of public records; nothing is estimated.</p>
    {neighbourhoods_html()}
  </section>

  <section id="sources">
    <h2>Sources</h2>
    <p class="note">All {len(S.ALL)} are <b>free and public</b> — city, state and federal records.
      Nothing is bought, and nothing here trains BizGenome's model. A green dot means loaded.</p>
    {src_table}
    <p class="note" style="margin-top:14px">Pull on a laptop with <code>python fetch.py</code>;
      the results reach this page through the bucket.</p>
  </section>

  <section id="websites">
    <h2>Business websites</h2>
    <p class="note">Every Queens business that lists a website, checked for free: still alive, dead
      (domain gone — often a closure), parked or for sale, taken over by spam, or really a platform
      page. Then <b>Firecrawl</b> reads the alive ones for the facts set in the requirements.
      Research only — kept apart from the public records.</p>
    <div id="crawl-overview" class="empty">Loading…</div>
    <details class="defs" id="crawl-canpull"><summary>What the scraper can pull</summary><ul></ul></details>
    <details class="panel" id="crawl-panel"><summary>Control panel</summary>
      <div class="pnl-login">
        <input id="crawl-token" type="password" placeholder="Password (FETCH_TOKEN)" autocomplete="current-password">
        <button type="button" id="crawl-unlock">Unlock</button>
        <span class="muted" id="crawl-msg"></span>
      </div>
      <div id="crawl-admin" hidden>
        <div class="stats" id="crawl-stats"></div>
        <div id="crawl-credits" class="credits"></div>
        <div id="crawl-live" class="live idle"><span class="dot"></span><b id="live-title">Idle</b>
          <span id="live-detail" class="muted"></span>
          <div class="bar"><div id="live-bar"></div></div></div>
        <h3>Run</h3>
        <div class="pnl-row">
          <label>Sites <input id="crawl-limit" type="number" min="1" max="1000" value="50"></label>
          <span id="crawl-estimate" class="muted"></span>
          <button type="button" id="crawl-start">Start</button>
          <button type="button" id="crawl-stop" class="ghost">Stop</button>
          <button type="button" id="crawl-collect" class="ghost">Collect now</button>
        </div>
        <div id="crawl-runs"></div>
        <h3>Free website check</h3>
        <div class="pnl-row">
          <label>Sites <input id="check-limit" type="number" min="1" max="50000" value="2000"></label>
          <button type="button" id="check-start" class="ghost">Check now</button>
          <span class="muted">Re-checks sites not checked in 30 days. Progress shows in Collection.</span>
        </div>
        <h3>Requirements — what to extract</h3>
        <div class="pnl-row">
          <label>Mode <select id="cfg-mode"><option value="structured">Structured facts (5 credits/page)</option>
            <option value="text">Page text only (1 credit/page)</option></select></label>
          <label>Re-read after <input id="cfg-recrawl" type="number" min="1" max="365"> days</label>
          <label>Min. confidence <input id="cfg-conf" type="number" min="0" max="1" step="0.05"></label>
          <label><input id="cfg-blocked" type="checkbox"> also try sites with a bot wall (often fail, still cost)</label>
          <label>Linked pages per site <input id="cfg-follow" type="number" min="0" max="2"></label>
          <label><input id="cfg-evidence" type="checkbox"> keep the quote behind each fact</label>
        </div>
        <label class="blk">Instructions to the extractor<textarea id="cfg-prompt" rows="2"></textarea></label>
        <table class="cfg"><thead><tr><th class="l">Field</th><th class="l">Type</th>
          <th class="l">What to find</th><th></th></tr></thead><tbody id="cfg-fields"></tbody></table>
        <div class="pnl-row">
          <button type="button" id="cfg-add" class="ghost">+ Add a field</button>
          <label>Only categories <input id="cfg-cats" placeholder="e.g. restaurant, casual_eatery"></label>
          <label>Only neighbourhoods <input id="cfg-ntas" placeholder="e.g. QN0702, QN2501"></label>
          <button type="button" id="cfg-save">Save requirements</button>
        </div>
        <h3>Statistics</h3>
        <div class="stats" id="stat-totals"></div>
        <p class="note" id="stat-last"></p>
        <div id="stat-fields"></div>
        <h3>Latest results <a href="#" id="crawl-csv">download CSV</a></h3>
        <div class="scroll" id="crawl-results"></div>
      </div>
    </details>
  </section>

  <section id="collection">
    <h2>Collection</h2>
    <p class="note">Every pull and every upload, newest first — from this server and from any
      laptop that pulls. Times are UTC. <b>Succeeded</b> means the rows were saved; the GCS
      column says separately whether they reached the bucket. A job that stops without
      reporting is marked as such, never left looking like a success.</p>
    {collection_html()}
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
</div>
{NB_JS}
{CRAWL_JS}
<script>
/* Keep the collection table current every 10 s, but ONLY while something is
   queued or running, and only while the tab is visible. Polling forever kept
   the free server from ever sleeping. The server renders and escapes it. */
(function () {{
  var box = document.getElementById('jobs');
  if (!box) return;
  var timer = null;
  function active() {{ return box.getAttribute('data-active') === '1'; }}
  function schedule() {{
    clearTimeout(timer);
    if (active() && !document.hidden) timer = setTimeout(tick, 10000);
  }}
  function tick() {{
    fetch('/collection', {{cache: 'no-store'}})
      .then(function (r) {{ return r.ok ? r.text() : null; }})
      .then(function (h) {{
        if (!h) return;
        var t = document.createElement('div');
        t.innerHTML = h;
        var fresh = t.querySelector('#jobs');
        if (fresh) {{ box.replaceWith(fresh); box = fresh; }}
      }})
      .catch(function () {{}})
      .then(schedule);
  }}
  document.addEventListener('visibilitychange', schedule);
  schedule();
}})();
</script>
</body></html>"""


@app.get("/api/status")
def status():
    rows = loaded()
    act = [j for j in jobs.listing(60) if j["state"] in ("queued", "running", "stalled")]
    return {"sources_declared": len(S.ALL),
            "sources_loaded": len(rows),
            "rows": sum(r[1] for r in rows),
            "tables": [{"key": k, "rows": n, "bytes": b, "fetched": t} for k, n, b, t in rows],
            "fetching": sorted({j["source"] for j in act
                                if j["host"] == jobs.host() and j["state"] in jobs.ACTIVE}),
            "jobs_active": [{k: j.get(k) for k in ("source", "state", "step", "rows_downloaded",
                                                   "host", "origin", "last_activity")}
                            for j in act],
            "database": "local duckdb (no external database)",
            "bucket": (f"gs://{__import__('gcs').bucket()}" if __import__('gcs').enabled()
                       else "not configured"),
            "loaded_from": _loaded_from,
            "database_busy": busy(),
            "commit": (os.environ.get("RENDER_GIT_COMMIT") or "local")[:7],
            "checked": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def _admin_error(request: Request):
    """None if the request carries the admin token; else the 403 to return.

    Anything that starts work (a pull, a crawl, a site check) or shows scraped
    content needs FETCH_TOKEN (Render env) in an X-Fetch-Token header. With no
    token configured those actions are simply off. Anonymous pulls once let
    anyone start eleven at once on a 512 MB server (review, 2026-10-17)."""
    token = os.environ.get("FETCH_TOKEN") or ""
    given = request.headers.get("x-fetch-token") or ""
    if not token:
        return JSONResponse({"error": "the control panel is off on this server: set FETCH_TOKEN "
                             "in Render's Environment tab"}, status_code=403)
    if not hmac.compare_digest(given.encode(), token.encode()):
        return JSONResponse({"error": "wrong password"}, status_code=403)
    return None


@app.post("/fetch/{key}")
async def fetch(key: str, request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    if key not in S.BY_KEY and key not in ("core", "all"):
        return JSONResponse({"error": f"unknown source '{key}'"}, status_code=404)
    # The big ones would run this 512 MB dyno out of memory, or be cut off when
    # it sleeps. They are pulled on a laptop and reach this page via the bucket.
    if key == "all" or "local" in getattr(S.BY_KEY.get(key), "tags", ()):
        return JSONResponse({"error": f"'{key}' is too big to pull on this server. "
                             "Run fetch.py on a laptop; it reaches this page through "
                             "the bucket."}, status_code=409)
    keys = [s.key for s in S.core()] if key == "core" else [key]
    with _lock:
        # One web run at a time, whatever the key: two saves at once already
        # come close to the free server's 512 MB.
        running = sorted({j["source"] for j in jobs.active_here()})
        if running:
            return JSONResponse({"status": "busy", "key": key, "running": running,
                                 "error": "a pull is already running here; try when it ends"},
                                status_code=409)
        run = jobs.new_run()
        for k in keys:                 # queued now, so the page shows them at once
            jobs.queue(k, run, "web")
    jobs.mirror_soon()

    def go():
        arg = "--core" if key == "core" else f"--only={key}"
        code = None
        try:
            # --force: after a rebuild every table already counts as fetched, so
            # without it a live refresh silently did nothing and never reached
            # the bucket. --run hands the process the jobs queued above.
            code = subprocess.run([sys.executable, str(HERE / "fetch.py"), arg, "--force",
                                   f"--run={run}"], cwd=HERE, timeout=1800,
                                  check=False).returncode
        except subprocess.TimeoutExpired:
            code = "timeout"
        except Exception as e:  # noqa: BLE001
            code = type(e).__name__
        finally:
            # Whatever the process did not close itself, it did not finish. An
            # empty "running" list is no longer read as success.
            jobs.reap(run, code)

    asyncio.get_running_loop().run_in_executor(None, go)
    return {"status": "queued", "key": key, "run": run, "poll": "/api/jobs"}


# ── business websites: the free check + Firecrawl (crawlkit.py) ─────────
def _ck():
    import crawlkit
    return crawlkit


def _crawl_call(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 — shown in the panel, never a 500
        return JSONResponse({"error": jobs.clean(f"{e}")}, status_code=400)


@app.get("/api/crawl/overview")
def crawl_overview():
    """Public: counts only — no URLs, no extracted text."""
    ck = _ck()
    return _crawl_call(lambda: {**ck.overview(), "can_pull": ck.CAN_PULL})


@app.get("/api/crawl/admin")
def crawl_admin(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    ck = _ck()

    def build():
        try:
            cr = ck.credits()
        except Exception as e:  # noqa: BLE001
            cr = {"error": jobs.clean(str(e))}
        c = ck.config()
        cols, rows = ck.results(50)
        ok = ck.CRAWLABLE if c.get("include_blocked") else ("alive", "alive_js")
        waiting = ck.query(f"""SELECT count(DISTINCT domain) FROM site_checks
                               WHERE status IN {ok}""")[0][0]
        return {"credits": cr, "config": c, "credits_per_page": ck.CREDITS,
                "field_types": list(ck.FIELD_TYPES), "crawlable_sites": waiting,
                "runs": [{k: v for k, v in r.items() if k != "urls"} for r in ck.runs()][-10:],
                "results": {"columns": cols, "rows": [[None if v is None else str(v) for v in r] for r in rows]}}
    return _crawl_call(build)


@app.get("/api/crawl/progress")
def crawl_progress(request: Request):
    """The panel's live status: polled every few seconds while a run is going."""
    denied = _admin_error(request)
    if denied:
        return denied
    return _crawl_call(lambda: _ck().progress())


@app.post("/api/crawl/config")
async def crawl_config(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    body = await request.json()
    return _crawl_call(lambda: {"saved": _ck().save_config(body)})


@app.post("/api/crawl/start")
async def crawl_start(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    body = await request.json()
    limit = int(body.get("limit") or 0) or None
    return _crawl_call(lambda: {"started": _ck().start(limit, origin="web")})


@app.post("/api/crawl/stop")
def crawl_stop(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    return _crawl_call(lambda: {"stopped": _ck().stop_run()})


@app.post("/api/crawl/collect")
def crawl_collect(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    return _crawl_call(lambda: {"runs": _ck().refresh()})


@app.post("/api/crawl/check")
async def crawl_check(request: Request):
    """The free website check, in the background (a subprocess, like /fetch)."""
    denied = _admin_error(request)
    if denied:
        return denied
    body = await request.json()
    limit = max(1, min(int(body.get("limit") or 2000), 50000))
    with _lock:
        running = sorted({j["source"] for j in jobs.active_here() if j.get("step") != jobs.REMOTE_STEP})
        if running:
            return JSONResponse({"error": f"busy: {', '.join(running)} is running here"}, status_code=409)
        run = jobs.new_run()
        jobs.queue("site_checks", run, "web")
    jobs.mirror_soon()

    def go():
        code = None
        try:
            code = subprocess.run([sys.executable, str(HERE / "crawl.py"), "check", f"--limit={limit}",
                                   f"--run={run}"], cwd=HERE, timeout=3600, check=False).returncode
        except subprocess.TimeoutExpired:
            code = "timeout"
        except Exception as e:  # noqa: BLE001
            code = type(e).__name__
        finally:
            jobs.reap(run, code)

    asyncio.get_running_loop().run_in_executor(None, go)
    return {"status": "queued", "limit": limit, "poll": "/api/jobs"}


@app.get("/api/crawl/results.csv")
def crawl_results_csv(request: Request):
    denied = _admin_error(request)
    if denied:
        return denied
    import csv
    import io
    from fastapi.responses import Response
    cols, rows = _ck().results(100_000)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    w.writerows(rows)
    return Response(buf.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=queens_web_facts.csv"})


def _crawl_collector():
    """Collect finished Firecrawl runs while this server is awake: Firecrawl
    keeps results ~24 h, so nobody has to remember to press Collect."""
    import time as _time
    while True:
        _time.sleep(120)
        try:
            if os.environ.get("FIRECRAWL_API_KEY"):
                ck = _ck()
                if any(not r.get("collected_at") for r in ck.runs()):
                    ck.refresh()
        except Exception as e:  # noqa: BLE001
            print(f"[crawl] collector: {type(e).__name__}: {jobs.clean(e)}", flush=True)


@app.on_event("startup")
def _start_collector():
    threading.Thread(target=_crawl_collector, daemon=True, name="crawl-collector").start()


@app.get("/collection", response_class=HTMLResponse)
def collection():
    """The collection table on its own — the page polls this."""
    return collection_html()


@app.get("/api/jobs")
def api_jobs(limit: int = 100):
    return {"host": jobs.host(), "jobs": jobs.listing(max(1, min(limit, 200))),
            "checked": datetime.now(timezone.utc).isoformat(timespec="seconds")}

def _one_read(sql: str) -> str | None:
    """None if `sql` is exactly one read statement; otherwise the reason not.

    ⚠️ This used to check only the first word, so `SELECT 1; COPY ...` passed
    and DuckDB ran both. DuckDB's own parser decides now. DESCRIBE, SHOW and
    SUMMARIZE all parse as SELECT."""
    import duckdb
    try:
        stmts = duckdb.extract_statements(sql)
    except Exception as e:  # noqa: BLE001 — a syntax error is shown, not raised
        return str(e)[:200]
    if len(stmts) != 1:
        return "One statement at a time."
    if stmts[0].type.name != "SELECT":
        return "Only read queries are allowed (SELECT, WITH, DESCRIBE, SHOW, SUMMARIZE)."
    used = _functions(sql)
    if used is None:
        return "That query can't be checked here. Use SELECT, DESCRIBE or SUMMARIZE."
    blocked = sorted(used & BLOCKED_FUNCTIONS)
    if blocked:
        return f"{', '.join(blocked)} is not available here."
    return None


# duckdb_secrets / which_secret show the bucket key's id. query / query_table
# run SQL built from a string, which no check can see into — a plain text
# filter on "duckdb_secrets" was bypassed with query('… duckdb_' || 'secrets()').
BLOCKED_FUNCTIONS = {"duckdb_secrets", "which_secret", "query", "query_table"}


def _functions(sql: str) -> set[str] | None:
    """Every function a SELECT calls, read from DuckDB's own parse tree —
    subqueries, CTEs, LATERAL and DESCRIBE/SUMMARIZE wrappers included.
    None if the statement can't be serialised (then it isn't run)."""
    import duckdb
    import json
    try:
        tree = json.loads(duckdb.connect().execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    except Exception:  # noqa: BLE001
        return None
    if tree.get("error"):
        return None
    out: set[str] = set()

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "function_name" and isinstance(v, str):
                    out.add(v.lower())
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(tree)
    return out


@app.get("/query", response_class=HTMLResponse)
def query(sql: str = ""):
    """Read-only SQL over the research tables.

    This page is PUBLIC. The data is all public records, so exposure is not the
    worry — abuse is. The connection is read-only, only read statements are
    accepted, and results are capped.
    """
    sql = (sql or "").strip().rstrip(";")
    if not sql:
        return render()
    why = _one_read(sql)
    if why:
        return render(sql, f'<div class="empty">{html.escape(why)}</div>')
    cols, rows = q(sql, limit=200)
    if busy():
        return render(sql, '<div class="empty">The database is busy saving a table — '
                           'run this again in a minute.</div>')
    if cols == ["error"]:
        return render(sql, f'<div class="empty">{html.escape(str(rows[0][0]))}</div>')
    note = f'<p class="note">{len(rows)} row(s){" · capped at 200" if len(rows) == 200 else ""}</p>'
    return render(sql, note + table_html(cols, rows))
