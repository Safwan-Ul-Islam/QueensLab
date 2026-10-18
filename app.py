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
.card{border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin:0 0 14px;overflow-x:auto}
nav{max-width:100%;overflow-x:auto}
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
#crawl-results table.preview{table-layout:fixed}
#crawl-results table.preview td{white-space:nowrap;font-size:12.5px;overflow:hidden;text-overflow:ellipsis}
#crawl-results table.preview th:first-child{width:26%}
@media (max-width:600px){#crawl-results table.preview{min-width:640px}}
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
.seg{display:inline-flex;border:1px solid #dde2ea;border-radius:999px;padding:3px;margin:4px 0 6px;background:#fff}
.seg button{margin:0;background:transparent;color:var(--muted);border-radius:999px;padding:8px 18px;font-size:13.5px}
.seg button.on{background:var(--ink);color:#fff}
#q-examples button{font-weight:500}
#q-out td{white-space:normal;min-width:100px;font-size:12.5px}
.chat{display:flex;flex-direction:column;gap:10px;margin:10px 0 4px}
.chat:empty{display:none}
.msg{max-width:88%;border-radius:14px;padding:11px 14px;font-size:14.5px;line-height:1.55;white-space:pre-wrap}
.msg.user{align-self:flex-end;background:var(--ink);color:#fff;border-bottom-right-radius:4px}
.msg.bot{align-self:flex-start;background:var(--soft);border:1px solid var(--line);border-bottom-left-radius:4px;white-space:normal}
.msg.bot .text{white-space:pre-wrap}
.msg.bot .flag{margin-top:8px;font-size:12.5px;color:#9a5b00;background:#fff4e0;border-radius:8px;padding:6px 9px}
.msg.bot .sugg{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
.msg.bot .sugg button{margin:0;background:#fff;color:var(--ink);border:1px solid #d5dbe4;font-weight:500;
 font-size:12.5px;padding:6px 12px;border-radius:999px;text-align:left}
.msg.bot .sugg button:hover{background:var(--soft);border-color:#b9c1cd}
.msg.bot details{margin-top:10px;font-size:12.5px}
.msg.bot details summary{cursor:pointer;color:var(--muted);font-weight:600}
.msg.bot pre{white-space:pre-wrap;font:12px 'SFMono-Regular',Menlo,monospace;background:#fff;border:1px solid var(--line);
 border-radius:8px;padding:8px 10px;margin:8px 0}
.msg.bot .meta{font-size:11.5px;color:var(--faint);margin-top:6px}
.msg.thinking{color:var(--muted);font-style:italic}
.chatbar span{font-size:12.5px;max-width:70ch}
.ask{display:flex;gap:8px;margin:8px 0}
.ask input{flex:1;font:16px 'Inter',sans-serif;padding:15px 18px;border:1.5px solid #cfd5df;
 border-radius:12px;min-height:54px}
.ask input:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:transparent}
.ask button{margin-top:0;padding:0 28px;font-size:15px;border-radius:12px}
.ask-out{border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin:8px 0 12px;background:var(--soft)}
.ask-out pre{white-space:pre-wrap;font:12px 'SFMono-Regular',Menlo,monospace;background:#fff;
 border:1px solid var(--line);border-radius:8px;padding:9px 11px;margin:8px 0}
.ask-out .why{font-weight:600}
#collection:not(.show-all) #jobs tr.more{display:none}
button.toggle{margin-top:10px;background:#fff;color:var(--ink);border:1px solid #dde2ea;padding:8px 18px;font-size:13px}
button.toggle:hover{background:#f3f5f8;color:var(--ink)}

/* ── Firecrawl: deep, vivid orange and white ─────────────────────────── */
/* Text sits only on the deep oranges (white on #7C2D12..#C2410C = 9.4..5.2:1).
   The bright ones (#EA580C, #F97316: 3.6 / 2.8:1 under white) are for bars,
   borders and dots — never under text (accessibility review, 2026-10-18). */
#websites{--fc-900:#7C2D12;--fc-800:#9A3412;--fc-700:#C2410C;--fc-600:#EA580C;--fc-500:#F97316;--fc-200:#FED7AA;
 --fc-100:#FFEDD5;--fc-50:#FFF7ED}
#websites .fc-hero{position:relative;overflow:hidden;color:#fff;border-radius:16px;padding:22px 24px 20px;
 background:linear-gradient(120deg,var(--fc-900),var(--fc-800) 30%,var(--fc-700) 55%,#B4380D 75%,var(--fc-800));
 background-size:260% 260%;animation:fcflow 14s ease-in-out infinite;box-shadow:0 10px 30px -14px rgba(194,65,12,.55)}
#websites .fc-hero h2{color:#fff;font-size:21px}
#websites .fc-hero .note{color:#fff;margin:6px 0 0;max-width:80ch}
#websites .fc-hero .note b{color:#fff}
#websites .fc-tag{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
 background:#fff;color:var(--fc-800);border-radius:999px;padding:3px 10px;margin-bottom:8px}
@keyframes fcflow{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
@media (prefers-reduced-motion:reduce){#websites .fc-hero{animation:none}}
#websites .stat{background:#fff;border:1px solid var(--fc-200);box-shadow:inset 0 3px 0 var(--fc-600)}
#websites .stat b{color:var(--fc-700)}
#websites .stat span{color:#8A5A3C}
#websites .defs summary,#websites .panel>summary{color:var(--fc-700)}
#websites .panel{border:1.5px solid var(--fc-200);background:#fff}
#websites .panel>summary{font-size:15px;padding:4px 0}
#websites .panel[open]>summary{padding-bottom:8px}
#websites .panel h3{color:var(--fc-800)}
#websites button{background:var(--fc-700);color:#fff}
#websites button:hover{background:var(--fc-800)}
#websites button.ghost{background:#fff;color:var(--fc-700);border:1px solid var(--fc-200)}
#websites button.ghost:hover{background:var(--fc-50);border-color:var(--fc-500)}
#websites input:focus,#websites select:focus,#websites textarea:focus,#websites button:focus-visible{
 outline:2px solid var(--fc-700);outline-offset:2px}
#websites textarea{background:var(--fc-50)}
#websites a,#websites a.csv{color:var(--fc-700);font-weight:600}
#websites th{border-bottom-color:var(--fc-700)}
#websites .credits b{color:var(--fc-700)}
#websites .live{background:var(--fc-50);border-color:var(--fc-200)}
#websites .live.running .dot{background:var(--fc-600)}
#websites .live .bar{background:var(--fc-100)}
#websites .live .bar div{background:linear-gradient(90deg,var(--fc-600),var(--fc-500))}
#websites .live.done .bar div{background:var(--fc-700)}
#websites .live.done .dot{background:var(--fc-700)}
#websites input[type=checkbox]{accent-color:var(--fc-700)}
#websites .fbar .t{background:var(--fc-100)}
@media (max-width:600px){.fbar{grid-template-columns:110px 1fr 58px;gap:8px;font-size:11.5px}}
#websites .fbar .t div{background:linear-gradient(90deg,var(--fc-700),var(--fc-500))}
#websites .empty{background:var(--fc-50);border-color:var(--fc-200)}
.seg button#scope-firecrawl.on{background:#C2410C}
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


SHOW_JOBS = 5      # newest jobs shown before "Show all"


def collection_html(limit: int = 200) -> str:
    """Source | Status | Rows downloaded | Started | Last activity | Finished |
    GCS upload | Error — every machine's jobs, newest first.

    Never raises: rows can come from another machine's mirror, and one odd row
    must cost at most that row, not the whole page."""
    try:
        rows = jobs.listing(limit)
        capped = len(rows) >= limit
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
                row = _job_row(j)
            except Exception:  # noqa: BLE001 — a malformed row is skipped, not fatal
                continue
            # Beyond the newest SHOW_JOBS, rows wait behind "Show all" — except a
            # job still running, which is always in view.
            if len(trs) >= SHOW_JOBS and j.get("state") not in ("queued", "running", "stalled"):
                row = row.replace("<tr>", '<tr class="more">', 1)
            trs.append(row)
        body = ('<div class="scroll"><table><thead><tr><th class="l">Source</th>'
                '<th class="l">Status</th><th>Rows downloaded</th><th>Started</th>'
                '<th>Last activity</th><th>Finished</th><th class="l">GCS upload</th>'
                '<th class="l">Error</th></tr></thead><tbody>'
                + "".join(trs) + "</tbody></table></div>")
    return (f'<div id="jobs" data-active="{1 if active else 0}" data-total="{len(trs) if rows else 0}" '
            f'data-capped="{1 if rows and capped else 0}">{body}</div>')


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
    stat(box, (o.with_facts || 0).toLocaleString(), 'pages with facts');
    var ul = $('crawl-canpull').querySelector('ul');
    (o.can_pull || []).forEach(function (p) { var li = el('li'); li.appendChild(el('b', p[0])); li.appendChild(document.createTextNode(' — ' + p[1])); ul.appendChild(li); });
  }).catch(function () { $('crawl-overview').textContent = 'Not available right now.'; });

  var cfg = null, per = {structured: 5, text: 1, claude: 1}, types = ['text', 'number', 'yes/no', 'list', 'prices'];
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
    var claude = m === 'structured' && $('cfg-extractor').value === 'claude';
    $('cfg-extractor').disabled = m !== 'structured';
    var rate = m === 'text' ? per.text : (claude ? per.claude : per.structured);
    // ~0.12¢ a page measured on 20 pages (Haiku 5.5 prices); 0.2¢ leaves room for long pages
    $('crawl-estimate').textContent = 'up to ' + (k * rate * (1 + f)).toLocaleString() + ' credits' +
      (claude ? ' + about $' + Math.max(0.01, k * (1 + f) * 0.002).toFixed(2) + ' of Claude' : '');
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
      var pe = d.credits && d.credits.period_end ? new Date(d.credits.period_end) : null;
      stat(s, pe ? pe.toLocaleDateString('en-GB', {day: 'numeric', month: 'short', timeZone: 'UTC'}) : '—', 'credits renew');
      var runs = $('crawl-runs'); runs.textContent = '';
      (d.runs || []).slice().reverse().slice(0, 5).forEach(function (r) {
        runs.appendChild(el('div', r.started.slice(0, 16).replace('T', ' ') + ' UTC · ' + r.n + ' sites · ' +
          (r.mode === 'text' ? 'text' : (r.extractor === 'claude' ? 'facts by Claude' : 'facts by Firecrawl')) + ' · ' +
          (r.status || '') + ' ' + (r.completed || 0) + '/' + r.n + ' · ' + (r.credits_used || 0) + ' credits' +
          (r.ai && r.ai.pages ? ' · Claude $' + r.ai.usd.toFixed(3) : '') +
          (r.collected_at ? ' · collected' : (r.expires_at ? ' · results kept until ' + r.expires_at.slice(0, 16).replace('T', ' ') : '')), 'muted'));
      });
      $('cfg-mode').value = cfg.mode; $('cfg-extractor').value = cfg.extractor || 'claude'; $('cfg-prompt').value = cfg.prompt;
      $('cfg-recrawl').value = cfg.recrawl_days; $('cfg-conf').value = cfg.filters.min_confidence;
      $('cfg-blocked').checked = !!cfg.include_blocked;
      $('cfg-follow').value = cfg.follow_links || 0; $('cfg-evidence').checked = !!cfg.evidence;
      $('cfg-cats').value = (cfg.filters.categories || []).join(', '); $('cfg-ntas').value = (cfg.filters.ntas || []).join(', ');
      $('cfg-fields').textContent = ''; cfg.fields.forEach(fieldRow);
      $('crawl-limit').value = cfg.default_batch; estimate();
      var res = d.results || {columns: [], rows: []}, box = $('crawl-results'); box.textContent = '';
      if (!res.rows.length) { box.appendChild(el('div', 'No results yet.', 'empty')); return; }
      box.appendChild(el('p', 'The latest ' + res.rows.length + ' of ' + (res.total || res.rows.length).toLocaleString() +
        ' businesses read. The CSV has all of them.', 'note'));
      // A compact preview: the key columns, long text cut short (full text on hover).
      // Every column, untrimmed, is in the CSV.
      // name + neighbourhood, then the 3 requirement fields most often filled in
      // these rows (so the preview follows whatever the requirements are), then the date.
      var skip = {name: 1, nta2020: 1, category: 1, pages_read: 1, evidence: 1, changed_since_last_read: 1, fetched_at: 1};
      var scored = res.columns.map(function (c, i) {
        return {i: i, n: skip[c] ? -1 : res.rows.filter(function (r) { return r[i] != null && r[i] !== ''; }).length};
      }).filter(function (x) { return x.n > 0; }).sort(function (a, b) { return b.n - a.n || a.i - b.i; }).slice(0, 3)
        .sort(function (a, b) { return a.i - b.i; }).map(function (x) { return x.i; });
      var idx = [res.columns.indexOf('name'), res.columns.indexOf('nta2020')].concat(scored, [res.columns.indexOf('fetched_at')])
        .filter(function (i) { return i >= 0; });
      var t = el('table', null, 'preview'), hr = el('tr');
      idx.forEach(function (i) { hr.appendChild(el('th', res.columns[i].replace(/_/g, ' '), 'l')); });
      var th = el('thead'); th.appendChild(hr); t.appendChild(th); var tb = el('tbody');
      res.rows.forEach(function (r) {
        var tr = el('tr');
        idx.forEach(function (i) {
          var v = r[i] == null ? '' : String(r[i]);
          if (res.columns[i] === 'fetched_at') v = v.slice(0, 10);
          var td = el('td', v, 'l');
          if (v) td.title = v;            // the cell may be cut by CSS: the full text is on hover
          tr.appendChild(td);
        });
        tb.appendChild(tr);
      });
      t.appendChild(tb); box.appendChild(t);
    }).catch(function (e) { msg(e.message); });
  }
  var polling = null, wasRunning = false;
  function live() {
    return api('GET', '/api/crawl/progress').then(function (d) {
      var box = $('crawl-live'), a = d.active, st = d.stats || {}, last = st.last_run;
      $('crawl-admin').hidden = false;
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
        if (u.ai_usd) item('of Claude for reading pages (recent runs)', '$' + u.ai_usd.toFixed(2));
        if (cr.period_end) item('renews', cr.period_end.slice(0, 10));
      }
      if (d.running && a) {
        box.className = 'live running';
        var x = a.extracting, pct = x ? (x.of ? Math.round(100 * x.done / x.of) : 100)
                                      : (a.n ? Math.round(100 * (a.completed || 0) / a.n) : 0);
        if (x) {
          $('live-title').textContent = 'Claude is reading the pages' + (a.kind === 'follow' ? ' (linked pages)' : '');
          $('live-detail').textContent = x.done + ' of ' + x.of + ' pages read by Claude · ' + (a.completed || 0) +
            ' fetched by Firecrawl · ' + (a.credits_used || 0) + ' credits' +
            (a.ai ? ' · $' + a.ai.usd.toFixed(3) + ' of Claude' : '') + ' · saved every 50 pages';
        } else {
          $('live-title').textContent = a.kind === 'follow' ? 'Running — reading linked pages' : 'Running at Firecrawl';
          $('live-detail').textContent = (a.completed || 0) + ' of ' + a.n + ' pages done · ' + (a.credits_used || 0) +
            ' credits used · started ' + (a.started || '').slice(11, 16) + ' UTC · updates every 6 s' +
            (a.extractor === 'claude' ? ' · then Claude reads them' : '');
        }
        $('live-bar').style.width = pct + '%';
      } else if (last) {
        box.className = 'live ' + (last.status === 'completed' ? 'done' : (last.status === 'running' ? 'running' : 'failed'));
        $('live-title').textContent = last.status === 'completed' ? 'Finished' : ('Last run: ' + last.status);
        $('live-detail').textContent = (last.finished ? last.finished.slice(0, 16).replace('T', ' ') + ' UTC · ' : '') +
          (last.returned || 0) + ' of ' + (last.sent || 0) + ' sites read · ' + (last.linked_pages || 0) + ' linked pages · ' +
          last.businesses_with_facts + ' businesses with facts · ' + last.facts_filled + ' facts · ' + last.credits_used + ' credits' +
          (last.ai_usd ? ' · $' + last.ai_usd.toFixed(3) + ' of Claude' : '');
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
      stat(t, st.stored_in_bucket == null ? '—' : st.stored_in_bucket.toLocaleString(), 'rows saved in the bucket (failed sites included)');
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
    // load() clears the message, so the result is shown after it (unless load reported an error)
    msg(p); api(method, path, body).then(function (r) {
      load().then(function () { if (!$('crawl-msg').textContent) msg(done || 'Done.'); }); live();
    }).catch(function (e) { msg(e.message); });
  }
  $('crawl-unlock').onclick = function () { token = $('crawl-token').value || token; load(); live(); };
  if (token) { $('crawl-token').value = token; }
  // A remembered password: opening the panel loads it, no Unlock press needed.
  $('crawl-panel').addEventListener('toggle', function () {
    if ($('crawl-panel').open && token && $('crawl-admin').hidden) { load(); live(); }
  });
  $('crawl-limit').oninput = estimate; $('cfg-mode').onchange = estimate; $('cfg-extractor').onchange = estimate; $('cfg-follow').oninput = estimate;
  $('crawl-start').onclick = function () {
    var k = parseInt($('crawl-limit').value, 10);
    if (!confirm('Send ' + k + ' sites to Firecrawl? ' + $('crawl-estimate').textContent + '.')) return;
    var box = $('crawl-live'); box.className = 'live running';
    $('live-title').textContent = 'Starting…'; $('live-detail').textContent = 'sending ' + k + ' sites to Firecrawl'; $('live-bar').style.width = '2%';
    act('Starting…', 'POST', '/api/crawl/start', {limit: k}, 'Started — the status above updates by itself.');
    wasRunning = true;
  };
  $('crawl-stop').onclick = function () { if (confirm('Stop the run? What it finished is kept.')) act('Stopping…', 'POST', '/api/crawl/stop'); };
  $('crawl-collect').onclick = function () { act('Checking Firecrawl…', 'POST', '/api/crawl/collect', undefined,
    'Checked. Anything finished is being collected — the status above shows it.'); };
  $('check-start').onclick = function () { act('Starting the check…', 'POST', '/api/crawl/check', {limit: parseInt($('check-limit').value, 10)}, 'Checking — see Collection below.'); };
  $('cfg-add').onclick = function () { fieldRow({type: 'text'}); };
  $('cfg-save').onclick = function () {
    var fields = [];
    Array.prototype.forEach.call($('cfg-fields').rows, function (tr) {
      var i = tr.querySelectorAll('input,select');
      if (i[0].value.trim()) fields.push({name: i[0].value, type: i[1].value, description: i[2].value});
    });
    var split = function (v) { return v.split(',').map(function (x) { return x.trim(); }).filter(Boolean); };
    act('Saving…', 'POST', '/api/crawl/config', {mode: $('cfg-mode').value, extractor: $('cfg-extractor').value, prompt: $('cfg-prompt').value, fields: fields,
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


JOBS_JS = r"""<script>
/* Collection: the newest 5 rows, "Show all" for the rest. The toggle lives
   outside the auto-refreshed table, and the class sits on the section, so a
   refresh never collapses what you opened. */
(function () {
  var sec = document.getElementById('collection'), btn = document.getElementById('jobs-toggle');
  if (!sec || !btn) return;
  function label() {
    var box = document.getElementById('jobs');
    var total = box ? parseInt(box.getAttribute('data-total') || '0', 10) : 0;
    var more = box ? box.querySelectorAll('tr.more').length : 0;
    btn.hidden = more === 0;
    var open = sec.classList.contains('show-all');
    var capped = box && box.getAttribute('data-capped') === '1';
    btn.textContent = open ? 'Show the latest 5' : (capped ? 'Show the latest ' + total : 'Show all ' + total);
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  }
  btn.onclick = function () { sec.classList.toggle('show-all'); label(); };
  new MutationObserver(label).observe(sec, {childList: true});
  label();
})();
</script>"""


QUERY_JS = r"""<script>
/* The one Query box: Free data / Firecrawl data / Mixed, for typed SQL and
   for plain-English questions. Everything returned is set as text, never HTML. */
(function () {
  var $ = function (id) { return document.getElementById(id); };
  if (!$('sql-box')) return;
  function el(tag, text, cls) { var e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
  var NOTES = {
    free: 'Public data, open to everyone: nta_profile (the 82 neighbourhoods side by side), overture_places, ' +
          'dcwp_licences, dohmh_food, sla_liquor, nys_retail_food, nyc_311, subway_ridership, pluto, dob_permits, ' +
          'dob_now, dof_sales, storefront_vacancy, acs_tracts and more.',
    firecrawl: 'The crawled websites only (password): web_latest (one row per business, a column per field), ' +
               'web_facts (every page read, with its full text in markdown), site_checks (is each website alive, dead, parked...).',
    mixed: 'Both together (password): the crawled tables plus every public table under its own name, e.g. ' +
           'join web_latest.nta2020 to nta_profile.code.'
  };
  var scope = 'free', examples = {}, last = null;
  try { scope = localStorage.getItem('ql_scope') || 'free'; } catch (e) {}
  function token(again) {
    var t = '';
    try { t = localStorage.getItem('ql_token') || ''; } catch (e) {}
    if (!t || again) {
      t = window.prompt('Password (the control panel password):') || '';
      try { if (t) localStorage.setItem('ql_token', t); } catch (e) {}
    }
    return t;
  }
  function post(path, body, needPassword) {
    var h = {'Content-Type': 'application/json'};
    if (needPassword) { var t = token(); if (!t) return Promise.reject(new Error('A password is needed for this.')); h['X-Fetch-Token'] = t; }
    return fetch(path, {method: 'POST', headers: h, body: JSON.stringify(body)}).then(function (r) {
      return r.json().then(function (j) {
        if (r.status === 403) { try { localStorage.removeItem('ql_token'); } catch (e) {} }
        if (!r.ok) throw new Error(j.error || ('error ' + r.status));
        return j;
      });
    });
  }
  function renderExamples() {
    var box = $('q-examples'); box.textContent = '';
    (examples[scope] || []).forEach(function (x) {
      var b = el('button', x[0], 'ghost'); b.type = 'button';
      b.onclick = function () { $('sql-box').value = x[1]; };
      box.appendChild(b);
    });
  }
  function status() {
    var box = $('scope-status'); box.textContent = '';
    if (scope === 'free') return;
    box.textContent = 'Loading the Firecrawl data status…';
    fetch('/api/crawl/overview').then(function (r) { return r.json(); }).then(function (o) {
      box.textContent = 'Firecrawl data right now: ' + (o.businesses_read || 0).toLocaleString() + ' businesses read (' +
        (o.with_facts || 0).toLocaleString() + ' pages) · last collected ' +
        (o.last_read ? o.last_read.slice(0, 16) + ' UTC' : 'never') + ' · ' +
        (o.sites_checked || 0).toLocaleString() + ' websites checked. Each query reads the latest saved results.';
    }).catch(function () {});
  }
  function setScope(s) {
    if (typeof thread !== 'undefined' && s !== scope && thread.length) { thread = []; save(); redraw(); }
    scope = s;
    try { localStorage.setItem('ql_scope', s); } catch (e) {}
    ['free', 'firecrawl', 'mixed'].forEach(function (x) { $('scope-' + x).className = x === s ? 'on' : ''; });
    $('scope-note').textContent = NOTES[s];
    renderExamples(); status();
  }
  function show(d) {
    last = d; var out = $('q-out'); out.textContent = '';
    if (d.why || d.sql) {
      var a = el('div', null, 'ask-out');
      if (d.why) a.appendChild(el('div', d.why, 'why'));
      if (d.sql) { a.appendChild(el('pre', d.sql)); $('sql-box').value = d.sql; }
      if (d.error) a.appendChild(el('div', d.error, 'err'));
      var tk = d.tokens;
      if (tk) a.appendChild(el('p', d.model + ' · ' + (tk.input || 0) + ' in / ' + (tk.output || 0) + ' out tokens' +
        (tk.cache_read ? ' (+' + tk.cache_read + ' cached)' : '') + (d.attempts > 1 ? ' · fixed on retry' : ''), 'note'));
      out.appendChild(a);
    }
    var rows = d.rows || [];
    $('q-msg').textContent = rows.length + ' row(s)' + (d.truncated ? ' · capped' : '');
    $('q-csv').hidden = !rows.length;
    if (!rows.length) { if (!d.error) out.appendChild(el('div', 'No rows.', 'empty')); return; }
    var wrap = el('div', null, 'scroll'), t = el('table'), hr = el('tr');
    d.columns.forEach(function (c) { hr.appendChild(el('th', c, 'l')); });
    var th = el('thead'); th.appendChild(hr); t.appendChild(th); var tb = el('tbody');
    rows.forEach(function (r) { var tr = el('tr'); r.forEach(function (v) { tr.appendChild(el('td', v == null ? '' : v, 'l')); }); tb.appendChild(tr); });
    t.appendChild(tb); wrap.appendChild(t); out.appendChild(wrap);
  }
  function fail(e) { $('q-msg').textContent = ''; var out = $('q-out'); out.textContent = ''; out.appendChild(el('div', e.message, 'err')); }
  $('q-run').onclick = function () {
    var sql = $('sql-box').value.trim(); if (!sql) return;
    $('q-msg').textContent = 'Running…'; $('q-out').textContent = '';
    post('/api/query', {sql: sql, scope: scope}, scope !== 'free').then(show).catch(fail);
  };
  // ── the chat: a thread of questions and grounded answers ──
  var thread = [];
  try { thread = JSON.parse(sessionStorage.getItem('ql_chat') || '[]'); } catch (e) { thread = []; }
  function save() { try { sessionStorage.setItem('ql_chat', JSON.stringify(thread.slice(-8))); } catch (e) {} }
  function bubbleUser(text) { $('chat').appendChild(el('div', text, 'msg user')); }
  function bubbleBot(d) {
    var b = el('div', null, 'msg bot');
    b.appendChild(el('div', d.answer_text || d.why || d.error || 'No answer.', 'text'));
    if (d.error && d.answer_text == null) { var e = el('div', d.error, 'flag'); b.appendChild(e); }
    if (d.unsupported && d.unsupported.length) {
      b.appendChild(el('div', 'Not found in the data: ' + d.unsupported.join(', ') + ' — treat these with care.', 'flag'));
    }
    if (d.suggestions && d.suggestions.length) {
      var sg = el('div', null, 'sugg');
      d.suggestions.forEach(function (s) {
        var x = el('button', s); x.type = 'button';
        x.onclick = function () { $('ask-q').value = s; ask(); };
        sg.appendChild(x);
      });
      b.appendChild(sg);
    }
    if (d.sql) {
      var det = el('details'), sm = el('summary', 'SQL and data (' + ((d.rows || []).length) + ' rows' + (d.truncated ? ', first 200' : '') + ')');
      det.appendChild(sm); det.appendChild(el('pre', d.sql));
      if (d.rows && d.rows.length) {
        var wrap = el('div', null, 'scroll'), t = el('table'), hr = el('tr');
        d.columns.forEach(function (c) { hr.appendChild(el('th', c, 'l')); });
        var th = el('thead'); th.appendChild(hr); t.appendChild(th); var tb = el('tbody');
        d.rows.slice(0, 50).forEach(function (r) { var tr = el('tr'); r.forEach(function (v) { tr.appendChild(el('td', v == null ? '' : v, 'l')); }); tb.appendChild(tr); });
        t.appendChild(tb); wrap.appendChild(t); det.appendChild(wrap);
      }
      var edit = el('button', 'Open in the SQL box', 'ghost'); edit.type = 'button';
      edit.onclick = function () { $('sql-box').value = d.sql; last = d; show({columns: d.columns, rows: d.rows, truncated: d.truncated}); $('sql-box').scrollIntoView({behavior: 'smooth', block: 'center'}); };
      det.appendChild(edit);
      b.appendChild(det);
    }
    var tk = d.tokens || {};
    if (d.model) b.appendChild(el('div', d.model + ' · ' + ((tk.input || 0) + (tk.cache_read || 0)).toLocaleString() + ' in / ' + (tk.output || 0) + ' out tokens', 'meta'));
    $('chat').appendChild(b);
    return b;
  }
  function redraw() {
    $('chat').textContent = '';
    thread.forEach(function (t) { bubbleUser(t.q); bubbleBot(t.d); });
  }
  function ask() {
    var qtext = $('ask-q').value.trim(); if (!qtext) return;
    $('ask-q').value = '';
    bubbleUser(qtext);
    var wait = el('div', 'Thinking… writing the query, running it on the data, then reading the result.', 'msg bot thinking');
    $('chat').appendChild(wait); wait.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    $('ask-b').disabled = true;
    var history = thread.map(function (t) { return {q: t.q, sql: t.d.sql || '', answer: t.d.answer_text || ''}; });
    post('/api/ask', {question: qtext, scope: scope, history: history}, true).then(function (d) {
      wait.remove();
      thread.push({q: qtext, d: d}); save();
      bubbleBot(d).scrollIntoView({behavior: 'smooth', block: 'nearest'});
    }).catch(function (e) {
      wait.remove();
      bubbleBot({error: e.message, answer_text: e.message});
    }).then(function () { $('ask-b').disabled = false; $('ask-q').focus(); });
  }
  $('chat-new').onclick = function () { thread = []; save(); redraw(); $('ask-q').focus(); };
  redraw();
  $('ask-b').onclick = ask;
  $('ask-q').addEventListener('keydown', function (ev) { if (ev.key === 'Enter') ask(); });
  $('q-csv').onclick = function (ev) {
    ev.preventDefault(); if (!last) return;
    var q = function (v) { v = v == null ? '' : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
    var csv = [last.columns.map(q).join(',')].concat(last.rows.map(function (r) { return r.map(q).join(','); })).join('\n');
    var a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([csv], {type: 'text/csv'}));
    a.download = 'queens_query.csv'; a.click();
  };
  ['free', 'firecrawl', 'mixed'].forEach(function (x) { $('scope-' + x).onclick = function () { setScope(x); }; });
  fetch('/api/query/examples').then(function (r) { return r.json(); }).then(function (d) { examples = d; renderExamples(); }).catch(function () {});
  setScope(scope);
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
    <div class="scroll">{src_table}</div>
    <p class="note" style="margin-top:14px">Pull on a laptop with <code>python fetch.py</code>;
      the results reach this page through the bucket.</p>
  </section>

  <section id="websites">
    <div class="fc-hero">
      <span class="fc-tag">Firecrawl</span>
      <h2>Business websites</h2>
      <p class="note">Every Queens business that lists a website, checked for free: still alive, dead
        (domain gone — often a closure), parked or for sale, taken over by spam, or really a platform
        page. Then <b>Firecrawl</b> reads the alive ones for the facts set in the requirements.
        Research only — kept apart from the public records.</p>
    </div>
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
        <div id="crawl-credits" class="credits"><span class="muted">Loading credits…</span></div>
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
          <label>Mode <select id="cfg-mode"><option value="structured">Structured facts</option>
            <option value="text">Page text only (1 credit/page)</option></select></label>
          <label>Facts read by <select id="cfg-extractor">
            <option value="claude">Claude Haiku (1 credit + ~0.1¢ a page)</option>
            <option value="firecrawl">Firecrawl AI (5 credits a page)</option></select></label>
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
        <p class="note">To search this data, use the <a href="#query">Query</a> section with
          <b>Firecrawl data</b> or <b>Mixed</b> selected.</p>
        <div class="pnl-row"><h3 style="margin:0">Latest results</h3>
          <button type="button" id="crawl-csv" class="ghost">Download all as CSV</button></div>
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
    <button type="button" class="ghost toggle" id="jobs-toggle">Show all</button>
  </section>

  <section id="query">
    <h2>Query</h2>
    <div class="seg" role="tablist">
      <button type="button" id="scope-free" class="on">Free data</button>
      <button type="button" id="scope-firecrawl">Firecrawl data</button>
      <button type="button" id="scope-mixed">Mixed</button>
    </div>
    <p class="note" id="scope-note"></p>
    <p class="note" id="scope-status"></p>
    <div id="chat" class="chat"></div>
    <div class="ask"><input id="ask-q" placeholder="Ask anything about Queens — e.g. Where would a new bakery face the least competition?">
      <button type="button" id="ask-b">Ask</button></div>
    <div class="pnl-row chatbar"><button type="button" id="chat-new" class="ghost">New chat</button>
      <span class="muted">Claude writes the query, runs it on the selected data, then explains the result
        and suggests what to ask next. Numbers not found in the data are flagged. Needs the control
        panel password (about 2–4 cents a question). Or write SQL yourself below.</span></div>
    <div class="pnl-row" id="q-examples"></div>
    <textarea id="sql-box" rows="5"
      placeholder="SELECT name, restaurants_per_10k FROM nta_profile ORDER BY 2 DESC LIMIT 10">{html.escape(sql)}</textarea>
    <div class="pnl-row"><button type="button" id="q-run">Run query</button>
      <a href="#" id="q-csv" hidden>download this result as CSV</a>
      <span class="muted" id="q-msg"></span></div>
    <div id="q-out"></div>
    {result}
  </section>

  <footer>Queens open data · DuckDB · no external database ·
    <a href="https://github.com/Safwan-Ul-Islam/QueensLab" target="_blank" rel="noopener">source</a>
  </footer>
</div>
{NB_JS}
{JOBS_JS}
{CRAWL_JS}
{QUERY_JS}
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


@app.get("/favicon.ico")
def favicon():
    """A small icon, so browsers stop logging a 404 for it."""
    from fastapi.responses import Response
    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" '
           'rx="7" fill="#15171c"/><text x="16" y="22" font-size="17" text-anchor="middle" '
           'font-family="Arial" font-weight="700" fill="#fff">Q</text></svg>')
    return Response(svg, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


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
        cols, rows = ck.results(100_000)
        total = len(rows)
        rows = rows[:5]
        ok = ck.CRAWLABLE if c.get("include_blocked") else ("alive", "alive_js")
        waiting = ck.query(f"""SELECT count(DISTINCT domain) FROM site_checks
                               WHERE status IN {ok}""")[0][0]
        return {"credits": cr, "config": c, "credits_per_page": {**ck.CREDITS, "claude": ck.CLAUDE_CREDITS},
                "field_types": list(ck.FIELD_TYPES), "crawlable_sites": waiting,
                "runs": [{k: v for k, v in r.items() if k != "urls"} for r in ck.runs()][-10:],
                "results": {"columns": cols, "total": total,
                            "rows": [[None if v is None else str(v) for v in r] for r in rows]}}
    return _crawl_call(build)


@app.get("/api/crawl/progress")
def crawl_progress(request: Request):
    """The panel's live status: polled every few seconds while a run is going."""
    denied = _admin_error(request)
    if denied:
        return denied
    return _crawl_call(lambda: _ck().progress())


SCOPES = ("free", "firecrawl", "mixed")


@app.post("/api/query")
async def api_query(request: Request):
    """The one Query box. Free data is open to everyone; Firecrawl data and
    Mixed need the password. Same rules everywhere: one SELECT, no files, caps."""
    body = await request.json()
    scope = body.get("scope") if body.get("scope") in SCOPES else "free"
    if scope != "free":
        denied = _admin_error(request)
        if denied:
            return denied
    sql = str(body.get("sql") or "").strip().rstrip(";")
    why = _one_read(sql) if sql else "Write a query first."
    if why:
        return JSONResponse({"error": why}, status_code=400)
    if scope == "free":
        cols, rows = q(sql, limit=501)
        if busy():
            return JSONResponse({"error": "The database is busy saving a table — try again in a minute."},
                                status_code=409)
        if cols == ["error"]:
            return JSONResponse({"error": str(rows[0][0])}, status_code=400)
    else:
        try:
            cols, rows = _run_analysis(sql, 501, scope)
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"error": jobs.clean(str(e))}, status_code=400)
    return {"columns": cols, "rows": [[None if v is None else str(v) for v in r] for r in rows[:500]],
            "truncated": len(rows) > 500, "scope": scope}


@app.get("/api/query/examples")
def api_query_examples():
    return _ck().EXAMPLES


def _run_analysis(sql: str, limit: int, scope: str = "mixed"):
    """One query on the website-data database, interrupted after QUERY_SECONDS."""
    con = _ck().analysis_db(scope)
    timer = threading.Timer(QUERY_SECONDS, con.interrupt)
    timer.start()
    try:
        cur = con.execute(sql)
        return [d[0] for d in cur.description], cur.fetchmany(limit)
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "INTERRUPT" in msg.upper():
            msg = f"Stopped after {QUERY_SECONDS} seconds. Try a narrower query."
        raise ValueError(msg[:300]) from None
    finally:
        timer.cancel()
        con.close()


@app.post("/api/ask")
async def ask_question(request: Request):
    """Plain English -> SQL (written by Claude) -> run on the real data.
    Costs Anthropic tokens, so it needs the panel password."""
    denied = _admin_error(request)
    if denied:
        return denied
    import ask as A
    body = await request.json()
    scope = body.get("scope") if body.get("scope") in SCOPES else "free"
    question = body.get("question") or ""
    history = body.get("history") if isinstance(body.get("history"), list) else []

    def go():
        if scope == "free":
            c = con()
            if not c:
                raise ValueError("the database is busy or not loaded — try again in a minute")

            def run(sql):
                cols, rows = q(sql, limit=A.ROW_LIMIT + 1)
                if cols == ["error"]:
                    raise ValueError(rows[0][0])
                return cols, rows
            try:
                out = A.chat(question, scope, history, c, _one_read, run)
            finally:
                c.close()
        else:
            dc = _ck().analysis_db(scope)
            try:
                out = A.chat(question, scope, history, dc, _one_read,
                             lambda sql: _run_analysis(sql, A.ROW_LIMIT + 1, scope))
            finally:
                dc.close()
        out["rows"] = [[None if v is None else str(v) for v in r] for r in out["rows"]]
        return out
    return _crawl_call(go)


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
    # Claude reading a run's pages takes minutes: collect in the background,
    # the live status shows it
    return _crawl_call(lambda: {"runs": _ck().refresh(collect_done=False), "collecting": _ck().collect_soon()})


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
            ck = _ck()
            if ck.has_key():
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
