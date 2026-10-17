"""Business websites: a free health check, then Firecrawl for the facts.

Two steps, both usable from the command line (crawl.py) and from the page's
control panel (app.py):

1. CHECK (free, no Firecrawl). Every Queens business in `overture_places`
   that lists a website: does the domain still exist, does it load, is it
   parked or for sale, has it been taken over by spam, or is it really a
   directory/platform page? A business's website dying is a closure signal,
   and Firecrawl charges for dead sites too, so this runs first.

2. CRAWL (Firecrawl batch API). Only sites the check found ALIVE and the
   business's OWN. Firecrawl does the work on its side, so a run continues
   while our free server sleeps; results are kept there ~24 h and collected
   by the page (or `crawl.py collect`).

What to pull is set in the REQUIREMENTS (config): a list of fields — name,
type, description — plus a prompt. Structured mode costs 5 credits a page
(Firecrawl's AI fills the fields); text mode 1 credit (the page as text).

⚠️ RESEARCH ONLY. Everything lands in scraped/ (local, gitignored) and the
bucket's scraped/ prefix, never in raw/ with the public records, never in
BizGenome, never in training (BizGenome log, 2026-10-10). Only businesses'
own sites — never Google Maps, Yelp, social media or listing sites.
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import os
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gcs  # noqa: E402  (also loads .env)
import jobs  # noqa: E402

DB = HERE / "queens.duckdb"
OUT = HERE / "scraped"
API = "https://api.firecrawl.dev/v2"
PREFIX = os.environ.get("GCS_SCRAPED_PREFIX") or "scraped"
UA = "Mozilla/5.0 (compatible; QueensLab-research/1.0; site health check)"
CREDITS = {"structured": 5, "text": 1}       # per page, from Firecrawl's billing docs
REMOTE_STEP = jobs.REMOTE_STEP                # a job running on Firecrawl's side
_lock = threading.Lock()
try:                                   # Mozilla's CA list: this machine's Python lacked one,
    import certifi                     # and real sites (hrblock.com) looked "unreachable"
    _TLS = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _TLS = ssl.create_default_context()
# What Firecrawl is worth spending on. "blocked" (a bot wall) is opt-in: in the
# first run 2 of 7 such sites came back 403 from Firecrawl too — 5 credits each.
CRAWLABLE = ("alive", "alive_js", "blocked")

# Not a business's own site: platforms, directories, social. Never crawled.
PLATFORMS = ("facebook.", "instagram.", "yelp.", "google.", "goo.gl", "tiktok.", "twitter.",
             "x.com", "linktr.ee", "linkin.bio", "wikipedia.", "hub.biz", "minitable.", "nearme",
             "oncarrot.", "grubhub.", "doordash.", "ubereats.", "seamless.", "opentable.",
             "resy.", "toasttab.", "menufy.", "slicelife.", "beyondmenu.", "chownow.",
             "allmenus.", "menupages.", "zocdoc.", "healthgrades.", "vagaro.", "booksy.",
             "styleseat.", "groupon.", "mapquest.", "yellowpages.", "bbb.org", "nextdoor.",
             "linkedin.", "youtube.", "wa.me", "whatsapp.", "apple.com", "amazon.")
PARKED = re.compile(r"domain (is )?(for sale|may be for sale|parked)|buy this domain|this domain"
                    r"( name)? (is|has) (for sale|expired)|hugedomains|sedo\.com|dan\.com|afternic|"
                    r"godaddy\.com/domains|parkingcrew|bodis\.com|domain has expired|"
                    r"website (is )?(coming soon|under construction)", re.I)
SPAM = re.compile(r"\b(slot ?gacor|slot88|togel|judi|casino online|situs|bandar|sbobet|"
                  r"maxwin|betting|viagra|cialis|porn|escort)\b", re.I)

DEFAULT_CONFIG = {
    "mode": "structured",
    "prompt": ("Extract facts about this local business in Queens, New York, using only what "
               "the page itself states. Leave a field empty rather than guessing."),
    "fields": [
        {"name": "business_type", "type": "text",
         "description": "What kind of business this is, in a few words (e.g. 'Mexican bakery')."},
        {"name": "opening_hours", "type": "text",
         "description": "Regular opening hours as stated, e.g. 'Mon-Fri 9am-7pm; Sat 10am-5pm'. Empty if not stated."},
        {"name": "price_level", "type": "text",
         "description": "One of $, $$, $$$, $$$$ judged from prices shown on the site. Empty if no prices are shown."},
        {"name": "prices", "type": "prices",
         "description": "Up to 10 items or services with their price exactly as shown: item, price "
                        "as a number, currency (e.g. USD), unit if any (e.g. 'per hour')."},
        {"name": "services_or_products", "type": "list",
         "description": "The main services or products offered, up to 10."},
        {"name": "online_ordering_or_booking", "type": "yes/no",
         "description": "Can customers order, book or reserve online from this site?"},
        {"name": "delivery", "type": "yes/no", "description": "Does the business say it delivers?"},
        {"name": "languages", "type": "list",
         "description": "Languages the business says it serves customers in."},
        {"name": "closed_or_moved_notice", "type": "text",
         "description": "ONLY a notice that the business has permanently closed, moved, or changed "
                        "owner. Empty otherwise. Normal closing hours and marketing phrases are NOT notices."},
        {"name": "shows_queens_address", "type": "yes/no",
         "description": "Does the page show a street address in Queens, New York (or a Queens neighbourhood)?"},
        {"name": "year_established", "type": "number", "description": "Year founded, if stated."},
        # added 2026-10-18 (user: the 7 suggestions + 5 more)
        {"name": "street_address", "type": "text",
         "description": "The street address of THIS location as written on the page."},
        {"name": "branch_name", "type": "text",
         "description": "For a business with several locations, the name of this branch (e.g. 'Flushing')."},
        {"name": "locations_count", "type": "number",
         "description": "How many locations the business says it has, if stated."},
        {"name": "is_chain_or_franchise", "type": "yes/no",
         "description": "Is this part of a chain or franchise (several locations or a franchise brand)?"},
        {"name": "opening_soon_notice", "type": "text",
         "description": "ONLY a 'grand opening', 'opening soon' or 'now open' announcement. Empty otherwise."},
        {"name": "now_hiring", "type": "yes/no", "description": "Does the site say it is hiring?"},
        {"name": "service_modes", "type": "list",
         "description": "ONLY the ways of being served that the page itself mentions, using these words: "
                        "dine-in, takeout, catering, delivery, appointments, walk-ins, online only. "
                        "Empty if the page mentions none. Never list them all by default."},
        {"name": "delivery_apps", "type": "list",
         "description": "Delivery or ordering apps it links to or names (e.g. DoorDash, Uber Eats, Grubhub)."},
        {"name": "payment_options", "type": "list",
         "description": "Only payment methods the page names, e.g. cash, cards, SNAP/EBT, Apple Pay. "
                        "Empty if none are named."},
        {"name": "open_late", "type": "yes/no",
         "description": "Open after 10pm on any day, or 24 hours, according to the stated hours?"},
        {"name": "wheelchair_accessible", "type": "yes/no", "description": "Only if the site states it."},
        {"name": "parking", "type": "text",
         "description": "Only what the page says about PARKING for cars (own lot, street, validated). "
                        "Empty if parking is not mentioned. Pickup or delivery is not parking."},
        {"name": "seating", "type": "text", "description": "Seating capacity or description, if stated."},
        {"name": "outdoor_space", "type": "yes/no",
         "description": "Outdoor seating, patio, garden or yard, if stated."},
        {"name": "relevant_page_links", "type": "list",
         "description": "Up to 5 links on this page to its menu, services, prices, hours or locations pages."},
    ],
    "evidence": True,
    "follow_links": 1,
    "filters": {"categories": [], "ntas": [], "min_confidence": 0.7},
    "recrawl_days": 60,
    "default_batch": 200,
    "include_blocked": False,
}
FIELD_TYPES = {"text": {"type": "string"}, "number": {"type": "number"},
               "yes/no": {"type": "boolean"}, "list": {"type": "array", "items": {"type": "string"}},
               # item, amount, currency and unit as separate values — comparable across shops
               "prices": {"type": "array", "items": {"type": "object", "properties": {
                   "item": {"type": "string"}, "price": {"type": "number"},
                   "currency": {"type": "string"}, "unit": {"type": "string"}}}}}
EVIDENCE = {"type": "array", "description":
            "For each field you filled, the field name and the SHORT exact quote from the page "
            "that supports it.", "items": {"type": "object", "properties": {
                "field": {"type": "string"}, "quote": {"type": "string"}}}}
LINKS_FIELD = "relevant_page_links"     # read by the follow-up step
LINK_WORDS = re.compile(r"menu|service|price|pricing|rates|hours|about|location|contact|order|catering", re.I)


# ── small documents (config, run state), kept in the bucket and locally ───
def _q(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def doc_get(name: str, default):
    """The newest copy: the bucket if configured (shared by laptop and server),
    else the local file."""
    if gcs.enabled():
        try:
            rows = gcs.read_rows(f"{PREFIX}/{name}.parquet", timeout_s=10)
            if rows:
                return json.loads(rows[0]["doc"])
        except Exception as e:  # noqa: BLE001
            if not gcs.not_found(e):
                raise
    f = OUT / f"{name}.json"
    return json.loads(f.read_text()) if f.exists() else default


def doc_put(name: str, obj) -> None:
    OUT.mkdir(exist_ok=True)
    text = json.dumps(obj, default=str)
    (OUT / f"{name}.json").write_text(text)
    if gcs.enabled():
        gcs.write_rows(f"{PREFIX}/{name}.parquet", [("doc", "VARCHAR")], [[text]])


def config() -> dict:
    c = doc_get("crawl_config", None) or {}
    out = json.loads(json.dumps(DEFAULT_CONFIG))
    out.update({k: v for k, v in c.items() if k in DEFAULT_CONFIG})
    return out


def validate_config(c: dict) -> dict:
    """Clean a config coming from the page. Raises ValueError with a reason."""
    out = config()
    mode = c.get("mode", out["mode"])
    if mode not in CREDITS:
        raise ValueError("mode must be 'structured' or 'text'")
    fields = c.get("fields", out["fields"])
    if not isinstance(fields, list) or not 1 <= len(fields) <= 40:
        raise ValueError("between 1 and 40 fields")
    clean = []
    for f in fields:
        name = re.sub(r"[^a-z0-9_]", "_", str(f.get("name", "")).strip().lower())[:40].strip("_")
        if not name:
            raise ValueError("every field needs a name")
        if f.get("type") not in FIELD_TYPES:
            raise ValueError(f"field {name}: type must be one of {', '.join(FIELD_TYPES)}")
        clean.append({"name": name, "type": f["type"], "description": str(f.get("description", ""))[:400]})
    if len({f["name"] for f in clean}) != len(clean):
        raise ValueError("field names must be unique")
    filt = c.get("filters", out["filters"]) or {}
    out.update({
        "mode": mode, "fields": clean,
        "prompt": str(c.get("prompt", out["prompt"]))[:1000],
        "filters": {"categories": [str(x) for x in filt.get("categories", [])][:50],
                    "ntas": [str(x) for x in filt.get("ntas", [])][:100],
                    "min_confidence": max(0.0, min(1.0, float(filt.get("min_confidence", 0.7))))},
        "recrawl_days": max(1, min(365, int(c.get("recrawl_days", out["recrawl_days"])))),
        "default_batch": max(1, min(1000, int(c.get("default_batch", out["default_batch"])))),
        "include_blocked": bool(c.get("include_blocked", out["include_blocked"])),
        "evidence": bool(c.get("evidence", out["evidence"])),
        "follow_links": max(0, min(2, int(c.get("follow_links", out["follow_links"])))),
    })
    return out


def save_config(c: dict) -> dict:
    c = validate_config(c)
    doc_put("crawl_config", c)
    return c


def schema(c: dict) -> dict:
    props = {f["name"]: {**FIELD_TYPES[f["type"]], "description": f["description"]} for f in c["fields"]}
    if c.get("evidence"):
        props["evidence"] = EVIDENCE
    return {"type": "object", "properties": props}


def per_site(c: dict) -> int:
    """Credits per business, counting the follow-up pages it MAY use."""
    return CREDITS[c["mode"]] * (1 + (c.get("follow_links", 0) if c["mode"] == "structured" else 0))


# ── tables in scraped/: kept locally, mirrored to the bucket ──────────────
SITE_COLS = [("place_id", "VARCHAR"), ("name", "VARCHAR"), ("category", "VARCHAR"),
             ("nta2020", "VARCHAR"), ("ntaname", "VARCHAR"), ("website", "VARCHAR"),
             ("domain", "VARCHAR"), ("status", "VARCHAR"), ("reason", "VARCHAR"),
             ("http_status", "INTEGER"), ("final_url", "VARCHAR"), ("title", "VARCHAR"),
             ("checked_at", "TIMESTAMP")]
FACT_COLS = [("run_id", "VARCHAR"), ("place_id", "VARCHAR"), ("name", "VARCHAR"),
             ("category", "VARCHAR"), ("nta2020", "VARCHAR"), ("url", "VARCHAR"),
             ("final_url", "VARCHAR"), ("http_status", "INTEGER"), ("title", "VARCHAR"),
             ("mode", "VARCHAR"), ("facts", "VARCHAR"), ("markdown", "VARCHAR"),
             ("error", "VARCHAR"), ("fetched_at", "TIMESTAMP"),
             ("base_run", "VARCHAR"), ("page_kind", "VARCHAR")]


REFRESH_S = 300   # the other machine may have written newer results to the bucket


def local_table(name: str) -> Path | None:
    """scraped/<name>.parquet, refreshed from the bucket when the local copy is
    missing (a fresh server disk) or older than REFRESH_S. Every local write
    is uploaded at once, so the bucket is never behind this machine."""
    f = OUT / f"{name}.parquet"
    fresh = f.exists() and time.time() - f.stat().st_mtime < REFRESH_S
    if fresh or not gcs.enabled():
        return f if f.exists() else None
    OUT.mkdir(exist_ok=True)      # a fresh server disk has no scraped/ yet
    tmp = OUT / f"{name}.dl.parquet"
    con = gcs._scratch(timeout_s=20)
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet({_q(gcs.url(f'{name}.parquet', PREFIX))})) "
                    f"TO {_q(tmp)} (FORMAT PARQUET)")
        tmp.replace(f)
    except Exception as e:  # noqa: BLE001 — keep a local copy if there is one
        if not (gcs.not_found(e) or f.exists()):
            raise
    finally:
        con.close()
        tmp.unlink(missing_ok=True)
    return f if f.exists() else None


def upsert(name: str, cols: list[tuple], rows: list[list], key: list[str]) -> int:
    """Merge rows into scraped/<name>.parquet (newest wins per key), upload it."""
    import duckdb
    OUT.mkdir(exist_ok=True)
    have = local_table(name)
    c = duckdb.connect()
    try:
        c.execute("CREATE TABLE t (" + ", ".join(f'"{n}" {t}' for n, t in cols) + ")")
        if have:
            c.execute(f"INSERT INTO t BY NAME SELECT * FROM read_parquet({_q(have)}, union_by_name=true)")
        names = [n for n, _ in cols]
        if rows:
            c.execute("CREATE TABLE n AS SELECT * FROM t LIMIT 0")
            c.executemany(f"INSERT INTO n VALUES ({', '.join('?' * len(cols))})", rows)
            k = " AND ".join(f't."{x}" IS NOT DISTINCT FROM n."{x}"' for x in key)
            c.execute(f"DELETE FROM t WHERE EXISTS (SELECT 1 FROM n WHERE {k})")
            c.execute("INSERT INTO t SELECT * FROM n")
        f = OUT / f"{name}.parquet"
        tmp = OUT / f"{name}.tmp.parquet"
        c.execute(f"COPY (SELECT {', '.join(chr(34) + n + chr(34) for n in names)} FROM t) "
                  f"TO {_q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        tmp.replace(f)
        total = c.execute("SELECT count(*) FROM t").fetchone()[0]
    finally:
        c.close()
    if gcs.enabled():
        gcs.upload(f, f"{name}.parquet", prefix=PREFIX)
    return total


def query(sql: str, params=None) -> list[tuple]:
    """Read the scraped tables (and queens.duckdb as q) without touching the
    public SQL box's connection."""
    import duckdb
    c = duckdb.connect()
    c.execute("SET enable_progress_bar=false")
    try:
        if DB.exists():
            c.execute(f"ATTACH {_q(DB)} AS q (READ_ONLY)")
            if gcs.enabled():
                gcs.attach(c)
        for name in ("site_checks", "web_facts"):
            f = local_table(name)
            if f:
                c.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({_q(f)})")
            else:
                cols = SITE_COLS if name == "site_checks" else FACT_COLS
                c.execute(f"CREATE TABLE {name} (" + ", ".join(f'"{n}" {t}' for n, t in cols) + ")")
        return c.execute(sql, params or []).fetchall()
    finally:
        c.close()


# ── step 1: the free site check ───────────────────────────────────────────
def _domain(url: str) -> str:
    m = re.match(r"^https?://(?:www\.)?([^/:?#]+)", (url or "").strip().lower())
    return m.group(1) if m else ""


def _platform(domain: str) -> bool:
    return any(p in domain for p in PLATFORMS)


def _probe(url: str, domain: str) -> dict:
    """One business website: DNS, then one GET. Never more than one request."""
    if domain.endswith(".business.site"):
        return {"status": "dead", "reason": "Google shut down its free business.site pages (2024)"}
    try:
        socket.getaddrinfo(domain, 443)
    except socket.gaierror:
        return {"status": "dead", "reason": "the domain no longer exists (DNS)"}
    except Exception as e:  # noqa: BLE001
        return {"status": "unreachable", "reason": f"DNS: {type(e).__name__}"}
    for attempt_url in (url, url.replace("https://", "http://", 1)):
        try:
            req = urllib.request.Request(attempt_url, headers={"User-Agent": UA, "Accept": "text/html"})
            with urllib.request.urlopen(req, timeout=12, context=_TLS) as r:
                body = r.read(200_000).decode("utf-8", "replace")
                code, final = r.status, r.geturl()
            break
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 429, 503):   # a bot wall: the site is up, a browser gets in
                return {"status": "blocked", "reason": f"HTTP {e.code} — blocks simple requests",
                        "http_status": e.code}
            return {"status": "error", "reason": f"HTTP {e.code}", "http_status": e.code}
        except (ssl.SSLError, urllib.error.URLError) as e:
            if attempt_url.startswith("https://") and "ssl" in str(e).lower():
                continue                      # an expired certificate: try plain http once
            return {"status": "unreachable", "reason": f"{type(e).__name__}: {str(e)[:80]}"}
        except Exception as e:  # noqa: BLE001
            return {"status": "unreachable", "reason": f"{type(e).__name__}: {str(e)[:80]}"}
    else:
        return {"status": "unreachable", "reason": "SSL failure"}
    m = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
    title = re.sub(r"\s+", " ", m.group(1)).strip()[:200] if m else ""
    fd = _domain(final)
    out = {"http_status": code, "final_url": final[:500], "title": title}
    text = title + " " + body[:20_000]
    if PARKED.search(text):
        return {**out, "status": "parked", "reason": "parked, for sale, expired or 'coming soon'"}
    if SPAM.search(title) or len(SPAM.findall(body[:20_000])) >= 3:
        return {**out, "status": "hijacked", "reason": "now shows gambling/spam content"}
    if fd and _platform(fd):
        return {**out, "status": "platform", "reason": f"redirects to {fd}"}
    if len(re.sub(r"<[^>]+>", " ", body).split()) < 15:
        return {**out, "status": "alive_js",
                "reason": "almost no text without a browser (a JavaScript site) — Firecrawl renders it"}
    if fd and fd != domain and not (fd.endswith("." + domain) or domain.endswith("." + fd)):
        return {**out, "status": "alive", "reason": f"redirects to {fd}"}
    return {**out, "status": "alive", "reason": ""}


def check_sites(limit: int | None = None, recheck_days: int = 30, workers: int = 48,
                job=None, stop: threading.Event | None = None) -> dict:
    """Check every open Queens business website not checked in `recheck_days`."""
    picks = query(f"""
        SELECT id::VARCHAR, name, category, nta2020, ntaname, website FROM q.overture_places
        WHERE operating_status = 'open' AND website LIKE 'http%'
          AND id::VARCHAR NOT IN (SELECT place_id FROM site_checks
                         WHERE checked_at > now() - INTERVAL {int(recheck_days)} DAY)
        ORDER BY hash(id)""")
    by_domain: dict[str, list] = {}
    for row in picks:
        d = _domain(row[5])
        if d:
            by_domain.setdefault(d, []).append(row)
    domains = list(by_domain)[: limit or None]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows, counts = [], {}

    def one(d):
        first = by_domain[d][0]
        return d, ({"status": "platform", "reason": "a platform or directory, not the business's own site"}
                   if _platform(d) else _probe(first[5], d))

    pool = cf.ThreadPoolExecutor(max_workers=workers)
    try:
        futures = [pool.submit(one, d) for d in domains]
        for i, fut in enumerate(cf.as_completed(futures), 1):
            if stop is not None and stop.is_set():
                raise RuntimeError("stopped by you")
            d, r = fut.result()
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            for pid, name, cat, nta, ntaname, web in by_domain[d]:
                rows.append([pid, name, cat, nta, ntaname, web, d, r["status"], r.get("reason"),
                             r.get("http_status"), r.get("final_url"), r.get("title"), now])
            if job and i % 200 == 0:
                job.add_rows(200)
            if len(rows) >= 5000:              # save as we go: a stop loses little
                upsert("site_checks", SITE_COLS, rows, ["place_id"])
                rows = []
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        if rows:
            upsert("site_checks", SITE_COLS, rows, ["place_id"])
    return {"domains": len(domains), "counts": counts}


# ── step 2: Firecrawl ─────────────────────────────────────────────────────
class FirecrawlError(Exception):
    pass


def _key() -> str:
    k = os.environ.get("FIRECRAWL_API_KEY")
    if not k:
        raise FirecrawlError("FIRECRAWL_API_KEY is not set (QueensLab/.env, or Render's Environment tab)")
    return k


def _api(method: str, path: str, body=None, timeout: int = 60):
    url = path if path.startswith("http") else API + path
    req = urllib.request.Request(url, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {_key()}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:300]
        if e.code == 402:
            raise FirecrawlError("out of Firecrawl credits for this billing period") from e
        if e.code == 401:
            raise FirecrawlError("Firecrawl rejected the API key") from e
        raise FirecrawlError(f"Firecrawl HTTP {e.code}: {detail}") from e


def credits() -> dict:
    d = _api("GET", "/team/credit-usage").get("data", {})
    return {"remaining": d.get("remainingCredits"), "plan": d.get("planCredits"),
            "period_end": d.get("billingPeriodEnd")}


def candidates(limit: int, c: dict) -> list[tuple]:
    """Alive, own-site businesses not crawled within recrawl_days; one per domain."""
    f = c["filters"]
    ok = CRAWLABLE if c.get("include_blocked") else tuple(x for x in CRAWLABLE if x != "blocked")
    where, params = [f"s.status IN {ok}", "p.confidence >= ?"], [f["min_confidence"]]
    if f["categories"]:
        where.append("p.category IN (SELECT unnest(?))"); params.append(f["categories"])
    if f["ntas"]:
        where.append("p.nta2020 IN (SELECT unnest(?))"); params.append(f["ntas"])
    return query(f"""
        SELECT s.place_id, s.name, s.category, s.nta2020, s.website FROM site_checks s
        JOIN q.overture_places p ON p.id::VARCHAR = s.place_id
        WHERE {' AND '.join(where)}
          AND s.website NOT IN (SELECT url FROM web_facts
                                WHERE fetched_at > now() - INTERVAL {int(c['recrawl_days'])} DAY)
        QUALIFY row_number() OVER (PARTITION BY s.domain ORDER BY p.confidence DESC, s.place_id) = 1
        ORDER BY hash(s.place_id) LIMIT {int(limit)}""", params)


def _submit(urls: list[str], c: dict) -> dict:
    formats = (["markdown"] if c["mode"] == "text" else
               [{"type": "json", "schema": schema(c), "prompt": c["prompt"]}])
    r = _api("POST", "/batch/scrape", {"urls": urls, "formats": formats, "onlyMainContent": True,
                                       "ignoreInvalidURLs": True, "timeout": 30000})
    if not r.get("id"):
        raise FirecrawlError(f"Firecrawl did not start the batch: {str(r)[:200]}")
    return r


def _follow(run: dict) -> dict | None:
    """After a homepage run: read up to follow_links linked pages per business
    (menu, services, prices...), same site only. A second batch at Firecrawl."""
    from urllib.parse import urljoin
    c = config()
    k = c.get("follow_links", 0)
    if run.get("kind") != "home" or c["mode"] != "structured" or not k:
        return None
    rows = query("""SELECT place_id, name, category, nta2020, coalesce(final_url, url), facts
                    FROM web_facts WHERE run_id = ? AND facts IS NOT NULL""", [run["run_id"]])
    picks, seen = [], set()
    for pid, name, cat, nta, base, facts in rows:
        try:
            links = json.loads(facts).get(LINKS_FIELD) or []
        except (ValueError, AttributeError):
            links = []
        dom = _domain(base)
        good = []
        for link in links if isinstance(links, list) else []:
            u = urljoin(base, str(link).strip())
            if (u.startswith("http") and _domain(u) == dom and u.rstrip("/") != base.rstrip("/")
                    and u not in seen and not re.search(r"\.(pdf|jpg|png|zip)$", u, re.I)):
                good.append(u)
        good.sort(key=lambda u: not LINK_WORDS.search(u))      # menu/services/prices first
        for u in good[:k]:
            seen.add(u)
            picks.append([pid, name, cat, nta, u])
    if not picks:
        return None
    need = len(picks) * CREDITS["structured"]
    have = credits().get("remaining")
    if have is not None and need > have:
        jobs.Job(run["job_id"]).warn(f"linked pages skipped: {need:,} credits needed, {have:,} left")
        return None
    r = _submit([p[4] for p in picks], c)
    job = jobs.Job(jobs.queue("web_facts", jobs.new_run(), "follow-up")).start(step=REMOTE_STEP)
    job.warn(f"{len(picks)} linked pages of run {run['run_id']} · up to {need:,} credits")
    return {"run_id": uuid.uuid4().hex[:8], "batch_id": r["id"], "job_id": job.id, "mode": "structured",
            "n": len(picks), "credits_est": need,
            "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status": "scraping", "completed": 0, "credits_used": 0, "expires_at": None,
            "collected_at": None, "invalid": r.get("invalidURLs") or [],
            "kind": "follow", "parent": run["run_id"], "urls": picks}


def runs() -> list[dict]:
    return doc_get("crawl_runs", [])


def _save_runs(rs: list[dict]) -> None:
    # keep 30; drop the URL lists of runs already collected
    rs = rs[-30:]
    for r in rs[:-5]:
        if r.get("collected_at"):
            r.pop("urls", None)
    doc_put("crawl_runs", rs)


def start(limit: int | None = None, origin: str = "cli") -> dict:
    with _lock:
        c = config()
        active = [r for r in runs() if not r.get("collected_at")]
        if active:
            raise FirecrawlError(f"run {active[-1]['run_id']} is still at Firecrawl — stop or collect it first")
        picks = candidates(limit or c["default_batch"], c)
        if not picks:
            raise FirecrawlError("no alive, own-site businesses left to read (run the site check first?)")
        need = len(picks) * per_site(c)
        have = credits().get("remaining")
        if have is not None and need > have:
            raise FirecrawlError(f"this run needs {need:,} credits; {have:,} remain")
        r = _submit([p[4] for p in picks], c)
        job = jobs.Job(jobs.queue("web_facts", jobs.new_run(), origin)).start(step=REMOTE_STEP)
        job.warn(f"{len(picks)} sites · {c['mode']} · up to {need:,} credits"
                 + (f" (incl. up to {c['follow_links']} linked page per site)" if c["follow_links"]
                    and c["mode"] == "structured" else ""))
        run = {"run_id": uuid.uuid4().hex[:8], "batch_id": r["id"], "job_id": job.id,
               "mode": c["mode"], "n": len(picks), "credits_est": need,
               "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "status": "scraping", "completed": 0, "credits_used": 0, "expires_at": None,
               "collected_at": None, "invalid": r.get("invalidURLs") or [],
               "kind": "home", "parent": None,
               "urls": [list(p) for p in picks]}
        rs = runs(); rs.append(run); _save_runs(rs)
        return {k: v for k, v in run.items() if k != "urls"}


def refresh(collect_done: bool = True) -> list[dict]:
    """Update every uncollected run from Firecrawl; collect the finished ones."""
    out = []
    with _lock:
        rs = runs()
        changed = False
        for run in rs:
            if run.get("collected_at"):
                continue
            s = _api("GET", f"/batch/scrape/{run['batch_id']}")
            run.update(status=s.get("status"), completed=s.get("completed", 0),
                       credits_used=s.get("creditsUsed", 0), expires_at=s.get("expiresAt"))
            changed = True
            job = jobs.Job(run["job_id"])
            jobs._update(run["job_id"], rows_downloaded=run["completed"] or 0,
                         last_activity=jobs.now(), progress_at=jobs.now())
            if collect_done and run["status"] in ("completed", "failed", "cancelled"):
                n = _collect(run, s)
                run["collected_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                job.upload("uploaded" if gcs.enabled() else "off",
                           gcs.url("web_facts.parquet", PREFIX) if gcs.enabled() else "no bucket")
                if n < run["n"]:
                    job.warn(f"{run['n'] - n} of {run['n']} sites came back empty from Firecrawl "
                             "(not charged)")
                if run["status"] == "completed":
                    job.succeed(n)
                else:
                    job.fail(f"Firecrawl reported '{run['status']}' after {run['completed']} of {run['n']}"
                             f" — {n} results kept")
            out.append({k: v for k, v in run.items() if k != "urls"})
        followups = []
        for run in rs:
            if run.get("collected_at") and run.get("kind") == "home" and not run.get("followed"):
                run["followed"] = True
                changed = True
                try:
                    f = _follow(run)
                except FirecrawlError as e:
                    jobs.Job(run["job_id"]).warn(f"linked pages not read: {e}")
                    f = None
                if f:
                    followups.append(f)
        rs.extend(followups)
        if changed:
            _save_runs(rs)
    return out


def _collect(run: dict, first: dict) -> int:
    """Every result page of a batch -> web_facts (and its URL -> business)."""
    by_url = {u[4]: u for u in run.get("urls", [])}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows, page = [], first
    while True:
        for d in page.get("data") or []:
            meta = d.get("metadata") or {}
            src = meta.get("sourceURL") or meta.get("url") or ""
            pid, name, cat, nta, url = by_url.get(src, (None, None, None, None, src))
            facts = d.get("json")
            md = d.get("markdown")
            rows.append([run["run_id"], pid, name, cat, nta, url, meta.get("url"), meta.get("statusCode"),
                         (meta.get("title") or "")[:300], run["mode"],
                         json.dumps(facts) if facts is not None else None,
                         md[:20_000] if md else None, meta.get("error"), now,
                         run.get("parent") or run["run_id"], run.get("kind") or "home"])
        nxt = page.get("next")
        if not nxt:
            break
        page = _api("GET", nxt)
    if rows:
        upsert("web_facts", FACT_COLS, rows, ["run_id", "url"])
    return len(rows)


def stop_run() -> dict:
    """Cancel the run at Firecrawl, then keep whatever it finished."""
    rs = runs()
    active = [r for r in rs if not r.get("collected_at")]
    if not active:
        raise FirecrawlError("nothing is running")
    run = active[-1]
    try:
        _api("DELETE", f"/batch/scrape/{run['batch_id']}")
    except FirecrawlError as e:
        if "404" not in str(e):
            raise
    jobs.Job(run["job_id"]).warn("stopped by you")
    time.sleep(2)
    done = refresh()
    return next((r for r in done if r["run_id"] == run["run_id"]), {"run_id": run["run_id"]})


def _empty(v) -> bool:
    return v is None or v == "" or v == [] or v == {}


def _visits() -> dict:
    """{place_id: [visit, ...]} oldest first; a visit = one base run's pages
    (homepage first, then linked pages) merged: a field empty on the homepage
    is filled from a linked page; evidence quotes are pooled."""
    rows = query("""SELECT place_id, name, category, nta2020, url, page_kind, facts, fetched_at,
                           coalesce(base_run, run_id) AS visit
                    FROM web_facts WHERE facts IS NOT NULL AND place_id IS NOT NULL
                    ORDER BY place_id, fetched_at, page_kind = 'follow'""")
    out: dict = {}
    for pid, name, cat, nta, url, kind, facts, at, visit in rows:
        try:
            f = json.loads(facts) or {}
        except ValueError:
            continue
        vs = out.setdefault(pid, [])
        if not vs or vs[-1]["visit"] != visit:
            vs.append({"visit": visit, "name": name, "category": cat, "nta2020": nta, "url": url,
                       "pages": [], "facts": {}, "evidence": [], "at": at})
        v = vs[-1]
        v["pages"].append(url)
        v["at"] = max(v["at"], at)
        for k, val in f.items():
            if k == "evidence":
                v["evidence"] += [e for e in (val or []) if isinstance(e, dict)]
            elif _empty(v["facts"].get(k)) and not _empty(val):
                v["facts"][k] = val
    return out


def _show(v) -> str | None:
    if _empty(v):
        return None
    if isinstance(v, list):
        return "; ".join(
            " ".join(str(x.get(k)) for k in ("item", "price", "currency", "unit") if x.get(k) not in (None, ""))
            if isinstance(x, dict) else str(x) for x in v)
    return str(v)


def results(limit: int = 100) -> tuple[list[str], list[tuple]]:
    """One row per business: its latest visit (homepage + linked pages), one
    column per field, the evidence quotes, and which fields changed since the
    previous visit (empty until a business has been read twice)."""
    c = config()
    names = [f["name"] for f in c["fields"] if f["name"] != LINKS_FIELD]
    out = []
    for pid, vs in _visits().items():
        last, prev = vs[-1], (vs[-2] if len(vs) > 1 else None)
        changed = [n for n in names if prev is not None
                   and _show(last["facts"].get(n)) != _show(prev["facts"].get(n))]
        evidence = "; ".join(f"{e.get('field')}: \"{str(e.get('quote'))[:120]}\"" for e in last["evidence"][:12])
        out.append((last["name"], last["nta2020"], last["category"], " | ".join(last["pages"]),
                    *[_show(last["facts"].get(n)) for n in names], evidence or None,
                    ", ".join(changed) if changed else None, last["at"]))
    out.sort(key=lambda r: r[-1], reverse=True)
    return (["name", "nta2020", "category", "pages_read", *names, "evidence", "changed_since_last_read",
             "fetched_at"], out[:int(limit)])


def overview() -> dict:
    """Public-safe summary: no URLs, no extracted text."""
    checks = dict(query("SELECT status, count(*) FROM site_checks GROUP BY 1"))
    facts = query("SELECT count(*), count(facts), max(fetched_at) FROM web_facts")[0]
    last_check = query("SELECT max(checked_at) FROM site_checks")[0][0]
    active = [{k: r.get(k) for k in ("run_id", "status", "n", "completed", "credits_used",
                                      "expires_at", "started", "mode")}
              for r in runs() if not r.get("collected_at")]
    return {"site_checks": checks, "sites_checked": sum(checks.values()),
            "last_check": str(last_check) if last_check else None,
            "pages_read": facts[0], "with_facts": facts[1],
            "last_read": str(facts[2]) if facts[2] else None, "active_run": active[-1] if active else None}


CAN_PULL = [
    ("Facts you define", "Any field you can describe in a sentence — hours, prices, menu items, services, "
     "brands carried, languages, delivery, booking, payment methods, parking, wheelchair access, "
     "year founded, 'closed/moved' notices. Firecrawl's AI reads the page and fills them in. 5 credits a page."),
    ("The page as text", "The readable content of the page (markdown). 1 credit a page; facts are then "
     "found by searching the text."),
    ("Page details", "Title, description, language, the final address after redirects, and the HTTP status."),
    ("Free, without Firecrawl", "Whether each website still exists, loads, is parked or for sale, has been "
     "taken over by spam, or is really a platform page — for every Queens business that lists a site."),
    ("Not collected, on purpose", "Reviews, personal details of owners or staff, and anything from Google "
     "Maps, Yelp, social media or listing sites."),
]
