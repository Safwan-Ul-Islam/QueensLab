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
type, description — plus a prompt. Structured mode: Firecrawl reads the page
as text (1 credit) and Claude Haiku fills the fields (extract.py, about a
tenth of a cent a page); Firecrawl's own AI extraction (5 credits a page)
stays as an option, config "extractor". Text mode: the page text only.

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
CLAUDE_CREDITS = 1                            # structured with Claude: Firecrawl only reads the text
EXTRACTORS = ("claude", "firecrawl")
EXTRACT_WORKERS, EXTRACT_CHUNK = 6, 50        # pages Claude reads at once; pages saved per write
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
    "extractor": "claude",       # since 2026-10-18; "firecrawl" = Firecrawl's AI, 5 credits a page
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


_docs: dict = {}          # name -> (read at, value)
DOC_TTL = 30              # seconds a small document is trusted from memory


def doc_get(name: str, default):
    """The newest copy: the bucket if configured (shared by laptop and server),
    else the local file. Cached for DOC_TTL seconds: the panel polls every 6 s,
    and each bucket read costs ~0.5-3 s — one status refresh used to take ~18 s."""
    hit = _docs.get(name)
    if hit and time.time() - hit[0] < DOC_TTL:
        return json.loads(json.dumps(hit[1]))
    value = _doc_read(name, default)
    _docs[name] = (time.time(), value)
    return json.loads(json.dumps(value))


def _doc_read(name: str, default):
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
    _docs[name] = (time.time(), json.loads(text))


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
    extractor = c.get("extractor", out["extractor"])
    if extractor not in EXTRACTORS:
        raise ValueError("extractor must be 'claude' or 'firecrawl'")
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
        "mode": mode, "extractor": extractor, "fields": clean,
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


def page_credits(c: dict) -> int:
    """Firecrawl credits per page read."""
    if c["mode"] == "text":
        return CREDITS["text"]
    return CREDITS["structured"] if c.get("extractor") == "firecrawl" else CLAUDE_CREDITS


def per_site(c: dict) -> int:
    """Credits per business, counting the follow-up pages it MAY use."""
    return page_credits(c) * (1 + (c.get("follow_links", 0) if c["mode"] == "structured" else 0))


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
             ("base_run", "VARCHAR"), ("page_kind", "VARCHAR"),
             ("extractor", "VARCHAR")]          # who filled facts: a Claude model or 'firecrawl'
_I = {n: i for i, (n, _) in enumerate(FACT_COLS)}


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
        if DB.exists() and re.search(r"\bq\.", sql):    # only when the query needs the sources
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


KEY_NAMES = ("FIRECRAWL_API_KEY", "FC", "fc")   # the user named it "fc" on Render


def has_key() -> bool:
    return any(os.environ.get(n) for n in KEY_NAMES)


def _key() -> str:
    k = next((os.environ[n] for n in KEY_NAMES if os.environ.get(n)), None)
    if not k:
        raise FirecrawlError("the Firecrawl key is not set (FIRECRAWL_API_KEY or fc — "
                             "QueensLab/.env, or Render's Environment tab)")
    return k.strip()


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
            "period_start": d.get("billingPeriodStart"), "period_end": d.get("billingPeriodEnd")}


def usage(cr: dict | None = None) -> dict:
    """Credits QueensLab itself has used, from its own run records."""
    rs = runs()
    start = (cr or {}).get("period_start") or ""
    total = sum(r.get("credits_used") or 0 for r in rs)
    period = sum(r.get("credits_used") or 0 for r in rs if (r.get("started") or "") >= start[:19])
    active = [r for r in rs if not r.get("collected_at")]
    return {"all_time": total, "this_period": period,
            "this_run": (active[-1].get("credits_used") or 0) if active else None,
            "ai_usd": round(sum((r.get("ai") or {}).get("usd") or 0 for r in rs), 4)}


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
    # Structured mode keeps the page text too: markdown is the base read Firecrawl
    # bills anyway, so it costs nothing extra and lets pages be re-analysed later
    # without paying to read them again. With Claude as the extractor (the
    # default) Firecrawl only reads the text, and Claude fills the fields when
    # the run is collected.
    by_firecrawl = c["mode"] == "structured" and c.get("extractor") == "firecrawl"
    formats = ([{"type": "json", "schema": schema(c), "prompt": c["prompt"]}, "markdown"]
               if by_firecrawl else ["markdown"])
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
    fc = {**c, "extractor": run.get("extractor") or "firecrawl"}   # as its homepage run
    need = len(picks) * page_credits(fc)
    have = credits().get("remaining")
    if have is not None and need > have:
        jobs.Job(run["job_id"]).warn(f"linked pages skipped: {need:,} credits needed, {have:,} left")
        return None
    r = _submit([p[4] for p in picks], fc)
    job = jobs.Job(jobs.queue("web_facts", jobs.new_run(), "follow-up")).start(step=REMOTE_STEP)
    job.warn(f"{len(picks)} linked pages of run {run['run_id']} · up to {need:,} credits")
    return {"run_id": uuid.uuid4().hex[:8], "batch_id": r["id"], "job_id": job.id, "mode": "structured",
            "extractor": fc["extractor"], "n": len(picks), "credits_est": need,
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
    if not _lock.acquire(blocking=False):
        raise FirecrawlError("still collecting the last run (Claude is reading its pages) — "
                             "try again in a few minutes")
    try:
        c = config()
        active = [r for r in runs() if not r.get("collected_at")]
        if active:
            raise FirecrawlError(f"run {active[-1]['run_id']} is still at Firecrawl — stop or collect it first")
        picks = candidates(limit or c["default_batch"], c)
        if not picks:
            raise FirecrawlError("no alive, own-site businesses left to read (run the site check first?)")
        ex = c["extractor"] if c["mode"] == "structured" else None
        if ex == "claude":
            import ask
            if not ask.has_key():
                raise FirecrawlError("Claude reads the facts, but no Anthropic key is set (ANTHROPIC_API_KEY) — "
                                     "set it, or choose Firecrawl AI under Requirements")
        need = len(picks) * per_site(c)
        have = credits().get("remaining")
        if have is not None and need > have:
            raise FirecrawlError(f"this run needs {need:,} credits; {have:,} remain")
        r = _submit([p[4] for p in picks], c)
        job = jobs.Job(jobs.queue("web_facts", jobs.new_run(), origin)).start(step=REMOTE_STEP)
        job.warn(f"{len(picks)} sites · " + {"claude": "facts by Claude", "firecrawl": "facts by Firecrawl",
                                             None: "page text only"}[ex] + f" · up to {need:,} credits"
                 + (f" (incl. up to {c['follow_links']} linked page per site)" if c["follow_links"]
                    and c["mode"] == "structured" else ""))
        run = {"run_id": uuid.uuid4().hex[:8], "batch_id": r["id"], "job_id": job.id,
               "mode": c["mode"], "extractor": ex, "n": len(picks), "credits_est": need,
               "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "status": "scraping", "completed": 0, "credits_used": 0, "expires_at": None,
               "collected_at": None, "invalid": r.get("invalidURLs") or [],
               "kind": "home", "parent": None,
               "urls": [list(p) for p in picks]}
        rs = runs(); rs.append(run); _save_runs(rs)
        return {k: v for k, v in run.items() if k != "urls"}
    finally:
        _lock.release()


def refresh(collect_done: bool = True) -> list[dict]:
    """Update every uncollected run from Firecrawl; collect the finished ones.
    Never waits: while another thread collects (Claude reading a run's pages
    takes minutes) it returns the runs as they stand, so the panel's live
    status keeps answering."""
    if not _lock.acquire(blocking=False):
        return [{k: v for k, v in r.items() if k != "urls"} for r in runs() if not r.get("collected_at")]
    try:
        return _refresh(collect_done)
    finally:
        _lock.release()


def collect_soon() -> bool:
    """Collect finished runs on a background thread, so a web request never
    waits minutes for Claude. False when a collection is already going."""
    if _lock.locked():
        return False
    threading.Thread(target=_collect_quietly, daemon=True, name="crawl-collect").start()
    return True


def _collect_quietly() -> None:
    try:
        refresh()
    except Exception as e:  # noqa: BLE001 — a thread has nobody to raise to
        print(f"[crawl] collect: {type(e).__name__}: {jobs.clean(str(e))}", flush=True)


def _refresh(collect_done: bool) -> list[dict]:
    out = []
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
            n = _collect(run, s, save=lambda: _save_runs(rs))
            run["collected_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            run.pop("extracting", None)
            job.upload("uploaded" if gcs.enabled() else "off",
                       gcs.url("web_facts.parquet", PREFIX) if gcs.enabled() else "no bucket")
            if n < run["n"]:
                job.warn(f"{run['n'] - n} of {run['n']} sites came back empty from Firecrawl "
                         "(not charged)")
            ai = run.get("ai")
            if ai and ai.get("pages"):
                job.warn(f"Claude read {ai['pages'] - ai['failed']} of {ai['pages']} pages · ${ai['usd']:.3f}"
                         + (f" · {ai['failed']} not read (text kept; retry: python crawl.py extract)"
                            if ai["failed"] else ""))
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


def _collect(run: dict, first: dict, save=None) -> int:
    """Every result page of a batch -> web_facts (and its URL -> business).

    With Claude as the extractor the page text is saved first, then Claude
    fills the facts and they are saved chunk by chunk: a server that sleeps
    mid-way loses at most one chunk, and collecting again reuses every page
    already read instead of paying for it twice."""
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
                         md[:60_000] if md else None, meta.get("error"), now,
                         run.get("parent") or run["run_id"], run.get("kind") or "home",
                         "firecrawl" if facts is not None else None])
        nxt = page.get("next")
        if not nxt:
            break
        page = _api("GET", nxt)
    if not rows:
        return 0
    if run.get("extractor") == "claude":
        import extract
        done = dict(query("SELECT url, facts FROM web_facts WHERE run_id = ? AND facts IS NOT NULL",
                          [run["run_id"]]))
        for r in rows:
            if r[_I["url"]] in done:
                r[_I["facts"]], r[_I["extractor"]] = done[r[_I["url"]]], extract.MODEL
        upsert("web_facts", FACT_COLS, rows, ["run_id", "url"])
        _extract([r for r in rows if r[_I["facts"]] is None and r[_I["markdown"]]], run, save)
    else:
        upsert("web_facts", FACT_COLS, rows, ["run_id", "url"])
    _stored["at"] = 0.0               # recount on the next status refresh
    return len(rows)


def _extract(rows: list[list], run: dict | None = None, save=None) -> dict:
    """Claude fills the facts of these web_facts rows from their page text,
    with the requirements as they are now; saved every EXTRACT_CHUNK pages.
    Returns the AI tally (kept on the run too)."""
    import extract
    c = config()
    instructions = extract.system(c)
    ai = (run if run is not None else {}).setdefault(
        "ai", {"model": extract.MODEL, "pages": 0, "failed": 0, "usd": 0.0,
               "input_tokens": 0, "output_tokens": 0})

    def one(r):
        try:
            facts, used = extract.extract(r[_I["markdown"]], r[_I["url"]], c, instructions)
            return facts, used, None
        except Exception as e:  # noqa: BLE001 — one page failing must not stop the rest
            return None, {}, jobs.clean(str(e))

    if run is not None and rows:
        run["extracting"] = {"done": 0, "of": len(rows)}
        if save:
            save()
    for i in range(0, len(rows), EXTRACT_CHUNK):
        chunk = rows[i:i + EXTRACT_CHUNK]
        with cf.ThreadPoolExecutor(EXTRACT_WORKERS) as ex:
            answers = list(ex.map(one, chunk))
        for r, (facts, used, err) in zip(chunk, answers):
            ai["pages"] += 1
            ai["usd"] = round(ai["usd"] + extract.cost(used), 5)
            ai["input_tokens"] += sum(used.get(k, 0) for k in ("input_tokens", "cache_creation_input_tokens",
                                                               "cache_read_input_tokens"))
            ai["output_tokens"] += used.get("output_tokens", 0)
            if facts is None:
                ai["failed"] += 1
                r[_I["error"]] = f"Claude could not read it: {err}"[:300]
                continue
            r[_I["facts"]], r[_I["mode"]], r[_I["extractor"]] = json.dumps(facts), "structured", extract.MODEL
            if (r[_I["error"]] or "").startswith("Claude could not read it"):
                r[_I["error"]] = None
        upsert("web_facts", FACT_COLS, chunk, ["run_id", "url"])
        _stored["at"] = 0.0
        if run is not None:
            run["extracting"] = {"done": i + len(chunk), "of": len(rows)}
            if save:
                save()
    return ai


def extract_stored(all_pages: bool = False, limit: int | None = None) -> dict:
    """Claude reads pages already stored — no Firecrawl credits. By default the
    structured pages it could not read; with all_pages, every stored page (after
    the requirements change)."""
    if not _lock.acquire(blocking=False):
        raise FirecrawlError("a run is being collected right now — try again in a few minutes")
    try:
        cols = [n for n, _ in FACT_COLS if n != "extractor"]       # older files lack it; it is last
        where = "markdown IS NOT NULL AND length(markdown) > 0"
        if not all_pages:
            where += " AND facts IS NULL AND coalesce(mode, 'structured') = 'structured'"
        rows = [list(r) + [None] for r in query(
            f"SELECT {', '.join(chr(34) + n + chr(34) for n in cols)} FROM web_facts WHERE {where} "
            f"ORDER BY fetched_at DESC" + (f" LIMIT {int(limit)}" if limit else ""))]
        return _extract(rows) if rows else {"pages": 0}
    finally:
        _lock.release()


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
    done = refresh(collect_done=False)
    collect_soon()                        # what it finished is kept, collected in the background
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


def _fill(visits: list[dict], names: list[str]) -> dict:
    """How many of these businesses have each field filled. A yes/no field
    counts only a stated YES: the extractor answers 'no' when the page is
    silent, so counting 'no' made every yes/no field look 100% filled."""
    kinds = {f["name"]: f["type"] for f in config()["fields"]}
    def has(v, n):
        return v is True if kinds.get(n) == "yes/no" else not _empty(v)
    return {n: sum(1 for v in visits if has(v["facts"].get(n), n)) for n in names}


_stored = {"at": 0.0, "n": None}


def _stored_count():
    """Rows in the bucket's web_facts file, read from its footer; once a minute."""
    if not gcs.enabled():
        return None
    if time.time() - _stored["at"] < 60:
        return _stored["n"]
    con = gcs._scratch(timeout_s=15)
    try:
        _stored["n"] = con.execute("SELECT sum(num_rows) FROM parquet_file_metadata(?)",
                                   [gcs.url("web_facts.parquet", PREFIX)]).fetchone()[0]
    except Exception:  # noqa: BLE001
        pass
    finally:
        con.close()
    _stored["at"] = time.time()
    return _stored["n"]


def stats() -> dict:
    """What has been read, stored and filled — overall and for the latest run."""
    c = config()
    names = [f["name"] for f in c["fields"] if f["name"] != LINKS_FIELD]
    vis = _visits()
    latest = [vs[-1] for vs in vis.values()]
    pages, with_facts, errors = query(
        "SELECT count(*), count(facts), count(*) FILTER (WHERE facts IS NULL) FROM web_facts")[0]
    stored = _stored_count()
    fill = _fill(latest, names)
    total_filled = sum(fill.values())
    rs = runs()
    last = next((r for r in reversed(rs) if r.get("kind", "home") == "home"), None)
    last_stats = None
    if last:
        mine = [vs[-1] for vs in vis.values() if vs[-1]["visit"] == last["run_id"]]
        follow = next((r for r in rs if r.get("parent") == last["run_id"]), None)
        lf = _fill(mine, names)
        last_stats = {"run_id": last["run_id"], "started": last.get("started"),
                      "sent": last.get("n"), "returned": last.get("completed"),
                      "businesses_with_facts": len(mine), "facts_filled": sum(lf.values()),
                      "avg_fields_per_business": round(sum(lf.values()) / len(mine), 1) if mine else 0,
                      "linked_pages": follow.get("completed") if follow else 0,
                      "credits_used": (last.get("credits_used") or 0) + ((follow or {}).get("credits_used") or 0),
                      "ai_usd": round(((last.get("ai") or {}).get("usd") or 0)
                                      + (((follow or {}).get("ai") or {}).get("usd") or 0), 4),
                      "finished": (follow or last).get("collected_at") if last.get("collected_at") else None,
                      "status": "running" if not last.get("collected_at") or (follow and not follow.get("collected_at"))
                                else last.get("status")}
    return {"businesses_read": len(latest), "pages_read": pages, "pages_with_facts": with_facts,
            "pages_failed": errors, "stored_in_bucket": stored, "local_rows": pages,
            "facts_filled": total_filled, "fields": len(names),
            "field_fill": [{"field": n, "filled": fill[n],
                            "yes_no": any(f["name"] == n and f["type"] == "yes/no" for f in c["fields"]),
                            "pct": round(100 * fill[n] / len(latest), 1) if latest else 0} for n in names],
            "last_run": last_stats}


def progress() -> dict:
    """For the panel's live status: update from Firecrawl (collecting anything
    finished), then the active run, if any, and the statistics."""
    if any(not r.get("collected_at") for r in runs()):
        now = refresh(collect_done=False)
        if any(r.get("status") in ("completed", "failed", "cancelled") for r in now):
            collect_soon()
    active = [r for r in runs() if not r.get("collected_at")]
    a = active[-1] if active else None
    try:
        cr = credits()
    except Exception as e:  # noqa: BLE001 — say why, in the panel
        cr = {"error": jobs.clean(str(e))}
    return {"credits": cr, "usage": usage(cr), "running": bool(a),
            "active": {k: a.get(k) for k in ("run_id", "kind", "status", "n", "completed", "credits_used",
                                             "credits_est", "started", "expires_at", "extractor",
                                             "extracting", "ai")} if a else None,
            "stats": stats()}


_SQL_TYPES = {"text": "VARCHAR", "number": "DOUBLE", "yes/no": "BOOLEAN", "list": "VARCHAR[]",
              "prices": "JSON"}


CRAWL_TABLES = ("web_latest", "web_facts", "site_checks")


def analysis_db(scope: str = "mixed"):
    """An in-memory DuckDB for the Query box's 'Firecrawl data' and 'Mixed':

        web_latest   one row per business (latest read, linked pages merged),
                     one column per requirement field, typed
        web_facts    every page read (facts JSON, page text, evidence, status)
        site_checks  the free website check, one row per business
        <public>     Mixed only: every public table under its own name (and q.<name>)

    Built FRESH per query from the stored files (refreshed from the bucket
    within 5 minutes), so a query always sees the latest collected results.
    Then locked down like the public box: no files, no web."""
    import duckdb
    c = config()
    fields = [f for f in c["fields"] if f["name"] != LINKS_FIELD]
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    for name, cols in (("web_facts", FACT_COLS), ("site_checks", SITE_COLS)):
        f = local_table(name)
        if f:
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet({_q(f)}, union_by_name=true)")
        else:
            con.execute(f"CREATE TABLE {name} (" + ", ".join(f'"{n}" {t}' for n, t in cols) + ")")
    cols = [("place_id", "VARCHAR"), ("name", "VARCHAR"), ("category", "VARCHAR"), ("nta2020", "VARCHAR"),
            ("pages_read", "VARCHAR")] + [(f["name"], _SQL_TYPES[f["type"]]) for f in fields] + \
           [("evidence", "JSON"), ("changed_since_last_read", "VARCHAR[]"), ("reads", "INTEGER"),
            ("fetched_at", "TIMESTAMP")]
    con.execute("CREATE TABLE web_latest (" + ", ".join(f'"{n}" {t}' for n, t in cols) + ")")
    rows = []
    for pid, vs in _visits().items():
        last, prev = vs[-1], (vs[-2] if len(vs) > 1 else None)
        vals = []
        for f in fields:
            v = last["facts"].get(f["name"])
            if _empty(v):
                vals.append(None)
            elif f["type"] == "number":
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    vals.append(None)
            elif f["type"] == "yes/no":
                vals.append(bool(v) if isinstance(v, bool) else str(v).lower() in ("true", "yes"))
            elif f["type"] == "list":
                vals.append([str(x) for x in v] if isinstance(v, list) else [str(v)])
            elif f["type"] == "prices":
                vals.append(json.dumps(v))
            else:
                vals.append(str(v))
        changed = [f["name"] for f in fields if prev is not None
                   and _show(last["facts"].get(f["name"])) != _show(prev["facts"].get(f["name"]))]
        rows.append([pid, last["name"], last["category"], last["nta2020"], " | ".join(last["pages"]),
                     *vals, json.dumps(last["evidence"]), changed or None, len(vs), last["at"]])
    if rows:
        con.executemany(f"INSERT INTO web_latest VALUES ({', '.join('?' * len(cols))})", rows)
    if scope == "mixed" and DB.exists():
        con.execute(f"ATTACH {_q(DB)} AS q (READ_ONLY)")
        if gcs.enabled():
            gcs.attach(con)               # the public big tables are views over the bucket
        public = con.execute("""SELECT table_name FROM duckdb_tables() WHERE database_name = 'q'
                                UNION SELECT view_name FROM duckdb_views()
                                WHERE database_name = 'q' AND NOT internal""").fetchall()
        for (t,) in public:
            if not t.startswith("_") and t not in CRAWL_TABLES:
                con.execute(f'CREATE VIEW "{t}" AS SELECT * FROM q."{t}"')
    for stmt in ("SET memory_limit='300MB'", "SET threads=2",
                 "SET disabled_filesystems='LocalFileSystem,HTTPFileSystem'",
                 "SET lock_configuration=true"):
        con.execute(stmt)
    return con


EXAMPLES = {
    "free": [
        ("Restaurants per resident", "SELECT name, residents, restaurants, restaurants_per_10k\nFROM nta_profile "
         "WHERE kind = 'residential' ORDER BY restaurants_per_10k DESC LIMIT 15"),
        ("311 complaints by type", "SELECT complaint_type, count(*) AS complaints\nFROM nyc_311 GROUP BY 1 ORDER BY 2 DESC LIMIT 15"),
        ("Busiest subway stations", "SELECT station_complex, round(sum(TRY_CAST(riders AS DOUBLE)) / "
         "count(DISTINCT TRY_CAST(transit_timestamp AS TIMESTAMP)::DATE)) AS riders_per_day\nFROM subway_ridership "
         "GROUP BY 1 ORDER BY 2 DESC LIMIT 15"),
        ("Storefront vacancy by neighbourhood", "SELECT name, storefronts, vacant, vacancy_pct\nFROM nta_profile "
         "WHERE storefronts >= 30 ORDER BY vacancy_pct DESC LIMIT 15"),
    ],
    "firecrawl": [
        ("Businesses and their facts",
         "SELECT name, nta2020, business_type, opening_hours, price_level, services_or_products\n"
         "FROM web_latest ORDER BY fetched_at DESC LIMIT 50"),
        ("Every price found",
         "SELECT w.name, p.item, p.price, p.currency, p.unit\nFROM web_latest w, "
         "unnest(from_json(w.prices, '[{\"item\":\"VARCHAR\",\"price\":\"DOUBLE\",\"currency\":\"VARCHAR\",\"unit\":\"VARCHAR\"}]')) t(p)\n"
         "ORDER BY p.price DESC"),
        ("Dead websites by neighbourhood",
         "SELECT ntaname, count(*) AS sites,\n  count(*) FILTER (WHERE status IN ('dead','parked','hijacked')) AS gone,\n"
         "  round(100.0 * gone / sites, 1) AS pct_gone\nFROM site_checks GROUP BY 1 HAVING sites >= 30 ORDER BY pct_gone DESC"),
        ("Hiring or opening soon",
         "SELECT name, nta2020, now_hiring, opening_soon_notice FROM web_latest\n"
         "WHERE now_hiring OR opening_soon_notice IS NOT NULL"),
        ("What changed since the last read",
         "SELECT name, changed_since_last_read, reads FROM web_latest\nWHERE changed_since_last_read IS NOT NULL"),
        ("Search the page text",
         "SELECT name, url FROM web_facts\nWHERE markdown ILIKE '%halal%' LIMIT 50"),
    ],
    "mixed": [
        ("Open late, by neighbourhood",
         "SELECT n.name AS neighbourhood, count(*) AS open_late\nFROM web_latest w "
         "JOIN nta_profile n ON n.code = w.nta2020\nWHERE w.open_late GROUP BY 1 ORDER BY 2 DESC"),
        ("Dead websites vs storefront vacancy",
         "SELECT n.name, n.vacancy_pct,\n  round(100.0 * count(*) FILTER (WHERE s.status = 'dead') / count(*), 1) AS dead_site_pct\n"
         "FROM site_checks s JOIN nta_profile n ON n.code = s.nta2020\nGROUP BY 1, 2 HAVING count(*) >= 30 ORDER BY 3 DESC"),
        ("Crawled businesses with their neighbourhood",
         "SELECT w.name, w.business_type, n.name AS neighbourhood, n.median_income, n.subway_weekday_riders\n"
         "FROM web_latest w JOIN nta_profile n ON n.code = w.nta2020 LIMIT 50"),
    ],
}


def overview() -> dict:
    """Public-safe summary: no URLs, no extracted text."""
    checks = dict(query("SELECT status, count(*) FROM site_checks GROUP BY 1"))
    facts = query("SELECT count(*), count(facts), max(fetched_at), "
                  "count(DISTINCT place_id) FILTER (WHERE facts IS NOT NULL) FROM web_facts")[0]
    last_check = query("SELECT max(checked_at) FROM site_checks")[0][0]
    active = [{k: r.get(k) for k in ("run_id", "status", "n", "completed", "credits_used",
                                      "expires_at", "started", "mode")}
              for r in runs() if not r.get("collected_at")]
    return {"site_checks": checks, "sites_checked": sum(checks.values()),
            "last_check": str(last_check) if last_check else None,
            "pages_read": facts[0], "with_facts": facts[1], "businesses_read": facts[3],
            "last_read": str(facts[2]) if facts[2] else None, "active_run": active[-1] if active else None}


CAN_PULL = [
    ("Facts you define", "Any field you can describe in a sentence — hours, prices, menu items, services, "
     "brands carried, languages, delivery, booking, payment methods, parking, wheelchair access, "
     "year founded, 'closed/moved' notices. Firecrawl reads the page (1 credit) and Claude Haiku fills "
     "them in (about a tenth of a cent). Firecrawl's own AI can do it instead, at 5 credits a page."),
    ("The page as text", "The readable content of the page (markdown). 1 credit a page; facts are then "
     "found by searching the text."),
    ("Page details", "Title, description, language, the final address after redirects, and the HTTP status."),
    ("Free, without Firecrawl", "Whether each website still exists, loads, is parked or for sale, has been "
     "taken over by spam, or is really a platform page — for every Queens business that lists a site."),
    ("Not collected, on purpose", "Reviews, personal details of owners or staff, and anything from Google "
     "Maps, Yelp, social media or listing sites."),
]
