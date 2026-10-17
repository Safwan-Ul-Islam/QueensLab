#!/usr/bin/env python3
"""Read Queens businesses' OWN websites with Firecrawl.

    python crawl.py              50 pages (default) — fits Firecrawl's free plan
    python crawl.py --limit 200
    python crawl.py --dry-run    show which sites would be read; costs nothing

Which sites: open businesses from `overture_places` that list a website,
confidence >= 0.7, one page per domain, spread across Queens. A page read in
the last 7 days is skipped. The key is FIRECRAWL_API_KEY in .env.

⚠️ RESEARCH ONLY. These are scraped pages: they live in scraped/ (locally and
in the bucket), apart from the public records, and are never merged into
BizGenome or used to train anything (BizGenome log, 2026-10-10: "scraping
never touches the model"). Only a business's own site is read — never Google
Maps, Yelp, social media or listing sites.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gcs  # noqa: E402  (also loads .env)
import jobs  # noqa: E402

DB = HERE / "queens.duckdb"
OUT = HERE / "scraped"
PAGES = OUT / "web_pages.parquet"
API = "https://api.firecrawl.dev/v2/scrape"
MAX_CHARS = 20_000          # enough to find hours, menus and prices
RECENT_DAYS = 7


class OutOfCredits(Exception):
    pass


def _q(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def candidates(limit: int) -> list[tuple]:
    import duckdb
    c = duckdb.connect()
    try:
        c.execute(f"ATTACH {_q(DB)} AS q (READ_ONLY)")
        recent = ""
        if PAGES.exists():
            recent = (f"AND website NOT IN (SELECT url FROM read_parquet({_q(PAGES)}) "
                      f"WHERE fetched_at > now() - INTERVAL {RECENT_DAYS} DAY)")
        return c.execute(f"""
            SELECT id, name, category, nta2020, website FROM (
              SELECT *, regexp_extract(lower(website), '^https?://(www\\.)?([^/]+)', 2) AS domain
              FROM q.overture_places
              WHERE operating_status = 'open' AND confidence >= 0.7 AND website LIKE 'http%' {recent})
            WHERE domain <> ''
              AND domain NOT SIMILAR TO '.*(facebook|instagram|yelp|google|tiktok|twitter|x\\.com|linktr).*'
            QUALIFY row_number() OVER (PARTITION BY domain ORDER BY confidence DESC, id) = 1
            ORDER BY hash(id) LIMIT {int(limit)}""").fetchall()
    finally:
        c.close()


def scrape(url: str, key: str) -> dict:
    body = json.dumps({"url": url, "formats": ["markdown"], "onlyMainContent": True,
                       "timeout": 30000}).encode()
    req = urllib.request.Request(API, data=body, method="POST", headers={
        "Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 402:
            raise OutOfCredits("Firecrawl says the plan's credits are used up") from e
        if e.code == 401:
            raise OutOfCredits("Firecrawl rejected the API key") from e
        return {"ok": False, "error": f"HTTP {e.code}: {e.read().decode()[:200]}"}
    data = out.get("data", out) if isinstance(out, dict) else {}
    meta = data.get("metadata") or {}
    md = data.get("markdown") or ""
    return {"ok": bool(out.get("success", bool(md))), "markdown": md[:MAX_CHARS],
            "title": meta.get("title"), "description": meta.get("description"),
            "status": meta.get("statusCode"), "final_url": meta.get("sourceURL") or meta.get("url"),
            "error": out.get("error")}


def save(rows: list[dict]) -> int:
    """Append to scraped/web_pages.parquet (one file, all runs) and upload it."""
    import duckdb
    OUT.mkdir(exist_ok=True)
    c = duckdb.connect()
    try:
        c.execute("""CREATE TABLE t (place_id VARCHAR, name VARCHAR, category VARCHAR, nta2020 VARCHAR,
                     url VARCHAR, final_url VARCHAR, ok BOOLEAN, http_status INTEGER, title VARCHAR,
                     description VARCHAR, markdown VARCHAR, chars INTEGER, error VARCHAR,
                     fetched_at TIMESTAMP)""")
        if PAGES.exists():
            c.execute(f"INSERT INTO t SELECT * FROM read_parquet({_q(PAGES)})")
        c.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      [[r[k] for k in ("place_id", "name", "category", "nta2020", "url", "final_url", "ok",
                                       "http_status", "title", "description", "markdown", "chars",
                                       "error", "fetched_at")] for r in rows])
        tmp = PAGES.with_suffix(".tmp.parquet")
        c.execute(f"COPY t TO {_q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        tmp.replace(PAGES)
        return c.execute("SELECT count(*) FROM t").fetchone()[0]
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    picks = candidates(a.limit)
    if a.dry_run or not picks:
        for _id, name, cat, nta, url in picks[:20]:
            print(f"  {nta}  {str(name)[:34]:<34} {str(cat)[:22]:<22} {url}")
        print(f"\n  {len(picks)} site(s) would be read{' (dry run)' if a.dry_run else ''}\n")
        return 0
    key = os.environ.get("FIRECRAWL_API_KEY")
    if not key:
        print("  FIRECRAWL_API_KEY is not set — add it to .env (free key at firecrawl.dev)")
        return 2
    job = jobs.Job(jobs.queue("web_pages", jobs.new_run(), "scrape")).start(step="reading sites")
    rows, failed = [], 0
    try:
        for i, (pid, name, cat, nta, url) in enumerate(picks, 1):
            r = scrape(url, key)
            failed += not r["ok"]
            rows.append({"place_id": pid, "name": name, "category": cat, "nta2020": nta, "url": url,
                         "final_url": r.get("final_url"), "ok": r["ok"], "http_status": r.get("status"),
                         "title": r.get("title"), "description": r.get("description"),
                         "markdown": r.get("markdown"), "chars": len(r.get("markdown") or ""),
                         "error": jobs.clean(r.get("error")),
                         "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None)})
            job.add_rows(1)
            print(f"  {i:>3}/{len(picks)} {'ok ' if r['ok'] else 'ERR'} {url[:70]}", flush=True)
            time.sleep(1)   # polite: one site at a time
    except OutOfCredits as e:
        job.warn(str(e))
        print(f"\n  stopped: {e}")
    except (KeyboardInterrupt, SystemExit):
        if rows:
            save(rows)
        job.fail("interrupted")
        raise
    if not rows:
        job.fail("no page was read")
        return 1
    total = save(rows)
    if failed:
        job.warn(f"{failed} of {len(rows)} sites could not be read")
    if gcs.enabled():
        job.step("uploading")
        try:
            job.upload("uploaded", gcs.upload(PAGES, "web_pages.parquet", prefix="scraped"))
        except Exception as e:  # noqa: BLE001
            job.upload("failed", f"{type(e).__name__}: {e}")
    else:
        job.upload("off", "no bucket configured")
    job.succeed(len(rows))
    print(f"\n  read {len(rows) - failed} of {len(rows)} sites · {total} pages kept in {PAGES.name}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
