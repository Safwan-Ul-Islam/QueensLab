#!/usr/bin/env python3
"""Pull every declared Queens source into a local DuckDB file.

    python research/fetch.py --list              what is available
    python research/fetch.py --core              the 5 that matter most
    python research/fetch.py --all               everything
    python research/fetch.py --only dcwp_licences
    python research/fetch.py --all --force       ignore the "already fetched" cache

Writes:  research/raw/<key>.parquet   (the raw pull, kept)
         research/queens.duckdb       (a table per source)

⚠️  Nothing here touches the production database. That is the point.
"""
from __future__ import annotations

import argparse
import csv
import http.client
import io
import json
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import sources as S  # noqa: E402
import gcs  # noqa: E402

RAW = HERE / "raw"
DB = HERE / "queens.duckdb"
PAGE = 20_000                      # small enough for Render's 512MB free plan
WORKERS = 3                        # months pulled at once by pull_windows (laptop only)
UA = {"User-Agent": "BizGenome-research/1.0 (analysis only; not served)"}


def _get(url: str, timeout: int = 180) -> list[dict]:
    """GET one page. A busy server (5xx, 429, timeout, cut connection) is asked
    again with backoff — Socrata answered 503 to a plain count during probing.
    A bad query (other 4xx) fails at once; asking again cannot fix it."""
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if (e.code < 500 and e.code != 429) or attempt == 5:
                raise
            reason = f"HTTP {e.code}"
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as e:
            if attempt == 5:
                raise
            reason = type(e).__name__
        wait = 2 ** (attempt + 1)
        print(f"      retry {attempt+1}/5 in {wait}s ({reason})", flush=True)
        time.sleep(wait)
    return []


def _order(src: S.Source) -> str:
    """A TOTAL order, so offset paging never repeats or skips a row.

    ⚠️ Ordering by a column with ties (camis, borough_block_lot) let tied rows
    come back in a different order on the next page request. storefront_vacancy
    arrived with 18,547 exact duplicate rows — and as many real rows missing,
    since the total still matched — and dohmh_food with 74 (found 2026-10-17).
    `:id` breaks every tie; an aggregate is already unique on its group."""
    if src.group:
        return src.group
    return f"{src.order_by}, :id" if src.order_by else ":id"


def _where(src: S.Source, override: str | None = None) -> str | None:
    """The source's Queens filter, plus its date floor if it has one."""
    if override is not None:
        return override
    floor = f"{src.window_by} >= '{src.since}'" if src.window_by and src.since else None
    parts = [p for p in (src.where, floor) if p]
    return " AND ".join(f"({p})" for p in parts) if parts else None


def _pages(src: S.Source, where: str | None, fh, cap: int | None = None,
           quiet: bool = False) -> int:
    """Offset-page one query into an open NDJSON file. Peak memory is one page."""
    base = f"https://{src.domain}/resource/{src.dataset}.json"
    total, offset = 0, 0
    while True:
        params = {"$limit": PAGE, "$offset": offset, "$order": _order(src)}
        if where:
            params["$where"] = where
        # Dropped by accident in the streaming rewrite (d985008), so
        # storefront_vacancy was stored per storefront, not per lot, and its
        # panel on the page queried columns that did not exist.
        if src.select:
            params["$select"] = src.select
        if src.group:
            params["$group"] = src.group
        batch = _get(f"{base}?{urllib.parse.urlencode(params)}")
        got = len(batch)
        for r in batch:
            fh.write(json.dumps({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                 for k, v in r.items()}) + "\n")
        total += got
        if not quiet:
            print(f"      {total:>9,} rows", end="\r", flush=True)
        del batch                      # release the page before the next request
        fh.flush()
        if got < PAGE:                 # short page = last page
            break
        offset += PAGE
        if cap and total >= cap:
            print(f"\n      stopped at --cap {cap:,}")
            break
    return total


def pull_to_file(src: S.Source, path: Path, where_override: str | None = None,
                 cap: int | None = None) -> int:
    """Page through one dataset, writing each page straight to NDJSON.

    ⚠️ Earlier this accumulated every row in a list and only then wrote it.
    That was fine on a laptop and got OOM-killed on Render's 512MB free plan
    partway through PLUTO (324k rows) — the service restarted and the fetch
    vanished with no error. Peak memory is now one page, not one dataset.
    """
    with path.open("w", encoding="utf-8") as fh:
        return _pages(src, _where(src, where_override), fh, cap)


def _months(since: str):
    """[start, end) calendar-month windows from `since` up to now, UTC."""
    start, now = datetime.fromisoformat(since), _utc_now()
    while start < now:
        end = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield start, end
        start = end


def pull_windows(src: S.Source, parts: Path) -> int:
    """Pull a big time-series source one calendar month at a time.

    Two years of Queens 311 is 1.87M rows. As one query that means paging to
    an offset of 1.8M, where Socrata slows to a crawl and one bad minute loses
    the lot. As ~25 month queries of ~75k rows each, WORKERS at once, a month
    lands in parts/<YYYY-MM>.jsonl only once it is complete — so a run that
    dies resumes where it stopped. The newest month is always pulled again,
    because it is still filling up.
    """
    parts.mkdir(parents=True, exist_ok=True)
    wins = list(_months(src.since))
    newest = wins[-1][0]
    recent = time.time() - 24 * 3600
    base = f"https://{src.domain}/resource/{src.dataset}.json"

    def one(win):
        start, end = win
        out = parts / f"{start:%Y-%m}.jsonl"
        if out.exists() and start != newest and out.stat().st_mtime > recent:
            with out.open(encoding="utf-8") as fh:
                return start, sum(1 for _ in fh), None, True
        bound = (f"{src.window_by} >= '{start:%Y-%m-%dT%H:%M:%S}' AND "
                 f"{src.window_by} < '{end:%Y-%m-%dT%H:%M:%S}'")
        where = f"({src.where}) AND {bound}" if src.where else bound
        tmp = out.with_suffix(".partial")
        with tmp.open("w", encoding="utf-8") as fh:
            n = _pages(src, where, fh, quiet=True)
        tmp.replace(out)
        api = None
        if not src.group:   # completeness check; an aggregate has no cheap count
            q = urllib.parse.urlencode({"$select": "count(*) AS n", "$where": where})
            api = int(_get(f"{base}?{q}")[0]["n"])
        return start, n, api, False

    total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for start, n, api, kept in pool.map(one, wins):
            total += n
            flag = ("  (kept from the last run)" if kept else
                    "" if api in (None, n) else f"  ⚠ api says {api:,}")
            print(f"      {start:%Y-%m}{n:>10,} rows{flag}   total {total:,}", flush=True)
    return total


def rows_to_file(rows: list[dict], path: Path) -> int:
    """Dump an in-memory list to NDJSON. Only for the small sources (acs, cbp)
    that arrive whole; Socrata streams straight to disk instead."""
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                 for k, v in r.items()}) + "\n")
    return len(rows)


def save(key: str, tmp: Path, n_rows: int) -> Path:
    """NDJSON on disk -> DuckDB table -> Parquet. DuckDB does all of it.

    pyarrow is deliberately NOT a dependency: the copy in the user's anaconda
    env is compiled against NumPy 1.x and fails under NumPy 2.x. DuckDB reads
    JSON and writes Parquet itself, so that whole class of breakage is gone.
    """
    path = RAW / f"{key}.parquet"
    con = _connect_rw()
    # sample_size=-1 reads every row to learn the columns. Socrata leaves a
    # field OUT of a row when it is empty, and DuckDB's default looks at only
    # the first 20,480 rows, so a column first seen later is never created.
    # PLUTO (324k rows) came back with 93 of the dataset's 108 columns.
    con.execute(
        f'CREATE OR REPLACE TABLE "{key}" AS '
        "SELECT * FROM read_json_auto(?, union_by_name=true, sample_size=-1, "
        "maximum_object_size=20000000)",
        [str(tmp / "*.jsonl") if tmp.is_dir() else str(tmp)])
    con.execute(f'COPY "{key}" TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [str(path)])
    con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                   (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
    con.execute("DELETE FROM _fetch_log WHERE key = ?", [key])
    con.execute("INSERT INTO _fetch_log VALUES (?,?,?,?)",
                [key, n_rows, _utc_now(), path.stat().st_size])
    if gcs.enabled():
        try:
            gcs.attach(con)
            dest = gcs.push_table(con, key)
            gcs.push_log(con)
            print(f"\n    -> {dest}", end="")
        except Exception as e:  # noqa: BLE001 — the local copy is already safe
            print(f"\n    ⚠ bucket push failed, local copy kept: {type(e).__name__}: {str(e)[:120]}",
                  end="")
    con.close()
    _discard(tmp)
    _manifest_put(key, n_rows)
    return path


def _discard(tmp: Path) -> None:
    if tmp.is_dir():
        shutil.rmtree(tmp)
    else:
        tmp.unlink(missing_ok=True)


def _connect_rw():
    """Open the database for writing, waiting out another writer.

    DuckDB allows one writer per file. With a big source pulling in one shell
    and the rest in another, the second save now waits its turn (up to 20 min)
    instead of crashing after an hour of downloading."""
    import duckdb
    for _ in range(240):
        try:
            return duckdb.connect(str(DB))
        except duckdb.IOException as e:
            if "lock" not in str(e).lower():
                raise
            time.sleep(5)
    return duckdb.connect(str(DB))


MANIFEST = RAW / "_manifest.json"


def _utc_now() -> datetime:
    """Naive UTC, on purpose.

    Handing DuckDB a timezone-AWARE datetime makes it convert to the session
    time zone — the machine's local zone — before storing in a TIMESTAMP. On a
    Mac in Asia/Dhaka that silently stored every fetch time 6 hours ahead,
    while Render (UTC) stored the truth, so the same column held two clocks.
    Found 2026-10-17 when a live refresh appeared to happen 6h in the past."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _as_utc_naive(iso: str | None):
    """Manifest strings carry an offset; normalise in Python, not in DuckDB's
    cast, so the result never depends on the session time zone."""
    if not iso:
        return None
    t = datetime.fromisoformat(iso)
    if t.tzinfo is not None:
        t = t.astimezone(timezone.utc).replace(tzinfo=None)
    return t


def _manifest_put(key: str, n_rows: int) -> None:
    m = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    m[key] = {"rows": n_rows, "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    MANIFEST.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")


def rebuild_from_parquet(db: Path = DB) -> int:
    """Recreate the DuckDB file from the committed Parquet in raw/.

    This is what makes storage work on Render's free plan. The disk is wiped
    on every restart, but raw/*.parquet is IN THE REPO (~19 MB), so each boot
    rebuilds every table in about five seconds. Refreshing the data means:
    fetch locally, commit raw/, push — Render redeploys with it.
    """
    import duckdb
    m = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    con = duckdb.connect(str(db))
    con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                   (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
    total = 0
    for f in sorted(RAW.glob("*.parquet")):
        key = f.stem
        con.execute(f'CREATE OR REPLACE TABLE "{key}" AS SELECT * FROM read_parquet(?)', [str(f)])
        n = con.execute(f'SELECT count(*) FROM "{key}"').fetchone()[0]
        con.execute("DELETE FROM _fetch_log WHERE key = ?", [key])
        con.execute("INSERT INTO _fetch_log VALUES (?,?,?,?)",
                    [key, n, _as_utc_naive((m.get(key) or {}).get("fetched_at")),
                     f.stat().st_size])
        total += n
    con.close()
    return total


# Above either line, a table stays in the bucket and is read in place.
LAZY_ROWS = 250_000
LAZY_BYTES = 20_000_000


def rebuild_from_gcs(db: Path = DB) -> int:
    """Recreate the database from the bucket. Raises if the bucket is unusable,
    so the caller can fall back to the Parquet committed in raw/.

    Small tables are copied in. Big ones (311, permits, PLUTO, subway) become
    VIEWS over their Parquet in the bucket: copying ~4M rows into a 512 MB free
    dyno on every wake-up would take minutes, if it finished at all, while a
    view costs nothing until queried — and then reads only the columns asked
    for. Every connection that queries one needs the bucket key (app.con())."""
    import duckdb
    con = duckdb.connect(str(db))
    try:
        gcs.attach(con)
        tables = gcs.list_tables(con)
        if not tables:
            raise RuntimeError(f"no tables under gs://{gcs.bucket()}/{gcs.PREFIX}/")
        # The log first: its row and byte counts decide copy or view.
        try:
            con.execute("CREATE OR REPLACE TABLE _fetch_log AS SELECT * FROM read_parquet(?)",
                        [gcs.url("_fetch_log.parquet")])
        except Exception:  # noqa: BLE001 — tables without dates beat no tables
            con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                           (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
        size = {k: (r or 0, b or 0) for k, r, b in
                con.execute("SELECT key, rows, bytes FROM _fetch_log").fetchall()}
        total = 0
        for key in tables:
            src = gcs.url(f"{key}.parquet")
            rows, nbytes = size.get(key, (0, 0))
            if rows > LAZY_ROWS or nbytes > LAZY_BYTES:
                lit = src.replace("'", "''")
                con.execute(f'CREATE OR REPLACE VIEW "{key}" AS '
                            f"SELECT * FROM read_parquet('{lit}')")
            else:
                con.execute(f'CREATE OR REPLACE TABLE "{key}" AS SELECT * FROM read_parquet(?)',
                            [src])
            total += con.execute(f'SELECT count(*) FROM "{key}"').fetchone()[0]
        return total
    finally:
        con.close()


def push_all() -> int:
    """Upload every table already in the local database to the bucket."""
    import duckdb
    if not gcs.enabled():
        print("  GCS not configured — set GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET in .env")
        return 1
    con = duckdb.connect(str(DB))
    try:
        gcs.attach(con)
        keys = [r[0] for r in con.execute("SELECT key FROM _fetch_log ORDER BY rows").fetchall()]
        for k in keys:
            t0 = time.time()
            dest = gcs.push_table(con, k)
            n = con.execute(f'SELECT count(*) FROM "{k}"').fetchone()[0]
            print(f"  {k:<20}{n:>10,} rows -> {dest}  ({time.time()-t0:.1f}s)")
        gcs.push_log(con)
        print(f"  _fetch_log -> {gcs.url('_fetch_log.parquet')}")
        print(f"\n  bucket now holds {len(gcs.list_tables(con))} tables")
        return 0
    finally:
        con.close()


# Restored 2026-10-17: the streaming rewrite (d985008) deleted these while
# main() still called them, so refreshing acs_tracts or cbp_industry died
# with a NameError.
def pull_acs() -> list[dict]:
    """ACS 2023 5-year tracts for Queens County, from the public table download."""
    tables = {
        "population": ("ACSDT5Y2023.B01003", "B01003_001E"),
        "median_income": ("ACSDT5Y2023.B19013", "B19013_001E"),
        "median_age": ("ACSDT5Y2023.B01002", "B01002_001E"),
        "occupied_homes": ("ACSDT5Y2023.B25003", "B25003_001E"),
        "renter_homes": ("ACSDT5Y2023.B25003", "B25003_003E"),
    }
    by_geo: dict[str, dict] = {}

    def take(table_id: str, field: str, dest: str, keep_name: bool = False) -> None:
        url = ("https://data.census.gov/api/access/data/table?"
               + urllib.parse.urlencode({"id": table_id, "g": "050XX00US36081$1400000"}))
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=90) as resp:
            grid = json.loads(resp.read().decode())["response"]["data"]
        header, *body = grid
        idx = {name: i for i, name in enumerate(header)}
        for row in body:
            geo = row[idx["GEO_ID"]]
            rec = by_geo.setdefault(geo, {"tract": geo[-6:]})
            if keep_name:
                rec["name"] = row[idx["NAME"]]
            raw = row[idx[field]]
            try:
                number = float(raw)
            except (TypeError, ValueError):
                rec[dest] = None
                continue
            # Census uses large negatives for missing, not for a real zero.
            rec[dest] = None if number < -100000 else number

    take(*tables["population"], "population", keep_name=True)
    take(*tables["median_income"], "median_income")
    take(*tables["median_age"], "median_age")
    take(*tables["occupied_homes"], "occupied_homes")
    take(*tables["renter_homes"], "renter_homes")
    return list(by_geo.values())


_SECTOR = {
    "11": "Agriculture, forestry, fishing and hunting",
    "21": "Mining, quarrying, and oil and gas extraction",
    "22": "Utilities", "23": "Construction",
    "31": "Manufacturing", "32": "Manufacturing", "33": "Manufacturing",
    "42": "Wholesale trade", "44": "Retail trade", "45": "Retail trade",
    "48": "Transportation and warehousing", "49": "Transportation and warehousing",
    "51": "Information", "52": "Finance and insurance",
    "53": "Real estate and rental and leasing",
    "54": "Professional, scientific, and technical services",
    "55": "Management of companies and enterprises",
    "56": "Administrative and support and waste management",
    "61": "Educational services", "62": "Health care and social assistance",
    "71": "Arts, entertainment, and recreation",
    "72": "Accommodation and food services",
    "81": "Other services (except public administration)",
    "92": "Public administration",
}


def _industry_name(naics: str) -> str:
    if naics == "------":
        return "All industries"
    sector = _SECTOR.get(naics.replace("-", "")[:2], "")
    if "-" in naics:
        return sector or naics
    return f"{sector} · {naics}" if sector else naics


def pull_cbp() -> list[dict]:
    """Queens rows from the 2023 County Business Patterns county file."""
    url = "https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23co.zip"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=180) as resp:
        blob = resp.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        name = next(n for n in archive.namelist() if n.endswith(".txt"))
        text = archive.read(name).decode("latin1")
    rows = []
    for rec in csv.DictReader(io.StringIO(text)):
        if rec.get("fipstate") != "36" or rec.get("fipscty") != "081":
            continue
        naics = rec.get("naics") or ""

        def num(value: str | None):
            try:
                number = float(value) if value not in (None, "", "N") else None
            except (TypeError, ValueError):
                return None
            return number

        rows.append({
            "naics": naics,
            "industry": _industry_name(naics),
            "establishments": num(rec.get("est")),
            "employment": num(rec.get("emp")),
        })
    return rows


def already(key: str) -> tuple[int, str] | None:
    import duckdb
    if not DB.exists():
        return None
    con = _connect_rw()
    try:
        r = con.execute("SELECT rows, fetched_at FROM _fetch_log WHERE key = ?", [key]).fetchone()
        return (r[0], str(r[1])[:16]) if r else None
    except duckdb.CatalogException:
        return None
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--core", action="store_true")
    ap.add_argument("--only", action="append", default=[])
    ap.add_argument("--force", action="store_true", help="refetch even if already pulled")
    ap.add_argument("--cap", type=int, help="stop each source after N rows (for a quick trial)")
    ap.add_argument("--where", help="override the WHERE for --only (e.g. a date bound)")
    ap.add_argument("--push", action="store_true",
                    help="upload every local table to the GCS bucket, then exit")
    a = ap.parse_args()

    if a.push:
        return push_all()

    if a.list or not (a.all or a.core or a.only):
        print(f"\n  {'key':<20}{'rows when probed':>18}  source")
        print("  " + "-" * 74)
        for s in S.ALL:
            got = already(s.key)
            mark = f"  [have {got[0]:,}]" if got else ""
            n = f"{s.probed:,}" if s.probed else "—"
            print(f"  {s.key:<20}{n:>18}  {s.label}{mark}")
        print(f"\n  core = {', '.join(s.key for s in S.core())}")
        print("  python research/fetch.py --core        start here\n")
        return 0

    if a.only:
        picked = [S.BY_KEY[k] for k in a.only if k in S.BY_KEY]
        missing = [k for k in a.only if k not in S.BY_KEY]
        if missing:
            print(f"  unknown: {', '.join(missing)}"); return 2
    else:
        picked = S.core() if a.core else S.ALL

    print(f"\n  pulling {len(picked)} source(s) -> {DB.name}\n")
    total = 0
    for s in picked:
        got = already(s.key)
        if got and not a.force:
            print(f"  = {s.label:<40} have {got[0]:,} from {got[1]} (--force to refetch)")
            total += got[0]
            continue
        print(f"  > {s.label}")
        t0 = time.time()
        RAW.mkdir(parents=True, exist_ok=True)
        # --where / --cap are quick trials: one plain query, not month windows.
        windowed = bool(s.window_by) and not (a.where or a.cap)
        tmp = RAW / (f"{s.key}.parts" if windowed else f"{s.key}.jsonl")
        try:
            if s.kind == "acs":
                n = rows_to_file(pull_acs(), tmp)
            elif s.kind == "cbp":
                n = rows_to_file(pull_cbp(), tmp)
            elif windowed:
                n = pull_windows(s, tmp)
            else:
                # Socrata STREAMS to disk — never hold a whole dataset in RAM.
                n = pull_to_file(s, tmp, where_override=a.where if a.only else None,
                                 cap=a.cap)
        except Exception as e:  # noqa: BLE001 — one dead source must not stop the run
            print(f"\n    FAILED: {type(e).__name__}: {str(e)[:160]}\n")
            if tmp.is_file():   # a windowed pull keeps its finished months for the rerun
                tmp.unlink()
            continue
        if not n:
            print("    nothing returned\n"); _discard(tmp); continue
        p = save(s.key, tmp, n)
        total += n
        print(f"\n    {n:,} rows · {p.stat().st_size/1e6:.1f} MB · {time.time()-t0:.0f}s"
              f" -> table \"{s.key}\"\n")
    print(f"  done — {total:,} rows in {DB}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
