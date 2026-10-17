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
import signal
import sys
import tempfile
import threading
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
import jobs  # noqa: E402

RAW = HERE / "raw"
DERIVED = HERE / "derived"      # built from the sources (nta_profile.py), not pulled
DB = HERE / "queens.duckdb"
PAGE = 20_000                      # small enough for Render's 512MB free plan
WORKERS = 3                        # months pulled at once by pull_windows (laptop only)
UA = {"User-Agent": "BizGenome-research/1.0 (analysis only; not served)"}


_retries = 0
_retries_lock = threading.Lock()
# Set when a run is being stopped: month-window threads quit at their next page
# instead of finishing every remaining month first.
_stop = threading.Event()
_current = None            # the job this process is working on, for live retry counts


def _count_retry() -> None:
    global _retries
    with _retries_lock:
        _retries += 1
    if _current is not None:
        try:
            _current.retried()
        except Exception:  # noqa: BLE001 — a missed count must not break a pull
            pass


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
        _count_retry()
        print(f"      retry {attempt+1}/5 in {wait}s ({reason})", flush=True)
        time.sleep(wait)
    return []


def _order(src: S.Source) -> str:
    """A TOTAL order, so offset paging never repeats or skips a row.

    ⚠️ Ordering by a column with ties (camis, borough_block_lot) let tied rows
    come back in a different order on the next page request, so some arrived
    twice and as many never arrived (the total still matched): 2,682 in
    storefront_vacancy's raw pull, 15 in dohmh_food (found 2026-10-17). The
    city's files also hold genuinely identical rows — 15,865 and 59 — and
    those are kept. `:id` breaks every tie; an aggregate is unique on its group."""
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
           quiet: bool = False, job=None) -> int:
    """Offset-page one query into an open NDJSON file. Peak memory is one page."""
    base = f"https://{src.domain}/resource/{src.dataset}.json"
    total, offset = 0, 0
    while True:
        if _stop.is_set():
            raise RuntimeError("stopped — the run is being interrupted")
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
        if job and got:
            job.add_rows(got)
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
                 cap: int | None = None, job=None) -> int:
    """Page through one dataset, writing each page straight to NDJSON.

    ⚠️ Earlier this accumulated every row in a list and only then wrote it.
    That was fine on a laptop and got OOM-killed on Render's 512MB free plan
    partway through PLUTO (324k rows) — the service restarted and the fetch
    vanished with no error. Peak memory is now one page, not one dataset.
    """
    with path.open("w", encoding="utf-8") as fh:
        return _pages(src, _where(src, where_override), fh, cap, job=job)


def _months(since: str):
    """[start, end) calendar-month windows from `since` up to now, UTC."""
    start, now = datetime.fromisoformat(since), _utc_now()
    while start < now:
        end = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield start, end
        start = end


def pull_windows(src: S.Source, parts: Path, job=None) -> int:
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
                kept = sum(1 for _ in fh)
            if job:
                job.add_rows(kept)
            return start, kept, None, True
        bound = (f"{src.window_by} >= '{start:%Y-%m-%dT%H:%M:%S}' AND "
                 f"{src.window_by} < '{end:%Y-%m-%dT%H:%M:%S}'")
        where = f"({src.where}) AND {bound}" if src.where else bound
        tmp = out.with_suffix(".partial")
        with tmp.open("w", encoding="utf-8") as fh:
            n = _pages(src, where, fh, quiet=True, job=job)
        tmp.replace(out)
        api = None
        if not src.group:   # completeness check; an aggregate has no cheap count
            q = urllib.parse.urlencode({"$select": "count(*) AS n", "$where": where})
            api = int(_get(f"{base}?{q}")[0]["n"])
        return start, n, api, False

    total = 0
    # Not a `with` block: on Ctrl-C its exit would wait for EVERY queued month.
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        for start, n, api, kept in pool.map(one, wins):
            total += n
            if job and api not in (None, n):
                job.warn(f"{start:%Y-%m}: pulled {n:,}, the API counts {api:,}")
            flag = ("  (kept from the last run)" if kept else
                    "" if api in (None, n) else f"  ⚠ api says {api:,}")
            print(f"      {start:%Y-%m}{n:>10,} rows{flag}   total {total:,}", flush=True)
    except BaseException:
        _stop.set()                    # running months stop at their next page
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown()
    return total


def rows_to_file(rows: list[dict], path: Path) -> int:
    """Dump an in-memory list to NDJSON. Only for the small sources (acs, cbp)
    that arrive whole; Socrata streams straight to disk instead."""
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                 for k, v in r.items()}) + "\n")
    return len(rows)


def save(key: str, tmp: Path, n_rows: int, job=None) -> Path:
    """NDJSON on disk -> DuckDB table -> Parquet -> the bucket.

    pyarrow is deliberately NOT a dependency: the copy in the user's anaconda
    env is compiled against NumPy 1.x and fails under NumPy 2.x. DuckDB reads
    JSON and writes Parquet itself, so that whole class of breakage is gone.

    The database's write lock is held for the local load ONLY. It used to be
    held through the upload too, and every page request in that window found
    the database locked and showed nothing.
    """
    path = RAW / f"{key}.parquet"
    if job:
        job.step("saving")
    fetched = _utc_now()
    con = _connect_rw()
    try:
        # sample_size=-1 reads every row to learn the columns. Socrata leaves a
        # field OUT of a row when it is empty, and DuckDB's default looks at only
        # the first 20,480 rows, so a column first seen later is never created.
        # In PLUTO that was zonedist4, set on just 4 Queens lots (found
        # 2026-10-17). Its other 14 absent columns are empty for every Queens
        # lot, so the API never sends them; no scan can bring those back.
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
                    [key, n_rows, fetched, path.stat().st_size])
    finally:
        con.close()                    # the lock goes before any network traffic
    _discard(tmp)
    _manifest_put(key, n_rows)
    if not gcs.enabled():
        if job:
            job.upload("off", "no bucket configured")
        return path
    if job:
        job.step("uploading")
    try:
        dest = gcs.upload(path, f"{key}.parquet")
        gcs.put_log([(key, n_rows, fetched, path.stat().st_size)])
        print(f"\n    -> {dest}", end="")
        if job:
            job.upload("uploaded", dest)
    except Exception as e:  # noqa: BLE001 — the local copy is already safe
        if _interrupted(e):
            raise
        print(f"\n    ⚠ bucket upload failed, local copy kept: {type(e).__name__}: {str(e)[:120]}",
              end="")
        if job:
            job.upload("failed", f"{type(e).__name__}: {e}")
    return path


def _discard(tmp: Path) -> None:
    if tmp.is_dir():
        shutil.rmtree(tmp)
    else:
        tmp.unlink(missing_ok=True)


def _connect(read_only: bool = False):
    """Open the database, waiting out another process's write lock.

    DuckDB allows one writer per file. With a big source pulling in one shell
    and the rest in another, the second save now waits its turn (up to 20 min)
    instead of crashing after an hour of downloading."""
    import duckdb
    for _ in range(240):
        try:
            return duckdb.connect(str(DB), read_only=read_only)
        except duckdb.IOException as e:
            if "lock" not in str(e).lower():
                raise
            time.sleep(5)
    return duckdb.connect(str(DB), read_only=read_only)


def _connect_rw():
    return _connect(read_only=False)


def _q(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


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

    Since 2026-10-17 this is the FALLBACK, used only if the bucket cannot be
    read. The small tables' Parquet is in the repo; the five big ones are
    gitignored and live only in the bucket, so a fallback boot has no 311,
    permits, PLUTO or subway — and the page says where its data came from.
    """
    import duckdb
    m = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    con = duckdb.connect(str(db))
    con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                   (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
    for f in sorted(DERIVED.glob("*.parquet")):    # the neighbourhood profile
        con.execute(f'CREATE OR REPLACE TABLE "{f.stem}" AS SELECT * FROM read_parquet(?)', [str(f)])
    total = 0
    for f in sorted(RAW.glob("*.parquet")):
        key = f.stem
        if key.startswith(("_", "tmp_")):         # a crash mid-COPY leaves tmp_<key>.parquet
            continue
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
        try:
            log = gcs.read_log(con)
        except Exception:  # noqa: BLE001 — tables without dates beat no tables
            log = {}
        con.execute("""CREATE OR REPLACE TABLE _fetch_log
                       (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
        total = 0
        for key in tables:
            src = gcs.url(f"{key}.parquet")
            # Size from the file's OWN footer (one small read), not the log: a
            # lost log entry once meant "unknown size", which meant copying a
            # 1.9M-row table into a 512 MB server on every wake-up.
            rows = con.execute("SELECT sum(num_rows) FROM parquet_file_metadata(?)",
                               [src]).fetchone()[0] or 0
            _, fetched_at, nbytes = log.get(key, (rows, None, None))
            con.execute("INSERT INTO _fetch_log VALUES (?,?,?,?)", [key, rows, fetched_at, nbytes])
            if rows > LAZY_ROWS or (nbytes or 0) > LAZY_BYTES:
                lit = src.replace("'", "''")
                con.execute(f'CREATE OR REPLACE VIEW "{key}" AS '
                            f"SELECT * FROM read_parquet('{lit}')")
            else:
                con.execute(f'CREATE OR REPLACE TABLE "{key}" AS SELECT * FROM read_parquet(?)',
                            [src])
            total += con.execute(f'SELECT count(*) FROM "{key}"').fetchone()[0]
        # Derived tables (the neighbourhood profile) are small: always copied.
        try:
            for key in gcs.list_tables(con, prefix=gcs.DERIVED):
                con.execute(f'CREATE OR REPLACE TABLE "{key}" AS SELECT * FROM read_parquet(?)',
                            [gcs.url(f"{key}.parquet", gcs.DERIVED)])
        except Exception as e:  # noqa: BLE001 — sources without the profile beat nothing
            if not gcs.not_found(e):
                print(f"[queenslab] derived tables not loaded: {type(e).__name__}", flush=True)
        return total
    finally:
        con.close()


def push_all() -> int:
    """Upload every table already pulled to the bucket — one job per table, so
    the collection table shows exactly which ones made it."""
    if not gcs.enabled():
        print("  GCS not configured — set GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET in .env")
        return 1
    scratch = Path(tempfile.mkdtemp(prefix="queenslab-push-"))
    con = _connect(read_only=True)
    try:
        gcs.attach(con)       # a database rebuilt from the bucket holds views over gs://
        log = con.execute("SELECT key, rows, fetched_at, bytes FROM _fetch_log ORDER BY rows").fetchall()
        # The database is the truth. A Parquet file in raw/ is uploaded only if
        # its own row count matches the table's; otherwise (a stale checkout, a
        # half-written file, none at all) the table is exported fresh first.
        for key, rows, *_ in log:
            f = RAW / f"{key}.parquet"
            ok = False
            if f.exists():
                try:
                    ok = con.execute("SELECT sum(num_rows) FROM parquet_file_metadata(?)",
                                     [str(f)]).fetchone()[0] == rows
                except Exception:  # noqa: BLE001 — unreadable file: export instead
                    ok = False
            if not ok:
                con.execute(f'COPY "{key}" TO {_q(scratch / f"{key}.parquet")} '
                            "(FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()
    jobs.reap_dead()
    run = jobs.new_run()
    ids = {key: jobs.queue(key, run, "upload") for key, *_ in log}
    jobs.mirror(force=True)
    jobs.heartbeat_forever()
    signal.signal(signal.SIGTERM, _on_term)
    failed = 0
    try:
        for key, rows, fetched_at, nbytes in log:
            job = jobs.Job(ids[key]).start(step="uploading")
            local = scratch / f"{key}.parquet"
            local = local if local.exists() else RAW / f"{key}.parquet"
            t0 = time.time()
            try:
                dest = gcs.upload(local, f"{key}.parquet")
                gcs.put_log([(key, rows, fetched_at, nbytes)])
            except Exception as e:  # noqa: BLE001 — report it, try the next table
                if _interrupted(e):
                    raise KeyboardInterrupt from e
                failed += 1
                job.upload("failed", f"{type(e).__name__}: {e}")
                job.fail(f"upload failed — {type(e).__name__}: {e}")
                print(f"  {key:<20} FAILED: {type(e).__name__}: {str(e)[:120]}")
                continue
            job.upload("uploaded", dest)
            job.succeed(rows)
            print(f"  {key:<20}{rows:>10,} rows -> {dest}  ({time.time()-t0:.1f}s)")
    except (KeyboardInterrupt, SystemExit):
        jobs.close_run(run, "interrupted before this upload finished")
        jobs.mirror(force=True)
        raise
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    con = gcs._scratch()
    try:
        print(f"\n  bucket now holds {len(gcs.list_tables(con))} tables"
              + (f" · {failed} upload(s) FAILED" if failed else ""))
    finally:
        con.close()
    return 1 if failed else 0


def _interrupted(e: BaseException) -> bool:
    """Was this exception really Ctrl-C or SIGTERM? Inside a DuckDB call an
    interrupt surfaces as InterruptException or as RuntimeError('Query
    interrupted') caused by the KeyboardInterrupt/SystemExit. Treated as an
    ordinary failure, the run carried on and then hung forever on the next
    duckdb.connect (review, 2026-10-17)."""
    if isinstance(e, (KeyboardInterrupt, SystemExit)) or type(e).__name__ == "InterruptException":
        return True
    if isinstance(e.__cause__ or e.__context__, (KeyboardInterrupt, SystemExit)):
        return True
    return "query interrupted" in str(e).lower()


def _on_term(signum, frame):
    """SIGTERM ends a run the same way Ctrl-C does: recorded, not silent."""
    raise SystemExit(128 + signum)


def _note_retries(job, before: int) -> None:
    n = _retries - before
    if n:
        job.warn(f"{n} request{'s' if n > 1 else ''} retried — the source was busy")


def print_jobs() -> int:
    """The collection table, in the terminal."""
    rows = jobs.listing(40)
    if not rows:
        print("\n  no collection jobs yet\n")
        return 0

    def t(iso):
        return (iso or "")[5:16].replace("T", " ")
    print(f"\n  {'source':<20}{'state':<22}{'rows':>11}  {'started':<12}{'last activity':<14}"
          f"{'finished':<12}{'bucket':<9} where")
    print("  " + "-" * 112)
    for j in rows:
        state = j["state"] + (f" · {j['step']}" if j.get("step") and j["state"] == "running" else "")
        print(f"  {j['source']:<20}{state:<22}{j.get('rows_downloaded') or 0:>11,}  "
              f"{t(j.get('started')):<12}{t(j.get('last_activity')):<14}{t(j.get('finished')):<12}"
              f"{(j.get('gcs') or ''):<9} {j.get('host')} · {j.get('origin')}")
        for label, k in (("↳", "error"), ("⚠", "warnings")):
            if j.get(k):
                print(f"  {'':<20}{label} {j[k][:150]}")
    print(f"\n  times are UTC · this machine is {jobs.host()}\n")
    return 0


OVERTURE = "s3://overturemaps-us-west-2/release"
QUEENS_BBOX = (-73.9626, 40.5414, -73.7004, 40.8007)   # clips some Brooklyn/Nassau; polygons decide


def pull_overture(path: Path, job=None) -> int:
    """Overture Maps places for Queens -> NDJSON at `path`.

    Overture publishes GeoParquet monthly on a public S3 bucket; DuckDB reads
    only the row groups whose bounding box touches Queens (~160k places, ~40 s),
    then the 2020 NTA polygons cut it to Queens exactly. Needs `ntas` pulled."""
    import duckdb
    c = duckdb.connect()
    try:
        c.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial; SET s3_region='us-west-2'")
        c.execute(f"ATTACH {_q(DB)} AS q (READ_ONLY)")
        release = c.execute(
            "SELECT max(regexp_extract(file, 'release/([^/]+)/', 1)) "
            f"FROM glob('{OVERTURE}/*/theme=places/type=place/*.parquet')").fetchone()[0]
        x0, y0, x1, y1 = QUEENS_BBOX
        c.execute(f"""CREATE TEMP TABLE raw AS SELECT * FROM read_parquet(
                        '{OVERTURE}/{release}/theme=places/type=place/*.parquet', hive_partitioning=0)
                      WHERE bbox.xmin BETWEEN {x0} AND {x1} AND bbox.ymin BETWEEN {y0} AND {y1}""")
        c.execute("CREATE TEMP TABLE nta AS SELECT nta2020, ntaname, ST_GeomFromGeoJSON(the_geom) g FROM q.ntas")
        c.execute(f"""COPY (
            SELECT r.id, r.names.primary AS name, r.basic_category AS category,
                   r.taxonomy.primary AS category_detail, r.operating_status, r.confidence,
                   r.websites[1] AS website, r.phones[1] AS phone, r.brand.names.primary AS brand,
                   r.addresses[1].freeform AS address, r.addresses[1].postcode AS postcode,
                   ST_X(r.geometry) AS longitude, ST_Y(r.geometry) AS latitude,
                   n.nta2020, n.ntaname,
                   list_distinct([s.dataset FOR s IN r.sources]) AS contributors,
                   CASE WHEN list_contains([s.dataset FOR s IN r.sources], 'Foursquare')
                        THEN 'Apache-2.0 (Foursquare: keep NOTICE)'
                        WHEN list_contains([s.dataset FOR s IN r.sources], 'AllThePlaces')
                        THEN 'CC0-1.0 / CDLA-Permissive-2.0' ELSE 'CDLA-Permissive-2.0' END AS licence,
                   '{release}' AS overture_release
            FROM raw r JOIN nta n ON ST_Within(r.geometry, n.g)
            QUALIFY row_number() OVER (PARTITION BY r.id ORDER BY n.nta2020) = 1
        ) TO {_q(path)} (FORMAT JSON)""")
        n = c.execute(f"SELECT count(*) FROM read_json_auto({_q(path)})").fetchone()[0]
        if job:
            job.add_rows(n)
            job.warn(f"Overture release {release}")
        return n
    finally:
        c.close()


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
    # Spooled to a temp file and read line by line: holding the archive AND
    # its decoded text took ~600 MB, more than the free server has (review,
    # 2026-10-17). The source is also tagged laptop-only now.
    with tempfile.TemporaryFile() as blob:
        with urllib.request.urlopen(req, timeout=180) as resp:
            shutil.copyfileobj(resp, blob)
        blob.seek(0)
        with zipfile.ZipFile(blob) as archive:
            name = next(n for n in archive.namelist() if n.endswith(".txt"))
            with archive.open(name) as raw:
                return _cbp_rows(io.TextIOWrapper(raw, encoding="latin1", newline=""))


def _cbp_rows(text) -> list[dict]:
    rows = []
    for rec in csv.DictReader(text):
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
    ap.add_argument("--cap", type=int,
                    help="TRIAL: stop each source after N rows; saved as <key>__trial, never uploaded")
    ap.add_argument("--where",
                    help="TRIAL: override the WHERE for --only; saved as <key>__trial, never uploaded")
    ap.add_argument("--push", action="store_true",
                    help="upload every local table to the GCS bucket, then exit")
    ap.add_argument("--jobs", action="store_true",
                    help="show recent collection jobs (all machines), then exit")
    ap.add_argument("--run", help=argparse.SUPPRESS)   # set by the web app: claim its queued jobs
    a = ap.parse_args()

    if a.jobs:
        return print_jobs()
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
        print("  python fetch.py --core        start here\n")
        return 0

    if a.only:
        picked = [S.BY_KEY[k] for k in a.only if k in S.BY_KEY]
        missing = [k for k in a.only if k not in S.BY_KEY]
        if missing:
            print(f"  unknown: {', '.join(missing)}"); return 2
    else:
        picked = S.core() if a.core else S.ALL

    print(f"\n  pulling {len(picked)} source(s) -> {DB.name}\n")
    jobs.reap_dead()                   # close anything an earlier run left hanging
    total, todo = 0, []
    for s in picked:
        got = already(s.key)
        if got and not a.force:
            print(f"  = {s.label:<40} have {got[0]:,} from {got[1]} (--force to refetch)")
            total += got[0]
            continue
        todo.append(s)
    # Every source of the run is queued up front, so the page shows the queue.
    run = a.run or jobs.new_run()
    origin = "trial" if (a.where or a.cap) else "web" if a.run else "cli"
    ids = {s.key: jobs.claim(s.key, run, origin) for s in todo}
    if todo:
        jobs.mirror(force=True)
        jobs.heartbeat_forever()
    signal.signal(signal.SIGTERM, _on_term)
    global _current
    failed = 0
    trial = bool(a.where or a.cap)
    if trial:
        print("  TRIAL (--where/--cap): each source is saved as <key>__trial in the local\n"
              "  database only. The real table, its Parquet and the bucket are untouched.\n")
    for s in todo:
        job = jobs.Job(ids[s.key])
        before = _retries
        _stop.clear()          # a previous source's failure must not stop this one
        print(f"  > {s.label}")
        t0 = time.time()
        RAW.mkdir(parents=True, exist_ok=True)
        # --where / --cap are quick trials: one plain query, not month windows.
        windowed = bool(s.window_by) and not trial
        tmp = RAW / (f"{s.key}.parts" if windowed else f"{s.key}.jsonl")
        try:
            job.start()
            _current = job
            if s.kind == "acs":
                n = rows_to_file(pull_acs(), tmp)
                job.add_rows(n)
            elif s.kind == "cbp":
                n = rows_to_file(pull_cbp(), tmp)
                job.add_rows(n)
            elif s.kind == "overture":
                n = pull_overture(tmp, job)
            elif windowed:
                n = pull_windows(s, tmp, job=job)
            else:
                # Socrata STREAMS to disk — never hold a whole dataset in RAM.
                n = pull_to_file(s, tmp, where_override=a.where if a.only else None,
                                 cap=a.cap, job=job)
            if not n:
                _discard(tmp)
                raise RuntimeError("the source returned no rows")
            if trial:
                p = save_trial(s.key, tmp, job)
            else:
                p = save(s.key, tmp, n, job=job)
        except (KeyboardInterrupt, SystemExit) as e:
            _note_retries(job, before)
            job.fail("interrupted (Ctrl-C)" if isinstance(e, KeyboardInterrupt)
                     else f"stopped (exit {e.code})")
            jobs.cancel_run(run, "cancelled — the run was stopped before this started")
            jobs.mirror(force=True)
            if tmp.is_file():
                tmp.unlink()
            raise
        except Exception as e:  # noqa: BLE001 — one dead source must not stop the run
            if _interrupted(e):    # Ctrl-C/SIGTERM that surfaced through DuckDB: END the run
                job.fail("interrupted (Ctrl-C or stop signal)")
                jobs.cancel_run(run, "cancelled — the run was stopped before this started")
                jobs.mirror(force=True)
                raise KeyboardInterrupt from e
            print(f"\n    FAILED: {type(e).__name__}: {str(e)[:160]}\n")
            if tmp.is_file():   # a windowed pull keeps its finished months for the rerun
                tmp.unlink()
            _note_retries(job, before)
            job.fail(f"{type(e).__name__}: {e}")
            failed += 1
            continue
        finally:
            _current = None
        _note_retries(job, before)
        job.succeed(n)
        total += n
        size = f"{p.stat().st_size/1e6:.1f} MB · " if isinstance(p, Path) else ""
        print(f"\n    {n:,} rows · {size}{time.time()-t0:.0f}s"
              f" -> table \"{p if isinstance(p, str) else s.key}\"\n")
    print(f"  done — {total:,} rows in {DB}"
          + (f" · {failed} source(s) FAILED — see python fetch.py --jobs" if failed else "") + "\n")
    return 1 if failed else 0


def save_trial(key: str, tmp: Path, job) -> str:
    """A --where/--cap trial: loaded as <key>__trial for a look in DuckDB, and
    nothing else. It used to REPLACE the real table, locally and in the
    bucket, and be recorded as a normal successful pull (review, 2026-10-17)."""
    name = f"{key}__trial"
    job.step("saving")
    con = _connect_rw()
    try:
        con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM read_json_auto(?, '
                    "union_by_name=true, sample_size=-1, maximum_object_size=20000000)",
                    [str(tmp / "*.jsonl") if tmp.is_dir() else str(tmp)])
    finally:
        con.close()
    _discard(tmp)
    job.upload("off", f"trial only: kept locally as table {name}")
    job.warn(f"trial — the real table was not touched; this is table {name}")
    return name


if __name__ == "__main__":
    raise SystemExit(main())
