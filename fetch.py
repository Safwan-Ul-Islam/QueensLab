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
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import sources as S  # noqa: E402
import gcs  # noqa: E402

RAW = HERE / "raw"
DB = HERE / "queens.duckdb"
PAGE = 20_000                      # small enough for Render's 512MB free plan
UA = {"User-Agent": "BizGenome-research/1.0 (analysis only; not served)"}


def _get(url: str, timeout: int = 120) -> list[dict]:
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            if attempt == 3:
                raise
            wait = 2 ** attempt
            print(f"      retry {attempt+1}/3 in {wait}s ({type(e).__name__})", flush=True)
            time.sleep(wait)
    return []


def pull_to_file(src: S.Source, path: Path, where_override: str | None = None,
                 cap: int | None = None) -> int:
    """Page through one dataset, writing each page straight to NDJSON.

    ⚠️ Earlier this accumulated every row in a list and only then wrote it.
    That was fine on a laptop and got OOM-killed on Render's 512MB free plan
    partway through PLUTO (324k rows) — the service restarted and the fetch
    vanished with no error. Peak memory is now one page, not one dataset.
    """
    base = f"https://{src.domain}/resource/{src.dataset}.json"
    where = where_override if where_override is not None else src.where
    total, offset = 0, 0
    with path.open("w", encoding="utf-8") as fh:
        while True:
            params = {"$limit": PAGE, "$offset": offset}
            if where:
                params["$where"] = where
            if src.order_by:
                params["$order"] = src.order_by
            batch = _get(f"{base}?{urllib.parse.urlencode(params)}")
            if not batch:
                break
            got = len(batch)
            for r in batch:
                fh.write(json.dumps({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                     for k, v in r.items()}) + "\n")
            total += got
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
    import duckdb

    path = RAW / f"{key}.parquet"
    con = duckdb.connect(str(DB))
    con.execute(
        f'CREATE OR REPLACE TABLE "{key}" AS '
        "SELECT * FROM read_json_auto(?, union_by_name=true, maximum_object_size=20000000)",
        [str(tmp)])
    con.execute(f'COPY "{key}" TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [str(path)])
    con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                   (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
    con.execute("DELETE FROM _fetch_log WHERE key = ?", [key])
    con.execute("INSERT INTO _fetch_log VALUES (?,?,?,?)",
                [key, n_rows, datetime.now(timezone.utc), path.stat().st_size])
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
    tmp.unlink(missing_ok=True)
    _manifest_put(key, n_rows)
    return path


MANIFEST = RAW / "_manifest.json"


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
                    [key, n, (m.get(key) or {}).get("fetched_at"), f.stat().st_size])
        total += n
    con.close()
    return total


def rebuild_from_gcs(db: Path = DB) -> int:
    """Recreate the database from the bucket. Raises if the bucket is unusable,
    so the caller can fall back to the Parquet committed in raw/."""
    import duckdb
    con = duckdb.connect(str(db))
    try:
        gcs.attach(con)
        tables = gcs.list_tables(con)
        if not tables:
            raise RuntimeError(f"no tables under gs://{gcs.bucket()}/{gcs.PREFIX}/")
        total = 0
        for key in tables:
            con.execute(f'CREATE OR REPLACE TABLE "{key}" AS SELECT * FROM read_parquet(?)',
                        [gcs.url(f"{key}.parquet")])
            total += con.execute(f'SELECT count(*) FROM "{key}"').fetchone()[0]
        try:
            con.execute("CREATE OR REPLACE TABLE _fetch_log AS SELECT * FROM read_parquet(?)",
                        [gcs.url("_fetch_log.parquet")])
        except Exception:  # noqa: BLE001 — tables without dates beat no tables
            con.execute("""CREATE TABLE IF NOT EXISTS _fetch_log
                           (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)""")
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


def already(key: str) -> tuple[int, str] | None:
    import duckdb
    if not DB.exists():
        return None
    con = duckdb.connect(str(DB))
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
        tmp = RAW / f"{s.key}.jsonl"
        try:
            if s.kind == "acs":
                n = rows_to_file(pull_acs(), tmp)
            elif s.kind == "cbp":
                n = rows_to_file(pull_cbp(), tmp)
            else:
                # Socrata STREAMS to disk — never hold a whole dataset in RAM.
                n = pull_to_file(s, tmp, where_override=a.where if a.only else None,
                                 cap=a.cap)
        except Exception as e:  # noqa: BLE001 — one dead source must not stop the run
            print(f"\n    FAILED: {type(e).__name__}: {str(e)[:160]}\n")
            tmp.unlink(missing_ok=True)
            continue
        if not n:
            print("    nothing returned\n"); tmp.unlink(missing_ok=True); continue
        p = save(s.key, tmp, n)
        total += n
        print(f"\n    {n:,} rows · {p.stat().st_size/1e6:.1f} MB · {time.time()-t0:.0f}s"
              f" -> table \"{s.key}\"\n")
    print(f"  done — {total:,} rows in {DB}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
