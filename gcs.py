"""Google Cloud Storage, through DuckDB alone.

The bucket is the Bronze archive (BizGenome log, 2026-10-12: "GCS is the next
bronze archive"). Every Parquet file fetch.py writes is pushed here, and the
app rebuilds its database from here on boot — so data fetched on the live
service finally survives a restart.

Auth is an HMAC key (Cloud Storage → Settings → Interoperability) for a service
account scoped to THIS bucket only. DuckDB's httpfs speaks GCS natively with
it, so there is no google-cloud SDK, no `gcloud` login (which is interactive
and blocked in the build environment), and no new dependency.

Configured by three environment variables — locally from QueensLab/.env
(gitignored), on Render from the Environment tab:

    GCS_BUCKET          queenslab-research-yourteam-2026
    GCS_HMAC_KEY_ID     GOOG1E…
    GCS_HMAC_SECRET     …

If any is missing, everything falls back to the Parquet committed in raw/.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load_dotenv() -> None:
    """Tiny .env reader — not worth a dependency. Never overrides real env."""
    f = HERE / ".env"
    if not f.exists():
        return
    for line in f.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

# Where the tables live in the bucket. Overridable so a test run can write to
# a scratch prefix instead of over the real data.
PREFIX = os.environ.get("GCS_PREFIX") or "raw"
# Tables BUILT from the sources (the neighbourhood profile), kept apart from
# what was pulled.
DERIVED = os.environ.get("GCS_DERIVED_PREFIX") or "derived"


def bucket() -> str | None:
    return os.environ.get("GCS_BUCKET") or None


def enabled() -> bool:
    return bool(bucket() and os.environ.get("GCS_HMAC_KEY_ID")
                and os.environ.get("GCS_HMAC_SECRET"))


def url(name: str, prefix: str | None = None) -> str:
    return f"gs://{bucket()}/{prefix or PREFIX}/{name}"


def attach(con, timeout_s: int | None = None) -> None:
    """Give a DuckDB connection read/write access to the bucket.

    timeout_s: by default DuckDB waits 30 s per request and retries 3 times —
    about two minutes before a dead bucket is reported. Anything a page or a
    boot waits on passes a short timeout instead."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if timeout_s:
        con.execute(f"SET http_timeout={int(timeout_s)}")
        con.execute("SET http_retries=1")
    con.execute(
        "CREATE OR REPLACE SECRET queenslab_gcs (TYPE gcs, KEY_ID ?, SECRET ?)",
        [os.environ["GCS_HMAC_KEY_ID"], os.environ["GCS_HMAC_SECRET"]])


def not_found(e) -> bool:
    """Does this error just mean 'that object or glob does not exist'?"""
    return any(m in str(e) for m in ("404", "No files found", "does not exist"))


def _lit(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _scratch(timeout_s: int | None = None):
    """A throwaway in-memory DuckDB with the bucket attached. Uploads go
    through one of these, never through queens.duckdb, so the research
    database is not locked while bytes cross the network.

    Capped: an upload re-encodes the file, and uncapped DuckDB took about
    2.8 GB to re-encode nyc_311 (review, 2026-10-17). Row order is not
    something any table here relies on, so it is not preserved."""
    import duckdb
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET memory_limit='1GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={_lit(Path(tempfile.gettempdir()) / 'queenslab-duckdb')}")
    attach(con, timeout_s)
    return con


def upload(local: Path, name: str, prefix: str | None = None) -> str:
    """Copy a local Parquet file to <prefix>/<name>. Returns its gs:// URL."""
    dest = url(name, prefix)
    con = _scratch()
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet({_lit(local)})) TO {_lit(dest)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()
    return dest


def put_log(entries: list[tuple]) -> None:
    """Record fetch dates for these tables: (key, rows, fetched_at as naive
    UTC, bytes). The log rides along so a rebuild keeps fetch dates (an
    object's upload time says nothing about when the city's data was pulled).

    Each table gets its OWN object, raw/_log/<key>.parquet, written without
    reading anything first. One shared file meant read-merge-write, and two
    machines uploading at once could drop each other's entries (review,
    2026-10-17). The combined raw/_fetch_log.parquet is still rewritten, best
    effort, for code that reads only that."""
    con = _scratch()
    try:
        for key, rows, fetched_at, nbytes in entries:
            con.execute("CREATE OR REPLACE TEMP TABLE one "
                        "(key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)")
            con.execute("INSERT INTO one VALUES (?,?,?,?)", [key, rows, fetched_at, nbytes])
            con.execute(f"COPY one TO {_lit(url(f'_log/{key}.parquet'))} (FORMAT PARQUET)")
    finally:
        con.close()
    try:
        _merge_combined_log(entries)
    except Exception as e:  # noqa: BLE001 — the per-table objects are the record
        print(f"\n    (combined fetch log not updated: {type(e).__name__}: {str(e)[:100]})", end="")


def read_log(con) -> dict:
    """{key: (rows, fetched_at, bytes)} — per-table objects first, then the
    combined file for anything only it knows (written before the split)."""
    out = {}
    for path in (url("_fetch_log.parquet"), url("_log/*.parquet")):
        try:
            for key, rows, fetched_at, nbytes in con.execute(
                    "SELECT key, rows, fetched_at, bytes FROM read_parquet(?, union_by_name=true)",
                    [path]).fetchall():
                out[key] = (rows, fetched_at, nbytes)    # later source wins
        except Exception as e:  # noqa: BLE001
            if not not_found(e):
                raise
    return out


def _merge_combined_log(entries: list[tuple]) -> None:
    import duckdb
    con = _scratch()
    try:
        con.execute("CREATE TABLE log (key VARCHAR, rows BIGINT, fetched_at TIMESTAMP, bytes BIGINT)")
        try:
            con.execute("INSERT INTO log SELECT key, rows, fetched_at, bytes FROM read_parquet(?)",
                        [url("_fetch_log.parquet")])
        except duckdb.Error as e:
            # Only "there is no log yet" may continue. Anything else, and
            # writing now would replace the whole log with just these rows.
            if not any(m in str(e) for m in ("404", "No files found", "does not exist")):
                raise
        for key, rows, fetched_at, nbytes in entries:
            con.execute("DELETE FROM log WHERE key = ?", [key])
            con.execute("INSERT INTO log VALUES (?,?,?,?)", [key, rows, fetched_at, nbytes])
        con.execute(f"COPY log TO {_lit(url('_fetch_log.parquet'))} (FORMAT PARQUET)")
    finally:
        con.close()


def write_rows(path: str, schema: list[tuple[str, str]], rows: list[list]) -> str:
    """Write rows to gs://<bucket>/<path> as Parquet with a FIXED schema —
    no type guessing, so no timestamp ever passes through a time zone."""
    dest = f"gs://{bucket()}/{path}"
    con = _scratch()
    try:
        con.execute("CREATE TABLE t (" + ", ".join(f'"{n}" {t}' for n, t in schema) + ")")
        if rows:
            con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(schema))})", rows)
        con.execute(f"COPY t TO {_lit(dest)} (FORMAT PARQUET)")
    finally:
        con.close()
    return dest


def read_rows(pattern: str, timeout_s: int | None = None) -> list[dict]:
    """Every row of the Parquet at gs://<bucket>/<pattern> (a glob is fine)."""
    con = _scratch(timeout_s)
    try:
        cur = con.execute("SELECT * FROM read_parquet(?, union_by_name=true)",
                          [f"gs://{bucket()}/{pattern}"])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()


def list_tables(con, prefix: str | None = None) -> list[str]:
    rows = con.execute("SELECT file FROM glob(?)", [url("*.parquet", prefix)]).fetchall()
    names = [Path(r[0]).stem for r in rows]
    return sorted(n for n in names if not n.startswith("_"))
