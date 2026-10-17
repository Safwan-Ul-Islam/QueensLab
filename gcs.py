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
from pathlib import Path

HERE = Path(__file__).resolve().parent
PREFIX = "raw"


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


def bucket() -> str | None:
    return os.environ.get("GCS_BUCKET") or None


def enabled() -> bool:
    return bool(bucket() and os.environ.get("GCS_HMAC_KEY_ID")
                and os.environ.get("GCS_HMAC_SECRET"))


def url(name: str) -> str:
    return f"gs://{bucket()}/{PREFIX}/{name}"


def attach(con) -> None:
    """Give a DuckDB connection read/write access to the bucket."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE OR REPLACE SECRET queenslab_gcs (TYPE gcs, KEY_ID ?, SECRET ?)",
        [os.environ["GCS_HMAC_KEY_ID"], os.environ["GCS_HMAC_SECRET"]])


def push_table(con, key: str) -> str:
    """Write one DuckDB table to the bucket as Parquet. Returns its gs:// URL."""
    dest = url(f"{key}.parquet")
    con.execute(f'COPY "{key}" TO \'{dest}\' (FORMAT PARQUET, COMPRESSION ZSTD)')
    return dest


def push_log(con) -> None:
    """The fetch log rides along as Parquet, so a rebuild keeps fetch dates.
    (A checkout's mtimes, or an object's upload time, say nothing about when
    the data was actually pulled from the city.)"""
    con.execute(f"COPY _fetch_log TO '{url('_fetch_log.parquet')}' (FORMAT PARQUET)")


def list_tables(con) -> list[str]:
    rows = con.execute("SELECT file FROM glob(?)", [url("*.parquet")]).fetchall()
    names = [Path(r[0]).stem for r in rows]
    return sorted(n for n in names if not n.startswith("_"))
