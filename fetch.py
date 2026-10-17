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

RAW = HERE / "raw"
DB = HERE / "queens.duckdb"
PAGE = 50_000                      # Socrata's practical ceiling per request
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


def pull(src: S.Source, where_override: str | None = None, cap: int | None = None) -> list[dict]:
    """Page through one Socrata dataset. Ordered, so paging is stable."""
    base = f"https://{src.domain}/resource/{src.dataset}.json"
    where = where_override if where_override is not None else src.where
    rows: list[dict] = []
    offset = 0
    while True:
        params = {"$limit": PAGE, "$offset": offset}
        if where:
            params["$where"] = where
        if src.order_by:
            params["$order"] = src.order_by
        if src.select:
            params["$select"] = src.select
        if src.group:
            params["$group"] = src.group
        batch = _get(f"{base}?{urllib.parse.urlencode(params)}")
        if not batch:
            break
        rows.extend(batch)
        print(f"      {len(rows):>9,} rows", end="\r", flush=True)
        if len(batch) < PAGE:
            break
        offset += PAGE
        if cap and len(rows) >= cap:
            print(f"\n      stopped at --cap {cap:,}")
            break
    return rows


def save(key: str, rows: list[dict]) -> Path:
    """NDJSON -> DuckDB -> Parquet, using DuckDB for all of it.

    pyarrow is deliberately NOT a dependency: the copy in this anaconda env is
    compiled against NumPy 1.x and fails to import under NumPy 2.x. DuckDB
    reads JSON and writes Parquet on its own, so the whole problem disappears.
    """
    import duckdb

    RAW.mkdir(parents=True, exist_ok=True)
    tmp = RAW / f"{key}.jsonl"
    with tmp.open("w", encoding="utf-8") as fh:
        for r in rows:
            # nested fields -> JSON text, so the schema stays flat and stable
            fh.write(json.dumps({k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                                 for k, v in r.items()}) + "\n")

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
                [key, len(rows), datetime.now(timezone.utc), path.stat().st_size])
    con.close()
    tmp.unlink(missing_ok=True)
    return path


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
    a = ap.parse_args()

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
        try:
            if s.kind == "acs":
                rows = pull_acs()
            elif s.kind == "cbp":
                rows = pull_cbp()
            else:
                rows = pull(s, where_override=a.where if a.only else None, cap=a.cap)
        except Exception as e:  # noqa: BLE001 — one dead source must not stop the run
            print(f"\n    FAILED: {type(e).__name__}: {str(e)[:160]}\n")
            continue
        if not rows:
            print("    nothing returned\n"); continue
        p = save(s.key, rows)
        total += len(rows)
        print(f"\n    {len(rows):,} rows · {p.stat().st_size/1e6:.1f} MB · {time.time()-t0:.0f}s"
              f" -> table \"{s.key}\"\n")
    print(f"  done — {total:,} rows in {DB}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
