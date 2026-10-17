#!/usr/bin/env python3
"""Neighbourhood profile: every source side by side, for each of Queens' 82
2020 neighbourhoods (NTAs).

    python nta_profile.py        build from queens.duckdb, save, upload

Writes two small tables, nta_profile and nta_profile_notes, into queens.duckdb
and as Parquet under derived/, and uploads that Parquet to the bucket's
derived/ prefix. The page only SHOWS them. Building them means placing about
a million points inside polygons, which a 512 MB free server should not redo
on every wake-up.

How a record finds its neighbourhood: by its COORDINATES, inside the 2020 NTA
polygons (DuckDB spatial) — never by the NTA codes some sources carry. DCWP
and the old population file use the 2010 scheme and storefront filings
switched schemes in 2022; joining across schemes once matched zero rows.
Property sales have no coordinates, so they borrow PLUTO's for their tax lot,
or failing that (condo units) for their block, which never spans two NTAs.

Math decides: every figure is a count, sum, median or ratio over public
records. Nothing is estimated by a model.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gcs  # noqa: E402
import jobs  # noqa: E402

DB = HERE / "queens.duckdb"
OUT = HERE / "derived"
NEEDS = ["ntas", "tract_nta", "acs_tracts", "dcwp_licences", "dohmh_food", "nys_retail_food",
         "sla_liquor", "nyc_311", "dob_now", "dob_permits", "subway_ridership",
         "storefront_vacancy", "dof_sales", "pluto"]
MIN_RESIDENTS = 1000   # below this a per-resident figure means nothing (parks, airports)
KIND = {"0": "residential", "5": "Rikers Island", "6": "special area", "7": "cemetery",
        "8": "airport", "9": "park"}

# One statement per entry. `q` is queens.duckdb, attached read-only.
SQL = [
    "INSTALL spatial", "LOAD spatial",
    "CREATE TEMP TABLE nta AS SELECT nta2020 AS code, ntaname AS name, ntatype, "
    "ST_GeomFromGeoJSON(the_geom) AS geom FROM q.ntas",

    # Each source's own newest date sets its 12-month window, so a source
    # refreshed later than another is never measured against the wrong year.
    """CREATE TEMP TABLE win AS SELECT * FROM (VALUES
       ('311',     (SELECT max(TRY_CAST(created_date AS TIMESTAMP)) FROM q.nyc_311)),
       ('permits', (SELECT max(TRY_CAST(issued_date AS TIMESTAMP)) FROM q.dob_now)),
       ('bis',     (SELECT max(coalesce(TRY_CAST(left(issuance_date, 10) AS DATE),
                                        TRY_STRPTIME(left(issuance_date, 10), '%m/%d/%Y')::DATE))::TIMESTAMP
                    FROM q.dob_permits)),
       ('dcwp',    (SELECT max(TRY_CAST(license_creation_date AS TIMESTAMP)) FROM q.dcwp_licences)),
       ('dohmh',   (SELECT max(TRY_CAST(inspection_date AS TIMESTAMP)) FROM q.dohmh_food)),
       ('subway',  (SELECT max(TRY_CAST(transit_timestamp AS TIMESTAMP)) FROM q.subway_ridership))
       ) t(src, newest)""",
    "CREATE TEMP MACRO since(s) AS (SELECT newest - INTERVAL 365 DAY FROM win WHERE src = s)",

    # PLUTO lots with coordinates: the bridge for anything known only by tax lot.
    """CREATE TEMP TABLE lots AS
       SELECT left(bbl, 10) AS bbl, left(bbl, 6) AS block, TRY_CAST(longitude AS DOUBLE) x,
              TRY_CAST(latitude AS DOUBLE) y, coalesce(TRY_CAST(retailarea AS DOUBLE), 0) retail_sqft
       FROM q.pluto WHERE TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41""",

    # Restaurants: one row per CAMIS. A pre-permit inspection is what a new
    # restaurant gets before it opens. The first inspection of ANY type is no
    # use: the city keeps only ~3 years of history, so every older restaurant
    # appears to start in 2023-24. 1900-01-01 = not inspected yet.
    """CREATE TEMP TABLE rest AS
       SELECT camis, any_value(TRY_CAST(longitude AS DOUBLE)) x, any_value(TRY_CAST(latitude AS DOUBLE)) y,
              min(TRY_CAST(inspection_date AS TIMESTAMP)) FILTER (
                  WHERE inspection_type ILIKE '%pre-permit%' AND inspection_date NOT LIKE '1900%') prepermit,
              bool_and(inspection_date LIKE '1900%') awaiting
       FROM q.dohmh_food WHERE TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41 GROUP BY camis""",

    """CREATE TEMP TABLE pts AS
       WITH
       a AS (SELECT 'businesses' m, TRY_CAST(longitude AS DOUBLE) x, TRY_CAST(latitude AS DOUBLE) y, 1.0 v, NULL k
             FROM q.dcwp_licences WHERE license_status = 'Active'),
       b AS (SELECT 'new_licences_12m', TRY_CAST(longitude AS DOUBLE), TRY_CAST(latitude AS DOUBLE), 1.0, NULL
             FROM q.dcwp_licences WHERE TRY_CAST(license_creation_date AS TIMESTAMP) > since('dcwp')),
       c AS (SELECT 'restaurants', x, y, 1.0, NULL FROM rest),
       d AS (SELECT 'new_restaurants_12m', x, y, 1.0, NULL FROM rest WHERE prepermit > since('dohmh')),
       e AS (SELECT 'restaurants_awaiting', x, y, 1.0, NULL FROM rest WHERE awaiting),
       f AS (SELECT 'food_stores', (ST_GeomFromGeoJSON(georeference)).ST_X(), (ST_GeomFromGeoJSON(georeference)).ST_Y(), 1.0, NULL
             FROM q.nys_retail_food WHERE georeference IS NOT NULL),
       g AS (SELECT 'liquor', (ST_GeomFromGeoJSON(georeference)).ST_X(), (ST_GeomFromGeoJSON(georeference)).ST_Y(), 1.0, NULL
             FROM q.sla_liquor WHERE georeference IS NOT NULL),
       h AS (SELECT 'complaints_311_12m', TRY_CAST(longitude AS DOUBLE), TRY_CAST(latitude AS DOUBLE), 1.0, complaint_type
             FROM q.nyc_311 WHERE TRY_CAST(created_date AS TIMESTAMP) > since('311')),
       i AS (SELECT 'permits_12m', TRY_CAST(longitude AS DOUBLE), TRY_CAST(latitude AS DOUBLE), 1.0, NULL
             FROM q.dob_now WHERE TRY_CAST(issued_date AS TIMESTAMP) > since('permits')),
       j AS (SELECT 'permits_12m', TRY_CAST(gis_longitude AS DOUBLE), TRY_CAST(gis_latitude AS DOUBLE), 1.0, NULL
             FROM q.dob_permits
             WHERE coalesce(TRY_CAST(left(issuance_date, 10) AS DATE),
                            TRY_STRPTIME(left(issuance_date, 10), '%m/%d/%Y')::DATE)::TIMESTAMP > since('bis')),
       -- average weekday riders per station over the window, placed at the station
       s AS (SELECT 'subway_weekday_riders', any_value(TRY_CAST(longitude AS DOUBLE)), any_value(TRY_CAST(latitude AS DOUBLE)),
                    sum(TRY_CAST(riders AS DOUBLE)) / count(DISTINCT TRY_CAST(transit_timestamp AS TIMESTAMP)::DATE), NULL
             FROM q.subway_ridership
             WHERE TRY_CAST(transit_timestamp AS TIMESTAMP) > since('subway')
               AND dayofweek(TRY_CAST(transit_timestamp AS TIMESTAMP)) BETWEEN 1 AND 5
             GROUP BY station_complex_id),
       r AS (SELECT 'retail_sqft', x, y, retail_sqft, NULL FROM lots WHERE retail_sqft > 0)
       SELECT row_number() OVER () AS id, * FROM (
         SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c UNION ALL SELECT * FROM d
         UNION ALL SELECT * FROM e UNION ALL SELECT * FROM f UNION ALL SELECT * FROM g UNION ALL SELECT * FROM h
         UNION ALL SELECT * FROM i UNION ALL SELECT * FROM j UNION ALL SELECT * FROM s UNION ALL SELECT * FROM r
       ) t(metric, x, y, v, k)""",

    # A point on a shared border could fall in two polygons: keep one.
    """CREATE TEMP TABLE placed AS
       SELECT p.id, p.metric, p.v, p.k, n.code
       FROM pts p JOIN nta n ON ST_Within(ST_Point(p.x, p.y), n.geom)
       WHERE p.x IS NOT NULL AND p.y BETWEEN 40.4 AND 41
       QUALIFY row_number() OVER (PARTITION BY p.id ORDER BY n.code) = 1""",

    # Property sales: tax lot -> PLUTO coordinates, else the block's.
    """CREATE TEMP TABLE sales AS
       WITH s AS (SELECT row_number() OVER () AS id, TRY_CAST(sale_price AS DOUBLE) price,
                         borough || lpad(block, 5, '0') || lpad(lot, 4, '0') AS bbl,
                         borough || lpad(block, 5, '0') AS block
                  FROM q.dof_sales WHERE TRY_CAST(sale_price AS DOUBLE) > 10000),
            blk AS (SELECT block, any_value(x) x, any_value(y) y FROM lots GROUP BY 1),
            xy AS (SELECT s.id, s.price, coalesce(l.x, b.x) x, coalesce(l.y, b.y) y
                   FROM s LEFT JOIN lots l ON l.bbl = s.bbl LEFT JOIN blk b ON b.block = s.block)
       SELECT xy.id, xy.price, n.code FROM xy LEFT JOIN nta n ON ST_Within(ST_Point(xy.x, xy.y), n.geom)
       QUALIFY row_number() OVER (PARTITION BY xy.id ORDER BY n.code) = 1""",

    # Storefront vacancy: the latest reporting year that is (nearly) complete.
    # Filings from 2022-23 on already carry 2020 NTA codes.
    """CREATE TEMP TABLE vac_year AS
       WITH y AS (SELECT reporting_year yr, count(*) lots FROM q.storefront_vacancy
                  WHERE reporting_year ~ '^[0-9]{4}$' GROUP BY 1)
       SELECT max(yr) yr FROM y WHERE lots >= 0.9 * (SELECT max(lots) FROM y)""",
    """CREATE TEMP TABLE vac AS
       SELECT nta AS code, sum(TRY_CAST(storefronts AS BIGINT)) storefronts, sum(TRY_CAST(vacant AS BIGINT)) vacant
       FROM q.storefront_vacancy WHERE reporting_year = (SELECT yr FROM vac_year) GROUP BY 1""",

    # Residents and income: ACS 2023 tracts, rolled up through the city's own
    # 2020 tract -> NTA equivalency (tracts nest exactly inside NTAs).
    """CREATE TEMP TABLE pop AS
       SELECT e.ntacode AS code, sum(a.population) residents,
              sum(a.median_income * a.population) FILTER (WHERE a.median_income IS NOT NULL)
                / nullif(sum(a.population) FILTER (WHERE a.median_income IS NOT NULL), 0) income
       FROM q.acs_tracts a JOIN q.tract_nta e ON e.ct2020 = a.tract GROUP BY 1""",

    """CREATE TEMP TABLE top311 AS
       SELECT code, k AS top_complaint FROM (
         SELECT code, k, count(*) n FROM placed WHERE metric = 'complaints_311_12m' GROUP BY ALL)
       QUALIFY row_number() OVER (PARTITION BY code ORDER BY n DESC, k) = 1""",

    """CREATE OR REPLACE TABLE nta_profile AS
       WITH m AS (
         SELECT code,
           count(*) FILTER (WHERE metric = 'businesses') businesses,
           count(*) FILTER (WHERE metric = 'new_licences_12m') new_licences_12m,
           count(*) FILTER (WHERE metric = 'restaurants') restaurants,
           count(*) FILTER (WHERE metric = 'new_restaurants_12m') new_restaurants_12m,
           count(*) FILTER (WHERE metric = 'restaurants_awaiting') restaurants_awaiting,
           count(*) FILTER (WHERE metric = 'food_stores') food_stores,
           count(*) FILTER (WHERE metric = 'liquor') liquor,
           count(*) FILTER (WHERE metric = 'complaints_311_12m') complaints_311_12m,
           count(*) FILTER (WHERE metric = 'permits_12m') permits_12m,
           count(*) FILTER (WHERE metric = 'subway_weekday_riders') subway_stations,
           round(coalesce(sum(v) FILTER (WHERE metric = 'subway_weekday_riders'), 0)) subway_weekday_riders,
           round(coalesce(sum(v) FILTER (WHERE metric = 'retail_sqft'), 0)) retail_sqft
         FROM placed GROUP BY 1),
       sl AS (SELECT code, count(*) sales_12m, median(price) median_sale_price FROM sales
              WHERE code IS NOT NULL GROUP BY 1)
       SELECT n.code, n.name, coalesce(?::JSON->>n.ntatype, 'other') AS kind,
              round(p.residents)::BIGINT AS residents, round(p.income) AS median_income,
              coalesce(m.businesses, 0) businesses, coalesce(m.new_licences_12m, 0) new_licences_12m,
              coalesce(m.restaurants, 0) restaurants, coalesce(m.new_restaurants_12m, 0) new_restaurants_12m,
              coalesce(m.restaurants_awaiting, 0) restaurants_awaiting,
              coalesce(m.food_stores, 0) food_stores, coalesce(m.liquor, 0) liquor,
              coalesce(m.complaints_311_12m, 0) complaints_311_12m, t.top_complaint,
              coalesce(m.permits_12m, 0) permits_12m,
              coalesce(m.subway_stations, 0) subway_stations, coalesce(m.subway_weekday_riders, 0) subway_weekday_riders,
              v.storefronts, v.vacant, round(100.0 * v.vacant / nullif(v.storefronts, 0), 1) vacancy_pct,
              coalesce(sl.sales_12m, 0) sales_12m, sl.median_sale_price,
              coalesce(m.retail_sqft, 0) retail_sqft,
              -- per-resident figures only where people actually live
              CASE WHEN p.residents >= ? THEN round(1e4 * coalesce(m.businesses, 0) / p.residents, 1) END businesses_per_10k,
              CASE WHEN p.residents >= ? THEN round(1e4 * coalesce(m.restaurants, 0) / p.residents, 1) END restaurants_per_10k,
              CASE WHEN p.residents >= ? THEN round(1e4 * coalesce(m.food_stores, 0) / p.residents, 1) END food_stores_per_10k,
              CASE WHEN p.residents >= ? THEN round(1e4 * coalesce(m.liquor, 0) / p.residents, 1) END liquor_per_10k,
              CASE WHEN p.residents >= ? THEN round(1e3 * coalesce(m.complaints_311_12m, 0) / p.residents, 1) END complaints_per_1k,
              CASE WHEN p.residents >= ? THEN round(coalesce(m.retail_sqft, 0) / p.residents, 1) END retail_sqft_per_resident,
              ?::VARCHAR AS built_at
       FROM nta n LEFT JOIN pop p USING (code) LEFT JOIN m USING (code) LEFT JOIN top311 t USING (code)
       LEFT JOIN vac v USING (code) LEFT JOIN sl USING (code)
       ORDER BY n.code""",
]

# What each column means, which window it covers, and how much of its source
# was placed — shown under the table on the page, so no figure is unexplained.
METRICS = [
    ("residents", "People living here: ACS 2023 five-year estimates, census tracts summed into the NTA.", None, None),
    ("median_income", "Median household income: tract medians averaged, weighted by population (an approximation; medians cannot be added).", None, None),
    ("businesses", "Active DCWP licences — contractors, tobacco, second-hand dealers and other licensed trades. Not restaurants or grocers.", None, "businesses"),
    ("new_licences_12m", "DCWP licences first created in the 12 months to the source's newest record.", "dcwp", "new_licences_12m"),
    ("restaurants", "Food service establishments with a health inspection record (one per CAMIS).", None, "restaurants"),
    ("new_restaurants_12m", "Restaurants given a pre-permit inspection — what a new restaurant gets before opening — in the last 12 months.", "dohmh", "new_restaurants_12m"),
    ("restaurants_awaiting", "Restaurants on file that have not had a first inspection yet (the city's 1900-01-01 placeholder): newly registered.", None, "restaurants_awaiting"),
    ("food_stores", "NYS-licensed retail food stores: groceries, bodegas, delis, supermarkets.", None, "food_stores"),
    ("liquor", "Active NY State liquor licences, all kinds (bars, restaurants, liquor stores, wholesalers).", None, "liquor"),
    ("complaints_311_12m", "311 complaints made in the last 12 months, every type.", "311", "complaints_311_12m"),
    ("permits_12m", "Building work permits issued in the last 12 months: DOB NOW plus the older BIS system.", "permits", "permits_12m"),
    ("subway_weekday_riders", "Average weekday riders entering the subway stations inside the NTA over the last 12 months.", "subway", "subway_weekday_riders"),
    ("vacancy_pct", "Share of ground- and second-floor storefronts reported vacant (Local Law 157), latest complete filing year.", None, None),
    ("median_sale_price", "Median price of property sales over $10,000 in DOF's rolling 12-month file, all property types.", None, None),
    ("retail_sqft", "Retail floor area on the NTA's tax lots (PLUTO).", None, "retail_sqft"),
]


def build(db: Path = DB) -> tuple[int, list[tuple]]:
    """Build both tables inside `db`. Returns (rows, notes)."""
    import duckdb
    import json
    built = datetime.now(timezone.utc).isoformat(timespec="seconds")
    con = duckdb.connect()
    try:
        con.execute(f"ATTACH {_lit(db)} AS q (READ_ONLY)")
        have = {r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'q' "
            "UNION SELECT view_name FROM duckdb_views() WHERE database_name = 'q'").fetchall()}
        missing = [t for t in NEEDS if t not in have]
        if missing:
            raise RuntimeError(f"pull these first: {', '.join(missing)}")
        for stmt in SQL[:-1]:
            con.execute(stmt)
        con.execute(SQL[-1], [json.dumps(KIND)] + [MIN_RESIDENTS] * 6 + [built])
        n = con.execute("SELECT count(*) FROM nta_profile").fetchone()[0]
        notes = []
        windows = dict(con.execute("SELECT src, newest FROM win").fetchall())
        vac_year = con.execute("SELECT yr FROM vac_year").fetchone()[0]
        for col, text, win, metric in METRICS:
            w_from = w_to = None
            if win:
                w_to = windows[win]
                w_from = con.execute("SELECT ?::TIMESTAMP - INTERVAL 365 DAY", [w_to]).fetchone()[0]
            total = placed = None
            if metric:
                total, placed = con.execute(
                    """SELECT count(*), count(x.id) FROM pts p LEFT JOIN placed x USING (id)
                       WHERE p.metric = ?""", [metric]).fetchone()
            if col == "median_sale_price":
                total, placed = con.execute("SELECT count(*), count(code) FROM sales").fetchone()
                text += " Sales placed by tax lot, or by block for condo units."
            if col == "vacancy_pct":
                text = text.replace("latest complete filing year", f"filing year {vac_year}")
            notes.append((col, text,
                          str(w_from)[:10] if w_from else None, str(w_to)[:10] if w_to else None,
                          total, placed))
        con.execute("""CREATE OR REPLACE TABLE nta_profile_notes (metric VARCHAR, definition VARCHAR,
                       window_from VARCHAR, window_to VARCHAR, records BIGINT, placed BIGINT)""")
        con.executemany("INSERT INTO nta_profile_notes VALUES (?,?,?,?,?,?)", notes)
        OUT.mkdir(exist_ok=True)
        for t in ("nta_profile", "nta_profile_notes"):
            con.execute(f"COPY {t} TO {_lit(OUT / f'{t}.parquet')} (FORMAT PARQUET)")
        return n, notes
    finally:
        con.close()


def _lit(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def install(db: Path = DB) -> None:
    """Load the built Parquet into queens.duckdb (waits out a writer's lock)."""
    import fetch
    con = fetch._connect_rw()
    try:
        for t in ("nta_profile", "nta_profile_notes"):
            con.execute(f'CREATE OR REPLACE TABLE "{t}" AS SELECT * FROM read_parquet(?)',
                        [str(OUT / f"{t}.parquet")])
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DB), help=argparse.SUPPRESS)
    a = ap.parse_args()
    run = jobs.new_run()
    job = jobs.Job(jobs.queue("nta_profile", run, "derived")).start(step="building")
    try:
        t0 = time.time()
        n, notes = build(Path(a.db))
        if Path(a.db) == DB:
            install()
        for col, _text, w_from, w_to, total, placed in notes:
            if total:
                print(f"  {col:<24}{placed:>9,} of {total:>9,} placed ({100 * placed / total:5.1f}%)"
                      + (f"  · {w_from} → {w_to}" if w_to else ""))
        if gcs.enabled():
            job.step("uploading")
            dests = [gcs.upload(OUT / f"{t}.parquet", f"{t}.parquet", prefix=gcs.DERIVED)
                     for t in ("nta_profile", "nta_profile_notes")]
            job.upload("uploaded", dests[0])
        else:
            job.upload("off", "no bucket configured")
        job.succeed(n)
        print(f"\n  nta_profile: {n} neighbourhoods · {time.time() - t0:.0f}s -> {OUT}\n")
        return 0
    except (KeyboardInterrupt, SystemExit):
        job.fail("interrupted")
        raise
    except Exception as e:  # noqa: BLE001 — recorded, then shown
        job.fail(f"{type(e).__name__}: {e}")
        print(f"\n  FAILED: {type(e).__name__}: {e}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
