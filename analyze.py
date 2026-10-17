#!/usr/bin/env python3
"""First analyses over the Queens research database.

    python research/analyze.py                 run every analysis that has data
    python research/analyze.py --sql "SELECT ..."   ad-hoc query
    python research/analyze.py --tables        what is loaded

Each analysis declares the tables it needs and is SKIPPED with a note if they
are missing, so this runs usefully after `--core` and gets richer as you pull
more. Nothing here writes anywhere except stdout.
"""
from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
DB = Path(__file__).resolve().parent / "queens.duckdb"


def con():
    import duckdb
    if not DB.exists():
        print(f"  no database yet — run:  python research/fetch.py --core")
        raise SystemExit(1)
    return duckdb.connect(str(DB), read_only=True)


def have(c, *tables: str) -> bool:
    live = {r[0] for r in c.execute("SHOW TABLES").fetchall()}
    return all(t in live for t in tables)


def rule(title: str):
    print(f"\n  {title}\n  " + "─" * max(34, len(title)))


# ── analyses ─────────────────────────────────────────────────────────────
def a_inventory(c):
    rule("What is loaded")
    rows = c.execute("""SELECT key, rows, strftime(fetched_at,'%Y-%m-%d %H:%M') t,
                               round(bytes/1e6,1) mb
                        FROM _fetch_log ORDER BY rows DESC""").fetchall()
    print(f"  {'table':<20}{'rows':>12}{'MB':>8}  fetched")
    for k, n, t, mb in rows:
        print(f"  {k:<20}{n:>12,}{mb:>8}  {t}")
    print(f"  {'TOTAL':<20}{sum(r[1] for r in rows):>12,}")


def a_licence_mix(c):
    if not have(c, "dcwp_licences"):
        return "needs dcwp_licences"
    rule("Queens business mix — DCWP active licences")
    for cat, n in c.execute("""
            SELECT business_category, count(*) n FROM dcwp_licences
            WHERE license_status='Active'
            GROUP BY 1 ORDER BY n DESC LIMIT 15""").fetchall():
        print(f"  {n:>6}  {cat}")


def a_density(c):
    """The 'decentralise NYC business' thesis, measured."""
    if not have(c, "dcwp_licences", "nta_population"):
        return "needs dcwp_licences + nta_population"
    rule("Business density per 10k residents, by NTA")
    rows = c.execute("""
        WITH pop AS (
            SELECT nta_code, max(TRY_CAST(population AS BIGINT)) p
            FROM nta_population GROUP BY 1 HAVING p > 2000
        ), biz AS (
            SELECT nta, count(*) n FROM dcwp_licences
            WHERE license_status='Active' AND nta IS NOT NULL GROUP BY 1
        )
        SELECT b.nta, b.n, pop.p, round(10000.0*b.n/pop.p, 1) per10k
        FROM biz b JOIN pop ON pop.nta_code = b.nta
        ORDER BY per10k DESC""").fetchall()
    if not rows:
        return "no NTA overlap — check the nta/nta_code join keys"
    print(f"  {'NTA':<12}{'biz':>7}{'pop':>10}{'per 10k':>10}")
    for r in rows[:8]:
        print(f"  {r[0]:<12}{r[1]:>7}{r[2]:>10,}{r[3]:>10}")
    print(f"  {'…':<12}")
    for r in rows[-4:]:
        print(f"  {r[0]:<12}{r[1]:>7}{r[2]:>10,}{r[3]:>10}")
    mid = rows[len(rows)//2][3]
    print(f"\n  median {mid} per 10k · top {rows[0][3]} · bottom {rows[-1][3]}"
          f"  →  {rows[0][3]/rows[-1][3]:.0f}x spread across Queens")


def a_licence_vintage(c):
    """NOT business age — see the warning on sla_liquor in sources.py.

    originalissuedate == lastissuedate on every row and the series starts in
    2021, so this is the current licence TERM, not when a business opened. It
    still answers 'what has been licensed lately', which is a real signal, but
    it must never be read as business age.
    """
    if not have(c, "sla_liquor"):
        return "needs sla_liquor"
    rule("Liquor licences by term-issue year  (NOT business age)")
    rows = c.execute("""
        SELECT year(TRY_CAST(originalissuedate AS DATE)) y, count(*) n
        FROM sla_liquor WHERE originalissuedate IS NOT NULL
        GROUP BY 1 HAVING y IS NOT NULL ORDER BY y""").fetchall()
    top = max((r[1] for r in rows), default=1)
    for y, n in rows:
        print(f"  {y}  {n:>5}  {'█' * int(40 * n / top)}")
    print("\n  ⚠️  licence terms, not openings — this field cannot date a business")


def a_corridors(c):
    if not have(c, "sla_liquor"):
        return "needs sla_liquor"
    rule("Where Queens nightlife concentrates")
    for city, n in c.execute("""SELECT upper(city), count(*) n FROM sla_liquor
                                WHERE city IS NOT NULL GROUP BY 1
                                ORDER BY n DESC LIMIT 10""").fetchall():
        print(f"  {n:>5}  {city}")


def a_people(c):
    if not have(c, "acs_tracts"):
        return "needs acs_tracts"
    rule("People — ACS 2023, by census tract")
    n, pop = c.execute(
        "SELECT count(*), sum(population) FROM acs_tracts"
    ).fetchone()
    print(f"  {n:,} tracts · population summed across them {pop:,.0f}")
    print("  Income is each tract's own median, not a Queens-wide median.")
    print(f"\n  {'people':>8}  {'income':>10}  tract")
    for name, people, income in c.execute("""
            SELECT name, population, median_income FROM acs_tracts
            WHERE population IS NOT NULL ORDER BY population DESC LIMIT 8
            """).fetchall():
        money = f"{income:,.0f}" if income is not None else "—"
        print(f"  {people:>8,.0f}  {money:>10}  {name}")


def a_industries(c):
    if not have(c, "cbp_industry"):
        return "needs cbp_industry"
    rule("Industries — County Business Patterns 2023")
    print("  County totals. A dashed code is a sector. Employment is blank where Census hid it.")
    print(f"\n  {'estabs':>8}  {'jobs':>10}  industry")
    for naics, name, est, emp in c.execute("""
            SELECT naics, industry, establishments, employment FROM cbp_industry
            WHERE naics LIKE '%----' AND naics != '------'
            ORDER BY establishments DESC LIMIT 12
            """).fetchall():
        jobs = f"{emp:,.0f}" if emp is not None else "—"
        print(f"  {est:>8,.0f}  {jobs:>10}  {name} ({naics})")


def a_vacancy(c):
    if not have(c, "storefront_vacancy"):
        return "needs storefront_vacancy"
    rule("Storefronts empty at year end")
    print("  One row per building. A partial filing year is not the city's vacancy rate.")
    for year, buildings, fronts, empty in c.execute("""
            SELECT reporting_year, count(*),
                   sum(TRY_CAST(storefronts AS DOUBLE)),
                   sum(TRY_CAST(vacant AS DOUBLE))
            FROM storefront_vacancy GROUP BY 1 ORDER BY 1
            """).fetchall():
        share = (100.0 * (empty or 0) / fronts) if fronts else 0
        print(f"  {year:<16} {buildings:>7,.0f} buildings   "
              f"{empty or 0:>7,.0f} empty of {fronts or 0:>8,.0f}   {share:4.1f}%")


ANALYSES = [a_inventory, a_licence_mix, a_density, a_people, a_industries,
            a_vacancy, a_licence_vintage, a_corridors]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql"); ap.add_argument("--tables", action="store_true")
    a = ap.parse_args()
    c = con()
    if a.tables:
        for t in c.execute("SHOW TABLES").fetchall():
            n = c.execute(f'SELECT count(*) FROM "{t[0]}"').fetchone()[0]
            print(f"  {t[0]:<22}{n:>12,}")
        return 0
    if a.sql:
        for r in c.execute(a.sql).fetchall():
            print("  " + "  ".join(str(x) for x in r))
        return 0
    skipped = []
    for fn in ANALYSES:
        msg = fn(c)
        if msg:
            skipped.append(f"{fn.__name__.removeprefix('a_')}: {msg}")
    if skipped:
        print("\n  skipped (pull more first):")
        for s in skipped:
            print(f"    · {s}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
