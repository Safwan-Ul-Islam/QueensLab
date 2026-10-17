"""Every Queens data source, declared in one place.

A source is added by appending one entry here — nothing else changes.
Each `where` clause was PROBED against the live API before being written down;
the `probed` figure is what it returned on 2026-10-17.

⚠️  THE WALL: nothing in this folder may import from `pipeline/` or `ingestion/`,
and nothing fetched here may be written to the production database. This is the
research side. Mixing happens later, deliberately, not by accident.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Source:
    key: str                     # table name in DuckDB
    label: str                   # human name
    domain: str                  # socrata host
    dataset: str                 # socrata id
    where: str | None = None     # Queens filter, already verified
    probed: int | None = None    # rows this returned when probed
    note: str = ""
    order_by: str | None = None  # stable paging key
    tags: tuple = field(default_factory=tuple)
    kind: str = "socrata"        # socrata | acs | cbp
    select: str | None = None    # optional SoQL select
    group: str | None = None     # optional SoQL group


# ── Businesses: who is actually trading in Queens ────────────────────────
BUSINESSES = [
    Source("dcwp_licences", "DCWP licensed businesses",
           "data.cityofnewyork.us", "w7w3-xahh",
           where="upper(address_borough)='QUEENS'", probed=13_555,
           order_by="license_nbr", tags=("business", "core"),
           note="THE core list. Ships latitude, longitude, bbl, bin, census_tract, nta — "
                "no geocoding needed. 8,756 of these are status='Active'."),

    Source("dohmh_food", "DOHMH food establishments",
           "data.cityofnewyork.us", "43nn-pn8j",
           where="upper(boro)='QUEENS'", probed=74_524,
           order_by="camis", tags=("business", "core"),
           note="One row per INSPECTION, not per business — ~7,050 distinct CAMIS in Queens."),

    Source("sla_liquor", "NY State active liquor licences",
           "data.ny.gov", "9s3h-dpkz",
           where="upper(premisescounty)='QUEENS'", probed=5_360,
           order_by="licensepermitid", tags=("business",),
           note="⚠️ originalissuedate is NOT the business start date — it equals lastissuedate "
                "in every row and only reaches back to 2021, so it is the CURRENT licence "
                "term. Good for 'what has been licensed recently', useless for age. "
                "(Claimed as business age on 2026-10-16; wrong, corrected 2026-10-17.)"),

    Source("nys_retail_food", "NYS retail food stores",
           "data.ny.gov", "9a8c-vfzj",
           where="upper(county)='QUEENS'", probed=2_489,
           order_by="license_number", tags=("business",),
           note="Groceries, bodegas, delis — the categories DOHMH misses."),

    Source("sidewalk_cafes", "Sidewalk cafe licences (historical)",
           "data.cityofnewyork.us", "qcdj-rwhu",
           where=None, probed=None,
           order_by="business_name", tags=("business",),
           note="No borough column — has `city` (Astoria, Flushing...). Pulled whole, "
                "filter by ZIP in DuckDB."),
]

# ── Built form and property ──────────────────────────────────────────────
PROPERTY = [
    Source("pluto", "PLUTO tax lots",
           "data.cityofnewyork.us", "64uk-42ks",
           where="upper(borough)='QN'", probed=324_483,
           order_by="bbl", tags=("property", "core"),
           note="Every lot: land use, floor area, year built, commercial sqft."),

    Source("dof_sales", "DOF rolling property sales",
           "data.cityofnewyork.us", "usep-8jbt",
           where="borough='4'", probed=26_602,
           order_by="sale_date", tags=("property",),
           note="Borough 4 = Queens. Real transaction prices."),

    Source("dob_permits", "DOB permit issuance",
           "data.cityofnewyork.us", "ipu4-2q9a",
           where="upper(borough)='QUEENS'", probed=841_298,
           order_by="job__", tags=("property",),
           note="Big. Build-outs and renovations = businesses opening."),

    Source("dob_now", "DOB NOW build permits",
           "data.cityofnewyork.us", "rbx6-tga4",
           where="upper(borough)='QUEENS'", probed=198_081,
           order_by="job_filing_number", tags=("property",)),

    Source("storefront_vacancy", "LL157 storefront vacancy",
           "data.cityofnewyork.us", "92iy-9c3n",
           where="upper(borough)='QUEENS'", probed=47_278,
           order_by="reporting_year, borough_block_lot", tags=("property", "core"),
           select=("reporting_year, borough_block_lot, max(nta) AS nta, "
                   "count(*) AS storefronts, "
                   "sum(case when vacant_on_12_31='YES' then 1 else 0 end) AS vacant"),
           group="reporting_year, borough_block_lot",
           note="One row per building per year. The file has no per-storefront id, "
                "so the lot is the honest grain. 2025 filings are partial."),
]

# ── Geography and people ─────────────────────────────────────────────────
GEOGRAPHY = [
    Source("ntas", "2020 Neighborhood Tabulation Areas",
           "data.cityofnewyork.us", "9nt8-h7nd",
           where="upper(boroname)='QUEENS'", probed=82,
           order_by="nta2020", tags=("geo", "core"),
           note="82 NTAs. The right unit for 'which Queens neighbourhood' analysis."),

    Source("nta_population", "Population by NTA",
           "data.cityofnewyork.us", "swpk-hqdp",
           where="upper(borough)='QUEENS'", probed=116,
           order_by="nta_code", tags=("geo", "core"),
           note="Denominator for every per-capita figure."),
]

# ── Conditions and demand ────────────────────────────────────────────────
CONDITIONS = [
    Source("nyc_311", "311 complaints (last 2 years)",
           "data.cityofnewyork.us", "erm2-nwe9",
           where="upper(borough)='QUEENS' AND created_date > '2024-10-01T00:00:00'",
           probed=237_854,
           order_by="unique_key", tags=("conditions",),
           note="UNBOUNDED THIS TIMES OUT — 30M+ rows citywide. The date bound is required, "
                "not optional. 237,854 was the 90-day Queens count when probed."),

    Source("subway_ridership", "Subway hourly ridership by station",
           "data.ny.gov", "wujg-7c2s",
           where=None, probed=None,
           order_by="transit_timestamp", tags=("movement",),
           note="CITYWIDE and enormous. Filter to Queens stations after load, and always "
                "bound the date — see fetch.py --where."),
]

# Census does not use Socrata. api.census.gov now returns a missing-key page.
# These two are the public files that still answer without a key.
CENSUS = [
    Source("acs_tracts", "ACS 2023 people and income by tract",
           "data.census.gov", "ACSDT5Y2023",
           probed=725, tags=("geo",), kind="acs",
           note="725 Queens tracts. Population, median income, median age, "
                "occupied homes, renter homes. Public table download, no API key."),

    Source("cbp_industry", "County Business Patterns 2023",
           "www2.census.gov", "cbp23co",
           kind="cbp", tags=("business",),
           note="Queens County only (state 36, county 081). Establishments and jobs "
                "by industry. County totals, not a list of shop names."),
]

ALL: list[Source] = BUSINESSES + PROPERTY + GEOGRAPHY + CONDITIONS + CENSUS
BY_KEY = {s.key: s for s in ALL}


def core() -> list[Source]:
    """The subset worth pulling first."""
    return [s for s in ALL if "core" in s.tags]
