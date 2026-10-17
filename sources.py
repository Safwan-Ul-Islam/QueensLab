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
    kind: str = "socrata"        # socrata | acs | cbp | overture
    select: str | None = None    # optional SoQL select
    group: str | None = None     # optional SoQL group
    window_by: str | None = None # date column: pull one calendar month at a time
    since: str | None = None     # ...starting here (ISO). Needs window_by.


# Tag "local" = too big for Render's free dyno (512 MB, sleeps after 15 idle
# minutes). Pull on a laptop with fetch.py; the web /fetch endpoint refuses them.


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
           order_by="bbl", tags=("property", "local"),
           note="Every lot: land use, floor area, year built, commercial sqft. "
                "⚠️ NOT in --core on purpose: 324k rows with a server-side sort took "
                "16 minutes measured (2026-10-17), and Render's free dyno sleeps after "
                "15 idle minutes, so it will not reliably finish there. Pull it locally: "
                "python fetch.py --only pluto"),

    Source("dof_sales", "DOF rolling property sales",
           "data.cityofnewyork.us", "usep-8jbt",
           where="borough='4'", probed=26_602,
           order_by="sale_date", tags=("property",),
           note="Borough 4 = Queens. Real transaction prices."),

    Source("dob_permits", "DOB permit issuance",
           "data.cityofnewyork.us", "ipu4-2q9a",
           where="borough='QUEENS'", probed=841_308,
           order_by="job__", tags=("property", "local"),
           note="The OLD filing system (BIS): history from 1989, still issuing, but most "
                "new permits moved to DOB NOW (17k here in 2021, 1.2k so far in 2026). "
                "Read both. Dates come in two formats (YYYY-MM-DD and MM/DD/YYYY). "
                "Exact match on borough: upper() stops Socrata using its index."),

    Source("dob_now", "DOB NOW build permits",
           "data.cityofnewyork.us", "rbx6-tga4",
           where="upper(borough)='QUEENS'", probed=198_081,
           order_by="job_filing_number", tags=("property", "local")),

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
           note="82 NTAs in the 2020 scheme (QN0101…). ⚠️ DCWP licences and "
                "nta_population use the 2010 scheme (QN61, QN01) — the codes do NOT "
                "join across. Use nta_population.nta_name for names alongside DCWP."),

    Source("tract_nta", "2020 census tract to NTA equivalency",
           "data.cityofnewyork.us", "hm78-6dwm",
           where="boroname='Queens'", probed=725,
           order_by="geoid", tags=("geo",),
           note="The city's own table of which 2020 tract sits in which 2020 NTA. Tracts "
                "nest exactly, so ACS tract figures add up to current NTA populations — "
                "nta_population is 2010 counts on 2010 codes. Used by nta_profile.py."),

    Source("nta_population", "Population by NTA",
           "data.cityofnewyork.us", "swpk-hqdp",
           where="upper(borough)='QUEENS'", probed=116,
           order_by="nta_code", tags=("geo", "core"),
           note="Denominator for every per-capita figure."),
]

# ── Conditions and demand ────────────────────────────────────────────────
CONDITIONS = [
    Source("nyc_311", "311 complaints (since Oct 2024)",
           "data.cityofnewyork.us", "erm2-nwe9",
           where="borough='QUEENS'", probed=1_869_933,
           window_by="created_date", since="2024-10-01T00:00:00",
           order_by="unique_key", tags=("conditions", "local"),
           note="UNBOUNDED THIS TIMES OUT — 30M+ rows citywide; the date floor is required. "
                "Pulled month by month. upper(borough) timed out even on a count; the "
                "exact match answers in 5s."),

    Source("subway_ridership", "Subway riders by station and hour (2025 on)",
           "data.ny.gov", "5wq4-mkjj",
           where="borough='Queens'",
           window_by="transit_timestamp", since="2025-01-01T00:00:00",
           select=("transit_timestamp, station_complex_id, station_complex, latitude, "
                   "longitude, sum(ridership) AS riders, sum(transfers) AS transfers"),
           group="transit_timestamp, station_complex_id, station_complex, latitude, longitude",
           tags=("movement", "local"),
           note="One row per Queens station per hour; fare classes and payment methods "
                "summed at the source. This is the 2025-onward file. The old one "
                "(wujg-7c2s) stops at 31 Dec 2024."),
]

# ── Open places data (not Socrata) ──────────────────────────────────────
PLACES = [
    Source("overture_places", "Overture Maps places (open POI data)",
           "overturemaps.org", "theme=places/type=place",
           kind="overture", tags=("business", "local"),
           note="Every business and place Overture knows in Queens: name, category, "
                "open/permanently-closed status, website, phone, brand, address, confidence, "
                "and which contributors (Meta, Microsoft, Foursquare, BrightQuery, "
                "AllThePlaces...) it came from. Cut to Queens by the 2020 NTA polygons and "
                "tagged with the NTA. Licences differ per contributor (kept in `licence`): "
                "CDLA-Permissive-2.0, Apache-2.0 (Foursquare — keep its NOTICE), CC0. "
                "Latest monthly release found automatically."),
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
           kind="cbp", tags=("business", "local"),
           note="Queens County only (state 36, county 081). Establishments and jobs "
                "by industry. County totals, not a list of shop names."),
]

ALL: list[Source] = BUSINESSES + PLACES + PROPERTY + GEOGRAPHY + CONDITIONS + CENSUS
BY_KEY = {s.key: s for s in ALL}


def core() -> list[Source]:
    """The subset worth pulling first."""
    return [s for s in ALL if "core" in s.tags]
