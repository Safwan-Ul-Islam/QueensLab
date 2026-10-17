# Queens data — every source, with links

**Status column:** ✅ wired into `fetch.py` · ⬜ free, needs a small adapter · 💰 paid.
Row counts marked **probed** were returned by the live API on 2026-10-17.

---

## Already wired — `python research/fetch.py --all`

| Source | Link | Queens | Why it matters |
|---|---|---|---|
| ✅ **DCWP licensed businesses** | [`w7w3-xahh`](https://data.cityofnewyork.us/resource/w7w3-xahh.json) | **13,555** (8,756 active) | The core list. Ships lat/lng, BBL, BIN, census tract, NTA. |
| ✅ **DOHMH food establishments** | [`43nn-pn8j`](https://data.cityofnewyork.us/resource/43nn-pn8j.json) | **74,524** rows (~7,050 businesses) | One row per inspection, not per business. |
| ✅ **NYS liquor licences** | [`9s3h-dpkz`](https://data.ny.gov/resource/9s3h-dpkz.json) | **5,360** | Bars/restaurants. ⚠️ dates are licence terms, not openings. |
| ✅ **NYS retail food stores** | [`9a8c-vfzj`](https://data.ny.gov/resource/9a8c-vfzj.json) | **2,489** | Bodegas, groceries, delis. |
| ✅ **PLUTO tax lots** | [`64uk-42ks`](https://data.cityofnewyork.us/resource/64uk-42ks.json) | **324,483** | Land use, floor area, year built, commercial sqft. |
| ✅ **DOF rolling sales** | [`usep-8jbt`](https://data.cityofnewyork.us/resource/usep-8jbt.json) | **26,602** | Real transaction prices (borough = `4`). |
| ✅ **DOB permits** | [`ipu4-2q9a`](https://data.cityofnewyork.us/resource/ipu4-2q9a.json) | **841,308** | Build-outs = businesses opening. |
| ✅ **DOB NOW permits** | [`rbx6-tga4`](https://data.cityofnewyork.us/resource/rbx6-tga4.json) | **198,081** | The newer filing system. |
| ✅ **2020 NTAs** | [`9nt8-h7nd`](https://data.cityofnewyork.us/resource/9nt8-h7nd.json) | **82** | The right unit for neighbourhood analysis. |
| ✅ **Storefront vacancy** | [`92iy-9c3n`](https://data.cityofnewyork.us/resource/92iy-9c3n.json) | **47,278** buildings × year | Empty storefronts. Kept per building, because the file has no unit id. |
| ✅ **Population by NTA** | [`swpk-hqdp`](https://data.cityofnewyork.us/resource/swpk-hqdp.json) | **116** | Denominator for every per-capita figure. |
| ✅ **311 complaints** | [`erm2-nwe9`](https://data.cityofnewyork.us/resource/erm2-nwe9.json) | **1,869,933** since Oct 2024 | ⚠️ **Must** be date-bounded or it times out. Pulled one month at a time. |
| ✅ **Sidewalk cafes** | [`qcdj-rwhu`](https://data.cityofnewyork.us/resource/qcdj-rwhu.json) | **324** (whole file) | No borough column; filter by ZIP after load. |
| ✅ **Subway ridership** | [`5wq4-mkjj`](https://data.ny.gov/resource/5wq4-mkjj.json) | **1,171,471** station-hours, 79 stations | 2025 onward, summed per station per hour at the source. The 2020–24 file is [`wujg-7c2s`](https://data.ny.gov/resource/wujg-7c2s.json). |

**Browse for more:** [NYC Open Data](https://opendata.cityofnewyork.us/) · [NY State Open Data](https://data.ny.gov/) ·
Search the catalog: `https://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&q=YOUR+TERM`

---

## Free, worth adding next

| Source | Link | What it gives |
|---|---|---|
| ✅ **Census ACS 2023 tracts** | [data.census.gov table](https://data.census.gov/table/ACSDT5Y2023.B01003?g=050XX00US36081$1400000) | People, income, age, renters for 725 Queens tracts. `api.census.gov` now wants a key; this public download does not. `python research/fetch.py --only acs_tracts` |
| ✅ **County Business Patterns 2023** | [county file](https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23co.zip) | Queens establishments and jobs by industry. County totals, not shop names. `python research/fetch.py --only cbp_industry` |
| ⬜ **BLS QCEW** | [QCEW API](https://www.bls.gov/cew/downloadable-data-files.htm) | Employment + **wages** by county × industry. No key needed. |
| ⬜ **NYS corporate registry** | [data.ny.gov](https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6) | Incorporation dates — **real** business age, unlike the liquor file. |
| ⬜ **ACRIS** | [`bnx9-e6tj`](https://data.cityofnewyork.us/resource/bnx9-e6tj.json) | Deeds and mortgages. |
| ⬜ **HUD Fair Market Rents** | [huduser.gov](https://www.huduser.gov/portal/dataset/fmr-api.html) | Rent baselines by ZIP. |
| ⬜ **Overture Maps** | [overturemaps.org](https://overturemaps.org/download/) | Free GeoParquet POIs, monthly. DuckDB reads it natively. |
| ⬜ **Foursquare OS Places** | [HuggingFace](https://huggingface.co/datasets/foursquare/fsq-os-places) | ~100M POIs, **Apache-2.0** — commercial use allowed. |
| ⬜ **OpenStreetMap** | [Overpass](https://overpass-api.de/) | Amenities, free, no key. |

---

## Scrapers — for THIS folder only

Your own rule (log, 2026-10-10): *"Apify remains fine for internal research only."*
Research is the permitted side. **Nothing scraped may ever reach the production database.**

| Tool | Link | Cost | Good for |
|---|---|---|---|
| **Outscraper** | [outscraper.com](https://outscraper.com/) | ~$3/1k places | Google Maps category density, review mass per neighbourhood. |
| **Apify** | [apify.com](https://apify.com/store) | pay-per-run | Ready-made actors for Maps, Yelp, LoopNet. |
| **ScrapingBee / Bright Data** | [scrapingbee.com](https://www.scrapingbee.com/) | from ~$49/mo | Sites that block plain requests. |
| **Playwright** (already installed) | [playwright.dev](https://playwright.dev/python/) | free | BID sites, Queens EDC reports, anything small. |

**Worth scraping, because no free source has it:**
- **Commercial rent asks** — [LoopNet](https://www.loopnet.com/), [Crexi](https://www.crexi.com/) ·
  your only rent data today (LL157) is **aggregated**, not per-storefront
- **[Queens Economic Development Corp](https://queensny.org/)** — corridor reports
- **Queens BIDs** — [NYC SBS BID directory](https://www.nyc.gov/site/sbs/neighborhoods/bids.page)
- **Google Maps categories** — which trades cluster where

**Rules:** respect `robots.txt`, rate-limit, cache raw responses in `research/raw/`,
and record where every file came from.

---

## The wall

```
research/   →  DuckDB, Parquet, scraped data, exploratory SQL   ← this folder
pipeline/   →  the product. Neon Postgres. licensed sources only.
```

Nothing in `research/` imports from `pipeline/` or writes to `DATABASE_URL`.
When something here earns its way into the model, it gets a **proper collector**
in `ingestion/collectors/` with a licensed source — it does not get copied across.
