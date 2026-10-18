#!/usr/bin/env python3
"""Business websites — the free check, then Firecrawl. Same engine as the
page's control panel (crawlkit.py); use whichever is handier.

    python crawl.py check [--limit N]   free: alive / dead / parked / hijacked / platform
    python crawl.py start [--limit N]   send alive own-sites to Firecrawl (uses credits)
    python crawl.py status              progress of the run at Firecrawl, credits left
    python crawl.py collect             fetch finished results into scraped/web_facts
    python crawl.py stop                cancel the run, keep what it finished
    python crawl.py extract [--all]     Claude reads stored pages again, no credits: the ones it
                                        could not read, or --all (after the requirements change)
    python crawl.py config              show the requirements (what is extracted)
    python crawl.py results [--limit N]

RESEARCH ONLY — see crawlkit.py.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import crawlkit as ck  # noqa: E402
import jobs  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["check", "start", "status", "collect", "stop", "config", "results",
                                       "extract"])
    ap.add_argument("--all", action="store_true", help="extract: every stored page")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--run", help=argparse.SUPPRESS)       # the web app's run id
    a = ap.parse_args()
    try:
        if a.cmd == "check":
            run = a.run or jobs.new_run()
            job = jobs.Job(jobs.claim("site_checks", run, "web" if a.run else "cli")).start(step="checking sites")
            jobs.heartbeat_forever()
            try:
                r = ck.check_sites(limit=a.limit, job=job)
            except BaseException as e:
                job.fail(f"{type(e).__name__}: {e}")
                raise
            job.upload("uploaded" if ck.gcs.enabled() else "off",
                       ck.gcs.url("site_checks.parquet", ck.PREFIX) if ck.gcs.enabled() else "no bucket")
            job.warn(" · ".join(f"{k} {v:,}" for k, v in sorted(r["counts"].items(), key=lambda x: -x[1])))
            job.succeed(r["domains"])
            print(f"\n  checked {r['domains']:,} websites: {r['counts']}\n")
        elif a.cmd == "start":
            print(json.dumps(ck.start(a.limit), indent=1))
        elif a.cmd in ("status", "collect"):
            print(json.dumps({"credits": ck.credits(), "runs": ck.refresh(collect_done=a.cmd == "collect"
                                                                       or True)}, indent=1, default=str))
        elif a.cmd == "stop":
            print(json.dumps(ck.stop_run(), indent=1, default=str))
        elif a.cmd == "extract":
            print(json.dumps(ck.extract_stored(all_pages=a.all, limit=a.limit), indent=1))
        elif a.cmd == "config":
            print(json.dumps(ck.config(), indent=1))
        elif a.cmd == "results":
            cols, rows = ck.results(a.limit or 20)
            for r in rows:
                print({k: v for k, v in zip(cols, r) if v not in (None, "", "[]")})
    except ck.FirecrawlError as e:
        print(f"\n  {e}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
