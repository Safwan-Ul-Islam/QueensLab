"""Facts from a page's text, read by Claude — the default extractor since 2026-10-18.

Firecrawl reads the page as text (1 credit); Claude Haiku fills the
requirement fields from that text. Firecrawl's own AI extraction costs
5 credits a page and stays available as an option (config "extractor").

Tested before switching, on the same 20 pages (BizGenome log, 2026-10-18):
of 500 values, Haiku had 484 right (96.8%) and Firecrawl's extraction 433
(86.6%), judged field by field by Claude Sonnet against the page text. Haiku
was behind on 1 field of 25 (open_late, 18 vs 19 of 20).

The page text is stored (web_facts.markdown), so new requirements can be
read from pages already paid for: `python crawl.py extract --all`.
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request

import ask

MODEL = os.environ.get("EXTRACT_MODEL") or "claude-haiku-5-5"
# 2,000 cut 6 of the 20 test answers off mid-JSON (they scored as blank); the
# longest complete answer was 2,962 tokens.
MAX_TOKENS = 8000
PAGE_CHARS = 60_000       # the same cut as the stored copy, so a re-read sees the same text
# USD per million tokens, prompts up to 100k (Anthropic's pricing page, 2026-10-18).
# A page is at most ~15k tokens, so never the higher tier.
PRICE = {"input_tokens": 0.10, "cache_creation_input_tokens": 0.125,
         "cache_read_input_tokens": 0.01, "output_tokens": 0.50}

KINDS = {"text": "string", "number": "number", "yes/no": "boolean", "list": "array of strings",
         "prices": "array of {item, price, currency, unit}"}
# Written after the first test's misses; used only when the field is in the requirements.
HOW = {
    "business_type": "ALWAYS fill it — a short description of what the business is, inferred "
                     "from its name, headings and services if not stated outright.",
    "services_or_products": "list what the page offers (menu sections, service lists, product "
                            "categories), up to 10.",
    "street_address": "look everywhere — header, footer, contact or location sections, map "
                      "captions. Copy it as written.",
    "price_level": "if any prices are shown, judge it ($ cheap ... $$$$ expensive for that kind "
                   "of business); only leave empty when the page shows no prices.",
    "prices": "every item or service with a price on the page, up to 10.",
}


class ExtractError(Exception):
    pass


def system(c: dict) -> str:
    """The instructions, built from the requirements (config)."""
    lines = [c["prompt"], "",
             "Read the web page text the user sends and return ONLY one JSON object with exactly "
             "these keys (null, [] or false when the page does not state it):"]
    lines += [f'- "{f["name"]}" ({KINDS[f["type"]]}): {f["description"]}' for f in c["fields"]]
    if c.get("evidence"):
        lines.append('- "evidence": array of {"field", "quote"} — for each field you filled, the '
                     'exact supporting quote, AT MOST 10 WORDS.')
    lines += ["", "How to read the page:"]
    lines += [f"- {n}: {how}" for n, how in HOW.items() if any(f["name"] == n for f in c["fields"])]
    lines += ["- Numbers you cannot find: null, never 0. Yes/no facts the page does not mention: false.",
              "No text outside the JSON."]
    return "\n".join(lines)


def _call(system_text: str, user: str, max_tokens: int) -> dict:
    try:
        key = ask._key()
    except ask.AskError as e:
        raise ExtractError(str(e)) from e
    body = {"model": MODEL, "max_tokens": max_tokens,
            "system": [{"type": "text", "text": system_text, "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": user}]}
    req = urllib.request.Request(ask.API, data=json.dumps(body).encode(), method="POST",
                                 headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            # rate limit, overloaded, a passing server error: wait and try again
            if e.code in (429, 500, 502, 503, 504, 529) and attempt < 4:
                try:
                    wait = float(e.headers.get("retry-after") or 0)
                except ValueError:
                    wait = 0
                time.sleep(min(60, wait or 5 * (attempt + 1)))
                continue
            raise ExtractError(f"Claude HTTP {e.code}: {e.read().decode()[:200]}") from e
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as e:
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
                continue
            raise ExtractError(f"Claude unreachable: {e}") from e
    raise ExtractError("Claude did not answer")


def _number(v):
    if isinstance(v, bool) or v in (None, ""):
        return None
    try:
        x = float(str(v).replace(",", "").strip())
    except ValueError:
        return None
    return int(x) if x.is_integer() else x


def _clean(raw: dict, c: dict) -> dict:
    """Hold the answer to the requirement types, as Firecrawl's schema did."""
    out = {}
    for f in c["fields"]:
        v, t = raw.get(f["name"]), f["type"]
        if t == "number":
            v = _number(v)
        elif t == "yes/no":
            if not isinstance(v, bool):
                v = str(v).strip().lower() in ("true", "yes") if v not in (None, "") else None
        elif t == "list":
            v = [str(x) for x in v if x not in (None, "")] if isinstance(v, list) else ([str(v)] if v else [])
        elif t == "prices":
            v = [{"item": str(x.get("item") or ""), "price": _number(x.get("price")),
                  "currency": x.get("currency") or None, "unit": x.get("unit") or None}
                 for x in (v if isinstance(v, list) else []) if isinstance(x, dict) and x.get("item")]
        else:
            v = None if v in (None, "") else str(v)
        out[f["name"]] = v
    if c.get("evidence"):
        out["evidence"] = [{"field": str(e.get("field")), "quote": str(e.get("quote"))}
                           for e in (raw.get("evidence") or []) if isinstance(e, dict)]
    return out


def cost(usage: dict) -> float:
    return sum(usage.get(k, 0) * p for k, p in PRICE.items()) / 1e6


def extract(markdown: str, url: str, c: dict, system_text: str | None = None) -> tuple[dict, dict]:
    """(facts, token usage) for one page. Raises ExtractError."""
    user = f"PAGE ({url}):\n{markdown[:PAGE_CHARS]}"
    s = system_text or system(c)
    res = _call(s, user, MAX_TOKENS)
    if res.get("stop_reason") == "max_tokens":           # a very long answer: once more with room
        res = _call(s, user, MAX_TOKENS * 2)
        if res.get("stop_reason") == "max_tokens":
            raise ExtractError("the answer was cut off")
    text = "".join(b.get("text", "") for b in res.get("content", []) if b.get("type") == "text")
    i, j = text.find("{"), text.rfind("}")
    if i < 0 or j <= i:
        raise ExtractError("no JSON in the answer")
    try:
        raw = json.loads(text[i:j + 1])
    except ValueError as e:
        raise ExtractError(f"unreadable JSON: {e}") from e
    usage = {k: int(res.get("usage", {}).get(k) or 0) for k in PRICE}
    return _clean(raw if isinstance(raw, dict) else {}, c), usage
