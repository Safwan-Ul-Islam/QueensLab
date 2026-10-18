"""Plain-English questions -> SQL written by Claude -> run on the real data.

Claude only WRITES the query. The answer is that query's result over the
stored tables, shown next to the SQL it ran — so every number can be traced,
and nothing is made up ("Math decides. AI only explains").

Three scopes, the Query box's three buttons:
    free       the public tables only
    firecrawl  the crawled website data only
    mixed      both, the public tables under their own names

Costs money (Anthropic API), so the web endpoint needs the panel password,
and a daily cap stops a runaway. Key: ANTHROPIC_API_KEY (or QUEENSLAB_ANTHROPIC_KEY).
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gcs  # noqa: E402,F401  (loads .env)
import sources as S  # noqa: E402

API = "https://api.anthropic.com/v1/messages"
MODEL = os.environ.get("ASK_MODEL") or "claude-sonnet-5-5"
KEY_NAMES = ("QUEENSLAB_ANTHROPIC_KEY", "ANTHROPIC_API_KEY")
DAILY_LIMIT = int(os.environ.get("ASK_DAILY_LIMIT") or 300)
ROW_LIMIT = 200

_count = {"day": None, "n": 0}
_lock = threading.Lock()
_schemas: dict = {}          # scope -> (built at, text)


class AskError(Exception):
    pass


# Claude is DISCONNECTED from QueensLab (user, 2026-10-18): the key is shared with
# BizGenome, so the key itself stays live and QueensLab simply never uses it.
# Every Claude call here (Query-box chat, website fact-reading) goes through _key().
# To reconnect: set QUEENSLAB_CLAUDE=on (Render env / .env), or change the default below.
CLAUDE_ON = (os.environ.get("QUEENSLAB_CLAUDE") or "off").strip().lower() == "on"
OFF_MESSAGE = "Claude is switched off for QueensLab for now (QUEENSLAB_CLAUDE=off)"


def has_key() -> bool:
    return CLAUDE_ON and any(os.environ.get(n) for n in KEY_NAMES)


def _key() -> str:
    if not CLAUDE_ON:
        raise AskError(OFF_MESSAGE)
    k = next((os.environ[n] for n in KEY_NAMES if os.environ.get(n)), None)
    if not k:
        raise AskError("no Anthropic key on this server (ANTHROPIC_API_KEY in Render → Environment)")
    return k.strip()


GUIDE = """You write ONE DuckDB SQL SELECT query that answers a question about Queens, New York,
using only the tables listed below. Rules:
- DuckDB dialect. Exactly one SELECT (WITH is fine). Never INSERT/UPDATE/COPY/ATTACH/PRAGMA.
- Never call query(), query_table(), read_text(), read_csv(), read_parquet(), glob() or duckdb_secrets().
- Most columns pulled from city datasets are TEXT even when they hold numbers or dates:
  use TRY_CAST(x AS DOUBLE), TRY_CAST(x AS DATE), TRY_CAST(x AS TIMESTAMP).
- Neighbourhoods: the 82 2020 NTAs (codes like QN0101). Their names and per-resident figures are in
  nta_profile (code, name). DCWP `nta` and nta_population use OLD 2010 codes (like QN61) that do NOT
  match 2020 codes — never join those to nta_profile.code.
- Prefer nta_profile for neighbourhood comparisons: it already has per-10k and per-1k rates. For any
  "per resident / per capita" ranking use those *_per_10k / *_per_1k columns (they are NULL where fewer
  than 1,000 people live) and keep kind = 'residential' — never divide by tiny populations yourself
  (parks, airports and cemeteries have a handful of residents and would top every ranking).
- Add LIMIT {limit} unless the result is an aggregate with few rows. Give columns readable names.
- If the question cannot be answered from these tables, return empty SQL and say why.
Reply with exactly: <sql>...</sql><why>one plain-English sentence on what the query does</why>"""

CRAWL_GUIDE = """
The crawled website tables (research only):
- web_latest: one row per business read (latest read, linked pages merged).
  Column types: VARCHAR[] lists → use list_contains(col, 'x'), array_to_string(col, ', '), or unnest(col).
  BOOLEAN yes/no fields are TRUE only when the page said so. `prices` is JSON: an array of
  {item, price, currency, unit} → unnest(from_json(prices,
  '[{"item":"VARCHAR","price":"DOUBLE","currency":"VARCHAR","unit":"VARCHAR"}]')) t(p), then p.item, p.price.
  nta2020 joins q.nta_profile.code. `evidence` is JSON quotes per field.
- web_facts: every page sent to Firecrawl: url, title, http_status, facts (JSON text), markdown (the page
  text — search it with ILIKE; NULL with `error` set when Firecrawl could not read the site), fetched_at, run_id, page_kind ('home' or 'follow'), extractor (who filled facts: a
  Claude model name or 'firecrawl'; empty on reads before 2026-10-18, which were Firecrawl).
- site_checks: the free website check, one row per business: status is one of alive, alive_js, blocked,
  dead (domain gone), parked, hijacked (spam), platform (not their own site), error, unreachable;
  nta2020, ntaname, category, website, domain, reason, checked_at.
"""
MIXED_NOTE = """
- The public tables are also available under their own names (nta_profile, overture_places, ...):
  join crawled businesses to neighbourhoods with web_latest.nta2020 = nta_profile.code."""


def _describe(con, prefix: str = "") -> list[str]:
    """table: col1, col2 (TYPE), ... — compact; types shown only when not VARCHAR."""
    where = "table_catalog = 'q'" if prefix else "table_catalog NOT IN ('q', 'system', 'temp')"
    rows = con.execute(f"""SELECT table_name, column_name, data_type FROM information_schema.columns
                           WHERE ({where}) AND table_name NOT LIKE '\\_%' ESCAPE '\\'
                           ORDER BY table_name, ordinal_position""").fetchall()
    tables: dict[str, list[str]] = {}
    for t, c, ty in rows:
        tables.setdefault(t, []).append(c if ty == "VARCHAR" else f"{c} ({ty})")
    notes = {s.key: f"{s.label}. {s.note}".replace("⚠️", "").strip()[:260] for s in S.ALL}
    out = []
    for t, cols in tables.items():
        line = f"{prefix}{t}: {', '.join(cols)}"
        if t in notes:
            line += f"\n    about: {notes[t]}"
        out.append(line)
    return out


SCOPES = ("free", "firecrawl", "mixed")


def schema_text(scope: str, con) -> str:
    hit = _schemas.get(scope)
    if hit and time.time() - hit[0] < 600:
        return hit[1]
    guide = GUIDE.format(limit=ROW_LIMIT)
    if scope == "firecrawl":
        guide += CRAWL_GUIDE
    elif scope == "mixed":
        guide += CRAWL_GUIDE + MIXED_NOTE
    text = guide + "\n\nTABLES:\n" + "\n".join(_describe(con))
    _schemas[scope] = (time.time(), text)
    return text


def _call(system: str, messages: list[dict]) -> tuple[str, dict]:
    body = {"model": MODEL, "max_tokens": 1200,
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": messages}
    req = urllib.request.Request(API, data=json.dumps(body).encode(), method="POST", headers={
        "x-api-key": _key(), "anthropic-version": "2023-06-01", "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            out = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:300]
        if e.code == 401:
            raise AskError("the Anthropic key was rejected") from e
        raise AskError(f"Anthropic API HTTP {e.code}: {detail}") from e
    text = "".join(b.get("text", "") for b in out.get("content", []) if b.get("type") == "text")
    return text, out.get("usage", {})


def _parse(text: str) -> tuple[str, str]:
    sql = re.search(r"<sql>(.*?)</sql>", text, re.S)
    why = re.search(r"<why>(.*?)</why>", text, re.S)
    s = (sql.group(1) if sql else "").strip()
    s = re.sub(r"^```(?:sql)?|```$", "", s.strip(), flags=re.I).strip().rstrip(";")
    return s, (why.group(1).strip() if why else "")


def _spend_one() -> None:
    with _lock:
        today = date.today().isoformat()
        if _count["day"] != today:
            _count.update(day=today, n=0)
        if _count["n"] >= DAILY_LIMIT:
            raise AskError(f"today's limit of {DAILY_LIMIT} questions is reached (ASK_DAILY_LIMIT)")
        _count["n"] += 1


EXPLAIN = """You are now answering the user in a chat about Queens, New York data.
Their message is below, with the SQL that was run for it and the RESULT it returned.

Rules:
- Use ONLY numbers, names and facts that appear in the RESULT. Never invent, estimate or
  recall figures from memory. If the result is empty or does not answer the question, say so
  plainly and suggest what to ask instead.
- Speak to a non-technical founder: lead with the direct answer in 1-2 sentences, then at most
  4 short bullet points ("- ") with what stands out. Mention a caveat only if it matters
  (e.g. a figure is per 10,000 residents, a column only covers businesses read so far).
- No SQL, no column names in snake_case — say what they mean.
- Then suggest exactly 3 short follow-up questions the user could ask next, answerable from
  the tables you know.

Reply exactly as: <answer>...</answer><suggest>...</suggest><suggest>...</suggest><suggest>...</suggest>"""

NO_DATA = """You are answering the user in a chat about Queens, New York data. No query was run for
this message (it was not a data question, or the tables cannot answer it). Answer helpfully
in 1-4 sentences using only what the tables described above contain — what data exists and
how it could answer them. Do NOT state any statistic or number about Queens. Then suggest
exactly 3 questions they could ask that the tables CAN answer.

Reply exactly as: <answer>...</answer><suggest>...</suggest><suggest>...</suggest><suggest>...</suggest>"""


def _result_text(cols: list, rows: list, limit: int = 60) -> str:
    """The result as compact tab-separated text for the explainer."""
    def cell(v):
        v = "" if v is None else str(v)
        return v if len(v) <= 200 else v[:197] + "..."
    lines = ["\t".join(map(str, cols))] + ["\t".join(cell(v) for v in r) for r in rows[:limit]]
    more = f"\n({len(rows) - limit} more rows not shown)" if len(rows) > limit else ""
    return "\n".join(lines) + more


_NUM = re.compile(r"(?<![\w.])[-+]?\$?\d[\d,]*(?:\.\d+)?%?")


_TIME_UNIT = re.compile(r"^\s*(?:-\s*)?(?:months?|years?|weeks?|days?|hours?|minutes?|am\b|pm\b|a\.m\.|p\.m\.)", re.I)


def unsupported_numbers(text: str, cols: list, rows: list, question: str = "") -> list[str]:
    """Numbers in the explanation that the result does not contain.

    Each number is compared with every numeric value in the result, allowing
    the rounding a person would do (to the decimals they wrote, or within
    0.5%). Small counts (0-10, 'top 5') and the row count are allowed."""
    values = []
    for r in rows:
        for v in r:
            try:
                values.append(float(str(v).replace(",", "").replace("$", "").rstrip("%")))
            except (TypeError, ValueError):
                for m in _NUM.findall(str(v) if v is not None else ""):
                    try:
                        values.append(float(m.replace(",", "").replace("$", "").rstrip("%")))
                    except ValueError:
                        pass
    bad = []
    for m in _NUM.finditer(text or ""):
        raw = m.group(0)
        if re.search(r"\bper\s*$", (text or "")[max(0, m.start() - 6):m.start()], re.I):
            continue                      # "per 10,000 residents" is a unit, not a figure
        if _TIME_UNIT.match((text or "")[m.end():m.end() + 12]):
            continue                      # "the last 12 months", "after 10pm": a window, not a figure
        if raw in (question or ""):
            continue                      # the user's own number, repeated back
        n = raw.replace(",", "").replace("$", "").rstrip("%")
        try:
            x = float(n)
        except ValueError:
            continue
        if (x.is_integer() and 0 <= x <= 10) or x == len(rows):
            continue
        decimals = len(n.split(".")[1]) if "." in n else 0
        ok = any(round(v, decimals) == round(x, decimals) or (v and abs(v - x) / abs(v) <= 0.005)
                 or (raw.endswith("%") and 0 < v <= 1 and abs(v * 100 - x) <= 0.5)
                 or (abs(v) >= 1000 and abs(v - x) / abs(v) <= 0.05 and decimals == 0)  # "about 41,000"
                 for v in values)
        if not ok:
            bad.append(raw)
    return bad


def _explain(system: str, question: str, sql: str, cols: list, rows: list, history: list,
             tokens: dict) -> tuple[str, list[str]]:
    context = ""
    if history:
        context = "Earlier in this chat:\n" + "\n".join(
            f"- They asked: {h.get('q', '')[:300]}\n  You answered: {h.get('answer', '')[:400]}" for h in history[-3:]) + "\n\n"
    if sql:
        body = (f"{context}{EXPLAIN}\n\nUSER: {question}\n\nSQL RUN:\n{sql}\n\nRESULT "
                f"({len(rows)} rows):\n{_result_text(cols, rows)}")
    else:
        body = f"{context}{NO_DATA}\n\nUSER: {question}"
    _spend_one()
    text, usage = _call(system, [{"role": "user", "content": body}])
    tokens["input"] += usage.get("input_tokens", 0) + usage.get("cache_creation_input_tokens", 0)
    tokens["output"] += usage.get("output_tokens", 0)
    tokens["cache_read"] += usage.get("cache_read_input_tokens", 0)
    a = re.search(r"<answer>(.*?)</answer>", text, re.S)
    sugg = [x.strip() for x in re.findall(r"<suggest>(.*?)</suggest>", text, re.S) if x.strip()][:3]
    return (a.group(1).strip() if a else text.strip()), sugg


def chat(question: str, scope: str, history: list, describe_con, check, run) -> dict:
    """One chat turn: write the SQL (knowing the conversation so far), run it,
    then explain the RESULT in plain English with 3 follow-up suggestions.
    Numbers in the explanation that the result does not contain are listed in
    `unsupported`, so the page can flag them."""
    history = [h for h in (history or []) if isinstance(h, dict)][-4:]
    out = answer(question, scope, describe_con, check, run, history=history)
    if out.get("error"):
        out.update(answer_text=None, suggestions=[], unsupported=[])
        return out
    system = schema_text(scope, describe_con)
    rows = out.get("rows") or []
    text, sugg = _explain(system, question, out.get("sql") or "", out.get("columns") or [], rows,
                          history, out["tokens"])
    out.update(answer_text=text, suggestions=sugg,
               unsupported=unsupported_numbers(text, out.get("columns") or [], rows, question)
               if out.get("sql") else [])
    return out


def answer(question: str, scope: str, describe_con, check, run, history: list | None = None) -> dict:
    """question -> {sql, why, columns, rows, truncated, tokens, attempts}.

    describe_con: a connection to read the table list from (closed by caller)
    check(sql)   -> None or a reason the SQL is refused (the Query box's own rules)
    run(sql)     -> (columns, rows) or raises with the database's error"""
    question = (question or "").strip()[:1000]
    if not question:
        raise AskError("Type a question first.")
    system = schema_text(scope, describe_con)
    messages = []
    for h in history or []:               # follow-ups ("and in Flushing?") need the thread
        if h.get("q"):
            messages.append({"role": "user", "content": str(h["q"])[:1000]})
            messages.append({"role": "assistant",
                             "content": f"<sql>{h.get('sql') or ''}</sql><why>{str(h.get('answer') or '')[:300]}</why>"})
    messages.append({"role": "user", "content": question})
    tokens = {"input": 0, "output": 0, "cache_read": 0}
    sql = why = ""
    error = None
    for attempt in (1, 2):
        _spend_one()
        text, usage = _call(system, messages)
        tokens["input"] += usage.get("input_tokens", 0) + usage.get("cache_creation_input_tokens", 0)
        tokens["output"] += usage.get("output_tokens", 0)
        tokens["cache_read"] += usage.get("cache_read_input_tokens", 0)
        sql, why = _parse(text)
        if not sql:
            return {"sql": "", "why": why or "Claude could not write a query for that.", "columns": [],
                    "rows": [], "truncated": False, "tokens": tokens, "attempts": attempt, "model": MODEL}
        error = check(sql)
        if not error:
            try:
                cols, rows = run(sql)
                return {"sql": sql, "why": why, "columns": cols, "rows": rows[:ROW_LIMIT],
                        "truncated": len(rows) > ROW_LIMIT, "tokens": tokens, "attempts": attempt,
                        "model": MODEL}
            except Exception as e:  # noqa: BLE001 — sent back to Claude once to fix
                error = str(e)[:400]
        messages += [{"role": "assistant", "content": text},
                     {"role": "user", "content": f"That query failed: {error}\nFix it. Same reply format."}]
    return {"sql": sql, "why": why, "error": f"The query still failed after a retry: {error}",
            "columns": [], "rows": [], "truncated": False, "tokens": tokens, "attempts": 2, "model": MODEL}
