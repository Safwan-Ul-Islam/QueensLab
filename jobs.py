"""Collection jobs: what is being pulled, how far it got, and how it ended.

Before this, the only signal was the web app's in-memory `fetching` set. An
empty set meant a job had STOPPED, not that it had SUCCEEDED — a crash looked
exactly like a finish — and a restart erased even that. Now every pull, and
every upload, is a row with an explicit end state:

    queued -> running -> succeeded | failed

Where the rows live:

    jobs.sqlite                         this machine's jobs. SQLite, not DuckDB,
                                        so a pull holding queens.duckdb's write
                                        lock can never hide them.
    gs://<bucket>/jobs/<host>.parquet   one mirror per machine, so the live page
                                        shows pulls run on a laptop, and Render's
                                        own history survives its disk being wiped.

A job that dies without a word (kill -9, out of memory, a laptop going to
sleep) cannot report itself, so its record is closed from outside: its process
is gone (same machine), its runner saw it exit, or it has sent no heartbeat for
STALE seconds. The upload to the bucket is a separate column on purpose: a
table can be saved safely here and still not have reached the bucket.
"""
from __future__ import annotations

import hashlib
import os
import socket
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gcs  # noqa: E402

STORE = HERE / "jobs.sqlite"
KEEP = 200            # newest jobs kept per machine
HEARTBEAT = 30        # seconds between "still alive" stamps
STALE = 300           # active but silent this long = presumed dead
MIRROR_EVERY = 60     # bucket writes driven by heartbeats alone, at most this often
REMOTE_TTL = 20       # seconds other machines' jobs stay cached
ACTIVE = ("queued", "running")
# A job whose work happens elsewhere (a Firecrawl batch): no local process
# beats for it, so silence and a dead pid prove nothing. Its collector closes it.
REMOTE_STEP = "running at Firecrawl"

# Fixed types, so nothing is ever guessed — a guessed timestamp is how a time
# zone crept into the fetch log once already (BizGenome log, 2026-10-17).
SCHEMA = [("id", "VARCHAR"), ("run", "VARCHAR"), ("source", "VARCHAR"),
          ("origin", "VARCHAR"), ("host", "VARCHAR"), ("pid", "BIGINT"),
          ("status", "VARCHAR"), ("step", "VARCHAR"), ("rows_downloaded", "BIGINT"),
          ("queued", "VARCHAR"), ("started", "VARCHAR"), ("last_activity", "VARCHAR"),
          ("finished", "VARCHAR"), ("gcs", "VARCHAR"), ("gcs_detail", "VARCHAR"),
          ("error", "VARCHAR"), ("warnings", "VARCHAR"),
          ("progress_at", "VARCHAR"), ("retries", "BIGINT")]
COLUMNS = [c for c, _ in SCHEMA]

_DDL = """CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, run TEXT, source TEXT NOT NULL, origin TEXT, host TEXT,
    pid INTEGER, status TEXT NOT NULL, step TEXT, rows_downloaded INTEGER NOT NULL DEFAULT 0,
    queued TEXT, started TEXT, last_activity TEXT, finished TEXT,
    gcs TEXT, gcs_detail TEXT, error TEXT, warnings TEXT,
    progress_at TEXT, retries INTEGER NOT NULL DEFAULT 0)"""
# `last_activity` = the last sign of LIFE (heartbeat included).
# `progress_at`   = the last time the job actually got somewhere: rows, a new
#                   step, an upload result. A job retrying a dead server is alive
#                   but not progressing, and the page has to be able to say so.


# ── small helpers ─────────────────────────────────────────────────────────
def now() -> str:
    """ISO 8601 with an explicit +00:00 — a string, so no database converts it."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def age(iso: str | None) -> float | None:
    """Seconds since an ISO time, or None."""
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - t).total_seconds()


MACHINE_ID = HERE / ".machine_id"
_host: str | None = None


def host() -> str:
    """'render' on Render; elsewhere 'laptop-' + a random id kept in
    .machine_id (gitignored). The public page never shows whose computer it is.

    Not the hostname: macOS renames itself when it changes network, and a run
    whose host changed mid-way could no longer find (or close) its own jobs."""
    global _host
    if _host:
        return _host
    if os.environ.get("RENDER"):
        _host = "render"
        return _host
    try:
        mid = MACHINE_ID.read_text().strip()
    except OSError:
        mid = ""
    if not mid:
        # Seeded from the old hostname hash, so jobs already recorded under it
        # stay this machine's after the switch.
        mid = hashlib.sha1(socket.gethostname().encode()).hexdigest()[:6]
        try:
            MACHINE_ID.write_text(mid + "\n")
        except OSError:
            pass
    _host = "laptop-" + mid
    return _host


def prefix() -> str:
    return os.environ.get("GCS_JOBS_PREFIX") or "jobs"


def clean(text) -> str | None:
    """Errors end up on a PUBLIC page: no local paths, no key material."""
    if text is None:
        return None
    s = str(text)
    for secret in (os.environ.get("GCS_HMAC_SECRET"), os.environ.get("GCS_HMAC_KEY_ID")):
        if secret:
            s = s.replace(secret, "[redacted]")
    s = s.replace(str(HERE), "QueensLab").replace(str(Path.home()), "~")
    return s[:500]


def _db() -> sqlite3.Connection:
    c = sqlite3.connect(STORE, timeout=15, isolation_level=None)   # autocommit
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=15000")
    c.execute("PRAGMA journal_mode=WAL")      # readers never block the writer
    c.execute(_DDL)
    have = {r[1] for r in c.execute("PRAGMA table_info(jobs)")}
    for col, ddl in (("progress_at", "TEXT"), ("retries", "INTEGER NOT NULL DEFAULT 0")):
        if col not in have:                   # a store created before these existed
            try:
                c.execute(f"ALTER TABLE jobs ADD COLUMN {col} {ddl}")
            except sqlite3.OperationalError:  # another process added it first
                pass
    return c


def _update(job_id: str, **fields) -> None:
    sets = ", ".join(f"{k}=?" for k in fields)
    with closing(_db()) as c:
        c.execute(f"UPDATE jobs SET {sets} WHERE id=?", [*fields.values(), job_id])


def _alive(pid) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:      # exists, belongs to someone else
        return True
    except (OSError, ValueError):
        return False
    return True


def _close_active(where: str, params: list, why: str, at_last_sign: bool = False) -> int:
    """Fail this machine's still-active jobs matching `where`, with a reason.
    `last_activity` is left alone: it keeps saying when the job was last alive.
    For a job found dead (at_last_sign), Finished is that last sign of life —
    the best estimate of when it stopped — not the moment it was noticed."""
    with closing(_db()) as c:
        cur = c.execute(
            f"""UPDATE jobs SET status='failed', step=NULL,
                       finished=CASE WHEN ? THEN coalesce(last_activity, ?) ELSE ? END,
                       error=coalesce(error, ?),
                       gcs=CASE WHEN gcs='pending' THEN 'skipped' ELSE gcs END
                WHERE host=? AND status IN ('queued','running') AND ({where})""",
            [at_last_sign, now(), now(), why, host(), *params])
        return cur.rowcount


# ── creating jobs ─────────────────────────────────────────────────────────
def new_run() -> str:
    """A run = one invocation (one CLI command or one web request)."""
    return uuid.uuid4().hex[:10]


def queue(source: str, run: str, origin: str) -> str:
    """Record a job as queued. `origin`: cli | web | upload."""
    t = now()
    job_id = f"{t[:19].replace('-', '').replace(':', '')}Z-{source}-{uuid.uuid4().hex[:4]}"
    with closing(_db()) as c:
        c.execute("""INSERT INTO jobs (id, run, source, origin, host, pid, status,
                                       rows_downloaded, queued, last_activity, gcs)
                     VALUES (?,?,?,?,?,?, 'queued', 0, ?, ?, ?)""",
                  [job_id, run, source, origin, host(), os.getpid(), t, t,
                   "pending" if gcs.enabled() else "off"])
        # keep the newest KEEP per machine
        c.execute("""DELETE FROM jobs WHERE host=? AND id NOT IN
                     (SELECT id FROM jobs WHERE host=? ORDER BY queued DESC LIMIT ?)""",
                  [host(), host(), KEEP])
    return job_id


def claim(source: str, run: str, origin: str) -> str:
    """This run's queued job for `source` — the web app queues jobs before it
    starts the process that does them — or else a new one."""
    with closing(_db()) as c:
        r = c.execute("""SELECT id FROM jobs WHERE run=? AND source=? AND host=?
                         AND status='queued' ORDER BY queued LIMIT 1""",
                      [run, source, host()]).fetchone()
        if r:
            c.execute("UPDATE jobs SET pid=?, last_activity=? WHERE id=?",
                      [os.getpid(), now(), r["id"]])
            return r["id"]
    return queue(source, run, origin)


class Job:
    """One source being pulled or uploaded. add_rows is thread-safe: a
    month-by-month pull reports rows from several threads at once."""

    def __init__(self, job_id: str):
        self.id = job_id
        self._rows = 0
        self._lock = threading.Lock()
        self._written = 0.0
        self._ended = False

    def start(self, step: str = "downloading") -> "Job":
        t = now()
        _update(self.id, status="running", step=step, pid=os.getpid(),
                started=t, last_activity=t, progress_at=t)
        mirror(force=True)
        return self

    def add_rows(self, n: int) -> None:
        with self._lock:
            self._rows += n
            rows = self._rows
            due = time.time() - self._written >= 2
            if due:
                self._written = time.time()
        if due:   # max(): two threads' writes can land out of order
            t = now()
            with closing(_db()) as c:
                c.execute("""UPDATE jobs SET rows_downloaded=max(rows_downloaded, ?),
                             last_activity=?, progress_at=? WHERE id=?""", [rows, t, t, self.id])

    def retried(self) -> None:
        """One request had to be asked again. Counted live, so a job stuck
        retrying a struggling server shows it while it is still running."""
        with closing(_db()) as c:
            c.execute("UPDATE jobs SET retries=coalesce(retries, 0) + 1, last_activity=? WHERE id=?",
                      [now(), self.id])

    def step(self, name: str) -> None:
        t = now()
        _update(self.id, step=name, last_activity=t, progress_at=t)
        mirror(force=True)

    def upload(self, status: str, detail=None) -> None:
        """status: uploaded | failed | off (no bucket configured)."""
        t = now()
        _update(self.id, gcs=status, gcs_detail=clean(detail), last_activity=t, progress_at=t)
        mirror(force=True)

    def warn(self, text: str) -> None:
        w = clean(text)
        with closing(_db()) as c:
            c.execute("""UPDATE jobs SET warnings = CASE WHEN warnings IS NULL THEN ?
                         ELSE warnings || ' · ' || ? END WHERE id=?""", [w, w, self.id])

    def succeed(self, rows: int) -> None:
        self._end("succeeded", rows, None)

    def fail(self, error) -> None:
        self._end("failed", None, clean(error) or "failed")

    def _end(self, status: str, rows, error) -> None:
        if self._ended:
            return
        self._ended = True
        t = now()
        with closing(_db()) as c:
            prev = c.execute("SELECT status, error FROM jobs WHERE id=?", [self.id]).fetchone()
            note = None
            if status == "succeeded" and prev and prev["status"] == "failed":
                # Closed from outside while this process was paused (a laptop
                # asleep), and it then finished. It DID succeed; keep the story.
                note = f"recovered: had been marked failed ({prev['error']})"
            c.execute("""UPDATE jobs SET status=?, step=NULL, finished=?, last_activity=?, progress_at=?,
                                rows_downloaded = CASE WHEN ? IS NULL
                                    THEN max(rows_downloaded, ?) ELSE ? END,
                                error = CASE WHEN ? = 'succeeded' THEN NULL ELSE coalesce(?, error) END,
                                warnings = CASE WHEN ? IS NULL THEN warnings
                                    WHEN warnings IS NULL THEN ? ELSE warnings || ' · ' || ? END,
                                gcs = CASE WHEN gcs='pending' THEN 'skipped' ELSE gcs END
                         WHERE id=?""",
                      [status, t, t, t, rows, self._rows, rows, status, error,
                       note, note, note, self.id])
        mirror(force=True)


# ── closing jobs that cannot close themselves ─────────────────────────────
def reap(run: str, code) -> int:
    """A pull process has exited. Whatever of its run is still queued or
    running did NOT finish, whatever the process intended."""
    if code in (-9, 137):
        why = "killed (signal 9) — on the free server that usually means it ran out of memory"
    elif code == "timeout":
        why = "stopped at the 30-minute limit"
    elif isinstance(code, int) and code < 0:
        why = f"killed by signal {-code}"
    else:
        why = f"the process ended (exit {code}) before finishing this"
    n = _close_active("run=?", [run], why)
    if n:
        mirror(force=True)
    return n


def close_run(run: str, why: str) -> int:
    """Fail everything still active in a run that is being stopped."""
    return _close_active("run=?", [run], why)


def cancel_run(run: str, why: str) -> int:
    """Fail the still-queued jobs of a run that is being stopped."""
    return _close_active("run=? AND status='queued'", [run], why)


def reap_dead() -> int:
    """Close this machine's active jobs whose process is gone or silent."""
    me = os.getpid()
    with closing(_db()) as c:
        rows = c.execute("""SELECT id, pid, last_activity FROM jobs
                            WHERE host=? AND status IN ('queued','running')
                              AND coalesce(step, '') <> ?""",
                         [host(), REMOTE_STEP]).fetchall()
    n = 0
    for r in rows:
        if r["pid"] == me:
            continue                                 # ours, and we are running
        if _alive(r["pid"]):
            # Alive but silent is usually a machine that slept: it resumes and
            # finishes. The page shows it as 'stalled' meanwhile (listing).
            continue
        n += _close_active("id=?", [r["id"]],
                           "its process stopped without reporting — killed, crashed, or the machine shut down",
                           at_last_sign=True)
    if n:
        mirror(force=True)
    return n


_beating = False


def heartbeat_forever() -> None:
    """Stamp this process's active jobs every HEARTBEAT seconds while it lives.
    A long step (loading 2 GB into DuckDB) reports no rows, but still beats."""
    global _beating
    if _beating:
        return
    _beating = True

    def beat():
        while True:
            time.sleep(HEARTBEAT)
            try:
                with closing(_db()) as c:
                    c.execute("""UPDATE jobs SET last_activity=? WHERE host=? AND pid=?
                                 AND status IN ('queued','running')""",
                              [now(), host(), os.getpid()])
                mirror()
            except Exception:  # noqa: BLE001 — a missed beat is not worth a crash
                pass

    threading.Thread(target=beat, daemon=True, name="jobs-heartbeat").start()


# ── the bucket mirror ─────────────────────────────────────────────────────
_mirror_lock = threading.Lock()
_mirrored = 0.0
# True while this machine's history in the bucket could not be read back. A
# mirror written now would REPLACE that history with whatever little is local,
# so mirroring waits until a read succeeds (retried on each attempt).
_restore_pending = False


def mine(limit: int = KEEP) -> list[dict]:
    with closing(_db()) as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM jobs WHERE host=? ORDER BY queued DESC LIMIT ?", [host(), limit])]


def mirror(force: bool = False) -> None:
    """Copy this machine's newest jobs to the bucket. Never raises: monitoring
    must not break the work it watches."""
    global _mirrored
    if not gcs.enabled():
        return
    with _mirror_lock:
        if not force and time.time() - _mirrored < MIRROR_EVERY:
            return
        if _restore_pending and not _restore():
            return
        try:
            gcs.write_rows(f"{prefix()}/{host()}.parquet", SCHEMA,
                           [[r.get(c) for c in COLUMNS] for r in mine()])
            _mirrored = time.time()
        except Exception as e:  # noqa: BLE001
            print(f"[jobs] bucket mirror failed: {type(e).__name__}: {clean(e)[:160]}",
                  file=sys.stderr, flush=True)


def mirror_soon() -> None:
    """mirror() off the request thread, for the web app."""
    threading.Thread(target=mirror, kwargs={"force": True}, daemon=True).start()


_remote = {"at": 0.0, "rows": [], "busy": False}
_remote_lock = threading.Lock()


def _refresh_remote() -> None:
    try:
        rows = gcs.read_rows(f"{prefix()}/*.parquet", timeout_s=10)
    except Exception:  # noqa: BLE001 — no mirrors yet, or the bucket is down
        rows = None
    with _remote_lock:
        if rows is not None:
            _remote["rows"] = rows
        _remote["at"] = time.time()          # also after a failure: don't hammer
        _remote["busy"] = False


def others() -> list[dict]:
    """Jobs mirrored by OTHER machines. Cached; refreshed in the background
    so a page load never waits on the bucket (except the very first)."""
    if not gcs.enabled():
        return []
    with _remote_lock:
        first = _remote["at"] == 0.0
        due = time.time() - _remote["at"] >= REMOTE_TTL and not _remote["busy"]
        if due:
            _remote["busy"] = True
    if due:
        if first:
            _refresh_remote()
        else:
            threading.Thread(target=_refresh_remote, daemon=True).start()
    me = host()
    return [dict(r) for r in _remote["rows"] if r.get("host") != me]


# ── reading ───────────────────────────────────────────────────────────────
def listing(limit: int = 30) -> list[dict]:
    """Newest first, all machines. `state` is `status`, except that an active
    job from another machine that has gone silent shows as 'stalled'."""
    reap_dead()
    rows = mine() + others()
    rows.sort(key=lambda j: str(j.get("queued") or ""), reverse=True)
    # Anything still active is shown first, however old: a long pull must not
    # drop off the table just because shorter jobs were queued after it.
    active = [j for j in rows if j.get("status") in ACTIVE]
    rest = [j for j in rows if j.get("status") not in ACTIVE]
    out = []
    for j in (active + rest)[:max(limit, len(active))]:
        j["state"] = j.get("status") or "unknown"
        if (j["state"] in ACTIVE and j.get("step") != REMOTE_STEP
                and (age(j.get("last_activity")) or 0) > STALE):
            j["state"] = "stalled"
        out.append(j)
    return out


def active_here() -> list[dict]:
    reap_dead()
    with closing(_db()) as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM jobs WHERE host=? AND status IN ('queued','running')", [host()])]


def _restore() -> bool:
    """Merge this machine's history from the bucket into the local store.
    True when that worked or there is nothing to restore; False when the
    bucket could not be read (then nothing may be mirrored over it)."""
    global _restore_pending
    try:
        rows = gcs.read_rows(f"{prefix()}/{host()}.parquet", timeout_s=10)
    except Exception as e:  # noqa: BLE001
        if gcs.not_found(e):
            _restore_pending = False
            return True                      # first boot ever: nothing to restore
        _restore_pending = True
        print(f"[jobs] history not readable yet ({type(e).__name__}); not mirroring over it",
              flush=True)
        return False
    marks = ",".join("?" * len(COLUMNS))
    with closing(_db()) as c:
        for r in rows:
            c.execute(f"INSERT OR IGNORE INTO jobs ({','.join(COLUMNS)}) VALUES ({marks})",
                      [r.get(k) for k in COLUMNS])
    _restore_pending = False
    return True


def on_boot() -> None:
    """The web app is starting. On Render the disk is brand new: bring this
    machine's history back from the bucket — and nothing it says was running
    can still be, because that process went with the old disk."""
    try:
        with closing(_db()) as c:
            empty = c.execute("SELECT count(*) FROM jobs WHERE host=?",
                              [host()]).fetchone()[0] == 0
        if empty and gcs.enabled():
            _restore()
            if os.environ.get("RENDER"):
                _close_active("coalesce(step, '') <> ?", [REMOTE_STEP],
                              "the server restarted while this was running "
                              "(idle sleep, a deploy, or running out of memory)",
                              at_last_sign=True)
        reap_dead()
    except Exception as e:  # noqa: BLE001 — never block the page from booting
        print(f"[jobs] history not restored: {type(e).__name__}: {clean(e)}", flush=True)
    finally:
        heartbeat_forever()
        mirror_soon()
