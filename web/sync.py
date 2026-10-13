"""Copy new jobs from the scraper's rtjobs.db into the public site's store.

Runs next to the scraper (docker-compose.override.yaml `sync` service) and
pushes a trimmed copy of each job: title, company, location, a short excerpt
and the link to the original posting. Full descriptions stay on the server.
Opens rtjobs.db read-only, so the scraper is never blocked or modified.

    python -m web.sync           # loop every SYNC_INTERVAL_SECONDS (60)
    python -m web.sync --once    # one pass, e.g. for the demo seed

Re-running a pass is harmless: rows are upserted by job id.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

from web.store import StoreError, ensure_schema, open_store

SOURCE_DB = os.environ.get("WEB_JOBS_DB") or os.path.join(os.environ.get("DATA_DIR", "."), "rtjobs.db")
INTERVAL = int(os.environ.get("SYNC_INTERVAL_SECONDS", "60"))
RETENTION_DAYS = int(os.environ.get("PUBLIC_RETENTION_DAYS", "45"))
EXCERPT_CHARS = 320
CHUNK = 200
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
TAG_KEYS = ("salary", "workplace", "career_level", "job_type")
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def _json(raw):
    try:
        value = json.loads(raw) if raw else None
    except ValueError:
        return None
    return value


def excerpt(description: str | None) -> str:
    text = _SPACE_RE.sub(" ", html.unescape(_TAG_RE.sub(" ", description or ""))).strip()
    if len(text) <= EXCERPT_CHARS:
        return text
    cut = text[:EXCERPT_CHARS].rsplit(" ", 1)[0]
    return cut + " …"


def _location(extra: dict) -> str:
    for key in ("location", "detail_location", "city"):
        if isinstance(extra.get(key), str) and extra[key].strip():
            return extra[key].strip()
    return ""


def _skills(result_json) -> str | None:
    result = _json(result_json)
    if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
        return None
    names = [t if isinstance(t, str) else t.get("name") for t in result["tools"]
             if isinstance(t, str) or isinstance(t, dict)]
    names = [n for n in names if isinstance(n, str) and n.strip()][:12]
    return json.dumps(names, ensure_ascii=False) if names else None


def public_row(row: sqlite3.Row) -> tuple:
    extra = _json(row["extra"])
    extra = extra if isinstance(extra, dict) else {}
    tags = [extra[k].strip() for k in TAG_KEYS if isinstance(extra.get(k), str) and extra[k].strip()]
    return (row["id"], row["source"], row["title"], row["company"], _location(extra),
            row["posted_at"], row["scraped_at"], row["link"], excerpt(row["description"]),
            json.dumps(tags, ensure_ascii=False) if tags else None,
            row["job_family"], row["specialization"], _skills(row["result_json"]))


UPSERT_JOB = (
    "INSERT OR REPLACE INTO public_jobs(id, source, title, company, location, posted_at, scraped_at,"
    " link, excerpt, tags, job_family, specialization, skills) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
SET_STATE = "INSERT OR REPLACE INTO sync_state(key, value) VALUES (?, ?)"


def _state(store) -> dict:
    return {r["key"]: r["value"] for r in store.query("SELECT key, value FROM sync_state")}


def sync_once(store, source_path: str = SOURCE_DB) -> dict:
    """One pass: new jobs, late classifications, latest run per board, retention."""
    if not os.path.exists(source_path):
        raise FileNotFoundError(source_path)
    state = _state(store)
    last_id = int(state.get("last_job_id") or 0)
    # Classification cursor "updated_at|job_id": the worker publishes a whole
    # batch in the same second, so the time alone can't mark where we stopped.
    cur_at, _, cur_id = (state.get("enriched_cursor") or "|0").partition("|")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime(TS_FORMAT)
    src = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True, timeout=10)
    src.row_factory = sqlite3.Row
    try:
        src.execute("BEGIN")  # one consistent snapshot for every read below
        newest = src.execute(
            "SELECT updated_at, job_id FROM job_enrichments WHERE state = 'ready'"
            " ORDER BY updated_at DESC, job_id DESC LIMIT 1").fetchone()
        jobs = src.execute(
            "SELECT j.id, j.source, j.title, j.company, j.posted_at, j.scraped_at, j.link,"
            " j.description, j.extra, e.job_family, e.specialization, e.result_json"
            " FROM jobs j LEFT JOIN job_enrichments e ON e.job_id = j.id AND e.state = 'ready'"
            " WHERE j.id > ? AND j.scraped_at >= ? ORDER BY j.id", (last_id, cutoff)).fetchall()
        max_id = src.execute("SELECT COALESCE(MAX(id), 0) FROM jobs").fetchone()[0]
        enriched = src.execute(
            "SELECT job_id, job_family, specialization, result_json, updated_at FROM job_enrichments"
            " WHERE state = 'ready' AND (updated_at > ? OR (updated_at = ? AND job_id > ?))"
            " AND job_id <= ? ORDER BY updated_at, job_id",
            (cur_at, cur_at, int(cur_id or 0), last_id)).fetchall() if last_id else []
        runs = src.execute(
            "SELECT r.source, r.status, r.jobs_found, COALESCE(r.finished_at, r.started_at) finished_at"
            " FROM runs r JOIN (SELECT source, MAX(id) id FROM runs GROUP BY source) last"
            " ON last.id = r.id").fetchall()
    finally:
        src.close()

    for i in range(0, len(jobs), CHUNK):
        chunk = jobs[i:i + CHUNK]
        store.batch([(UPSERT_JOB, public_row(r)) for r in chunk]
                    + [(SET_STATE, ("last_job_id", str(chunk[-1]["id"])))])
    if max_id > max([last_id] + [r["id"] for r in jobs]):
        # Everything newer was older than the retention window: skip past it.
        store.batch([(SET_STATE, ("last_job_id", str(max_id)))])
    for i in range(0, len(enriched), CHUNK):
        chunk = enriched[i:i + CHUNK]
        store.batch([("UPDATE public_jobs SET job_family = ?, specialization = ?, skills = ? WHERE id = ?",
                      (r["job_family"], r["specialization"], _skills(r["result_json"]), r["job_id"]))
                     for r in chunk]
                    + [(SET_STATE, ("enriched_cursor", f"{chunk[-1]['updated_at']}|{chunk[-1]['job_id']}"))])
    if not last_id and jobs and newest:
        # First pass: the join above already carried every current classification.
        store.batch([(SET_STATE, ("enriched_cursor", f"{newest['updated_at']}|{newest['job_id']}"))])
    store.batch([("INSERT OR REPLACE INTO public_runs(source, status, jobs_found, finished_at)"
                  " VALUES (?,?,?,?)", (r["source"], r["status"], r["jobs_found"], r["finished_at"]))
                 for r in runs]
                + [("DELETE FROM public_jobs WHERE scraped_at < ?", (cutoff,))])
    return {"jobs": len(jobs), "classified": len(enriched), "runs": len(runs)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run one pass and exit")
    args = parser.parse_args(argv)
    store = open_store()
    while True:
        try:
            ensure_schema(store)
            done = sync_once(store)
            if done["jobs"] or done["classified"] or args.once:
                print(f"sync: {done['jobs']} new jobs, {done['classified']} classified", flush=True)
        except (StoreError, sqlite3.Error, FileNotFoundError) as exc:
            print(f"sync failed (retrying in {INTERVAL}s): {exc}", file=sys.stderr, flush=True)
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
