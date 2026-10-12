"""Personal web UI over the scraper's SQLite database.

Reads `jobs` / `job_enrichments` / `runs` from rtjobs.db and never writes to
it. Personal state (saved / applied / hidden, notes) lives in a separate
web.db attached to each connection, so the scraper's schema and its
rollbacks are untouched.

Run:  DATA_DIR=/data uvicorn web.app:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

DATA_DIR = os.environ.get("DATA_DIR", ".")
DB_PATH = os.environ.get("WEB_JOBS_DB", os.path.join(DATA_DIR, "rtjobs.db"))
WEB_DB_PATH = os.environ.get("WEB_DB", os.path.join(DATA_DIR, "web.db"))
PASSWORD = os.environ.get("WEB_PASSWORD", "")
LOCAL_TZ = ZoneInfo(os.environ.get("TZ") or "Africa/Cairo")
PAGE_SIZE = 40
STATUSES = ("saved", "applied", "hidden")
SOURCES = ("linkedin", "wuzzuf", "indeed", "naukrigulf")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

HERE = Path(__file__).parent
TAXONOMY_PATH = Path(os.environ.get(
    "WEB_TAXONOMY", HERE.parent / "markup" / "enrichment" / "taxonomy.json"))


def _load_families() -> dict[str, str]:
    try:
        return json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))["job_families"]
    except (OSError, ValueError, KeyError):
        return {}


FAMILIES = _load_families()

_WEB_SCHEMA = """
CREATE TABLE IF NOT EXISTS w.marks (
    job_id     INTEGER PRIMARY KEY,
    status     TEXT,
    note       TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS w.idx_marks_status ON marks(status);
"""


@contextmanager
def db():
    if not os.path.exists(DB_PATH):
        raise HTTPException(503, f"قاعدة البيانات مش موجودة: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("ATTACH DATABASE ? AS w", (WEB_DB_PATH,))
        conn.executescript(_WEB_SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_ts(value: str | None) -> datetime | None:
    try:
        return datetime.strptime(value[:19], TS_FORMAT) if value else None
    except ValueError:
        return None


def ago(value: str | None) -> str:
    """Relative Arabic label for a stored UTC timestamp."""
    ts = parse_ts(value)
    if ts is None:
        return ""
    minutes = int((utcnow() - ts).total_seconds() // 60)
    if minutes < 1:
        return "دلوقتي"
    for size, one, two, few, many in ((24 * 60, "يوم", "يومين", "أيام", "يوم"),
                                      (60, "ساعة", "ساعتين", "ساعات", "ساعة"),
                                      (1, "دقيقة", "دقيقتين", "دقايق", "دقيقة")):
        n = minutes // size
        if n:
            if n == 1:
                return f"من {one}"
            if n == 2:
                return f"من {two}"
            return f"من {n} {few if n <= 10 else many}"
    return ""


def local_time(value: str | None) -> str:
    ts = parse_ts(value)
    if ts is None:
        return ""
    return ts.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")


def location(extra: dict) -> str:
    for key in ("location", "detail_location", "city"):
        if isinstance(extra.get(key), str) and extra[key].strip():
            return extra[key].strip()
    return ""


def decode_extra(raw: str | None) -> dict:
    try:
        value = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def job_view(row: sqlite3.Row) -> dict:
    job = dict(row)
    job["extra"] = decode_extra(job.get("extra"))
    job["location"] = location(job["extra"])
    job["family_label"] = FAMILIES.get(job.get("job_family") or "", "")
    job["ago"] = ago(job.get("scraped_at"))
    return job


app = FastAPI(title="RTJobs Web", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
templates = Jinja2Templates(directory=HERE / "templates")
templates.env.filters["local_time"] = local_time
templates.env.globals.update(families=FAMILIES, sources=SOURCES)


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    """Optional single-user password (WEB_PASSWORD); any username works."""
    if PASSWORD:
        header = request.headers.get("authorization", "")
        supplied = ""
        if header.lower().startswith("basic "):
            try:
                supplied = base64.b64decode(header[6:]).decode().split(":", 1)[1]
            except (ValueError, IndexError):
                supplied = ""
        if not secrets.compare_digest(supplied.encode(), PASSWORD.encode()):
            return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="RTJobs"'})
    return await call_next(request)


def _filters(params) -> tuple[str, list, dict]:
    """WHERE clause + args for the list page, and the normalized filter values."""
    f = {
        "q": params.get("q", "").strip(),
        "x": params.get("x", "").strip(),
        "source": params.get("source", "") if params.get("source") in SOURCES else "",
        "family": params.get("family", "") if params.get("family") in FAMILIES else "",
        "status": params.get("status", ""),
        "days": params.get("days", ""),
    }
    where, args = ["1=1"], []
    for word in f["q"].split():
        where.append("(j.title LIKE ? OR j.company LIKE ? OR j.description LIKE ?)")
        args += [f"%{word}%"] * 3
    for word in f["x"].split():
        where.append("NOT (COALESCE(j.title,'') LIKE ? OR COALESCE(j.company,'') LIKE ?)")
        args += [f"%{word}%"] * 2
    if f["source"]:
        where.append("j.source = ?")
        args.append(f["source"])
    if f["family"]:
        where.append("e.job_family = ?")
        args.append(f["family"])
    if f["status"] in STATUSES:
        where.append("m.status = ?")
        args.append(f["status"])
    elif f["status"] == "new":
        where.append("m.status IS NULL")
    else:
        f["status"] = ""
        where.append("COALESCE(m.status, '') != 'hidden'")
    if f["days"] in ("1", "3", "7", "30"):
        where.append("j.scraped_at >= ?")
        args.append((utcnow() - timedelta(days=int(f["days"]))).strftime(TS_FORMAT))
    else:
        f["days"] = ""
    return " AND ".join(where), args, f


_JOIN = """
FROM jobs j
LEFT JOIN job_enrichments e ON e.job_id = j.id AND e.state = 'ready'
LEFT JOIN w.marks m ON m.job_id = j.id
"""


@app.get("/", response_class=HTMLResponse)
def index(request: Request, page: int = 1):
    where, args, f = _filters(request.query_params)
    page = max(page, 1)
    with db() as conn:
        total = conn.execute(f"SELECT COUNT(*) {_JOIN} WHERE {where}", args).fetchone()[0]
        rows = conn.execute(
            f"SELECT j.id, j.source, j.title, j.company, j.posted_at, j.scraped_at, j.link,"
            f" j.extra, e.job_family, e.specialization, m.status {_JOIN}"
            f" WHERE {where} ORDER BY j.id DESC LIMIT ? OFFSET ?",
            args + [PAGE_SIZE, (page - 1) * PAGE_SIZE]).fetchall()
        latest = conn.execute("SELECT COALESCE(MAX(id), 0) FROM jobs").fetchone()[0]
        counts = dict(conn.execute(
            "SELECT status, COUNT(*) FROM w.marks WHERE status IS NOT NULL GROUP BY status").fetchall())
    query = {k: v for k, v in f.items() if v}
    return templates.TemplateResponse(request, "index.html", {
        "jobs": [job_view(r) for r in rows], "f": f, "total": total, "page": page,
        "pages": max(1, -(-total // PAGE_SIZE)), "latest": latest, "counts": counts,
        "query": query,
    })


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int):
    with db() as conn:
        row = conn.execute(
            f"SELECT j.*, e.job_family, e.specialization, e.employer_sector, e.result_json,"
            f" m.status, m.note {_JOIN} WHERE j.id = ?", (job_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "الوظيفة مش موجودة")
    job = job_view(row)
    try:
        job["enrichment"] = json.loads(job.get("result_json") or "{}")
    except ValueError:
        job["enrichment"] = {}
    return templates.TemplateResponse(request, "job.html", {"job": job})


class Mark(BaseModel):
    status: str | None = None
    note: str | None = None


@app.post("/api/jobs/{job_id}/mark")
def mark(job_id: int, body: Mark):
    if body.status is not None and body.status not in STATUSES + ("",):
        raise HTTPException(400, "status غلط")
    with db() as conn:
        if conn.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,)).fetchone() is None:
            raise HTTPException(404, "الوظيفة مش موجودة")
        current = conn.execute("SELECT status, note FROM w.marks WHERE job_id = ?", (job_id,)).fetchone()
        status = current["status"] if current else None
        note = current["note"] if current else ""
        if body.status is not None:
            status = body.status or None
        if body.note is not None:
            note = body.note[:5000]
        conn.execute(
            "INSERT INTO w.marks(job_id, status, note, updated_at) VALUES (?,?,?,?)"
            " ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, note=excluded.note,"
            " updated_at=excluded.updated_at",
            (job_id, status, note, utcnow().strftime(TS_FORMAT)))
    return {"job_id": job_id, "status": status, "note": note}


@app.get("/api/latest")
def latest(since: int = 0):
    """Newest job id and how many arrived after `since` (the list page polls this)."""
    with db() as conn:
        top, new = conn.execute(
            "SELECT COALESCE(MAX(id), 0), COUNT(*) FILTER (WHERE id > ?) FROM jobs", (since,)).fetchone()
    return JSONResponse({"latest": top, "new": new})


@app.get("/stats", response_class=HTMLResponse)
def stats(request: Request):
    now = utcnow()
    since_14 = (now - timedelta(days=14)).strftime(TS_FORMAT)
    since_30 = (now - timedelta(days=30)).strftime(TS_FORMAT)
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        today_start = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        today = conn.execute("SELECT COUNT(*) FROM jobs WHERE scraped_at >= ?", (
            today_start.astimezone(timezone.utc).strftime(TS_FORMAT),)).fetchone()[0]
        per_day = {}
        for row in conn.execute("SELECT scraped_at FROM jobs WHERE scraped_at >= ?", (since_14,)):
            day = local_time(row[0])[:10]
            per_day[day] = per_day.get(day, 0) + 1
        by_source = conn.execute(
            "SELECT source, COUNT(*) n FROM jobs WHERE scraped_at >= ? GROUP BY source ORDER BY n DESC",
            (since_30,)).fetchall()
        companies = conn.execute(
            "SELECT company, COUNT(*) n FROM jobs WHERE scraped_at >= ? AND COALESCE(company,'') != ''"
            " GROUP BY company ORDER BY n DESC LIMIT 15", (since_30,)).fetchall()
        families = conn.execute(
            "SELECT e.job_family, COUNT(*) n FROM jobs j JOIN job_enrichments e ON e.job_id = j.id"
            " WHERE e.state = 'ready' AND j.scraped_at >= ? GROUP BY e.job_family ORDER BY n DESC",
            (since_30,)).fetchall()
        runs = conn.execute(
            "SELECT r.source, r.status, r.jobs_found, r.error, r.started_at, r.finished_at FROM runs r"
            " JOIN (SELECT source, MAX(id) id FROM runs GROUP BY source) last ON last.id = r.id"
            " ORDER BY r.source").fetchall()
        marks = dict(conn.execute(
            "SELECT status, COUNT(*) FROM w.marks WHERE status IS NOT NULL GROUP BY status").fetchall())
    days = [(now - timedelta(days=i)).replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
            for i in range(13, -1, -1)]
    series = [(d, per_day.get(d, 0)) for d in days]
    return templates.TemplateResponse(request, "stats.html", {
        "total": total, "today": today, "series": series,
        "series_max": max([n for _, n in series] + [1]),
        "by_source": by_source, "companies": companies,
        "families": [(FAMILIES.get(r[0], r[0]), r[1]) for r in families],
        "runs": [dict(r) | {"ago": ago(r["finished_at"] or r["started_at"])} for r in runs],
        "marks": marks,
    })
