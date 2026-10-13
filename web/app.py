"""Public job board over the jobs web/sync.py publishes.

Reads only the `public_jobs` / `public_runs` tables in the store (a local
SQLite file or Turso, see web/store.py), so it runs anywhere, including
Vercel. Visitors' saved / applied / hidden marks and notes live in their own
browser (localStorage); the server keeps no per-user state.

Run:  PUBLIC_DB=/data/public.db uvicorn web.app:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web.store import StoreError, open_store

PASSWORD = os.environ.get("WEB_PASSWORD", "")
LOCAL_TZ = ZoneInfo(os.environ.get("TZ") or "Africa/Cairo")
PAGE_SIZE = 40
MAX_IDS = 300
STATUSES = ("saved", "applied", "hidden")
SOURCES = ("linkedin", "wuzzuf", "indeed", "naukrigulf")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
# Lets Vercel's CDN answer repeat visits for a minute instead of the database.
CACHE = "public, max-age=0, s-maxage=60, stale-while-revalidate=300"

HERE = Path(__file__).parent
TAXONOMY_PATH = Path(os.environ.get(
    "WEB_TAXONOMY", HERE.parent / "markup" / "enrichment" / "taxonomy.json"))


def _load_families() -> dict[str, str]:
    try:
        return json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))["job_families"]
    except (OSError, ValueError, KeyError):
        return {}


FAMILIES = _load_families()
store = open_store()


def query(sql: str, args=()) -> list[dict]:
    try:
        return store.query(sql, args)
    except (StoreError, OSError, sqlite3.Error) as exc:
        raise HTTPException(503, "قاعدة البيانات مش متاحة دلوقتي، جرّب كمان شوية") from exc


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


def _json_list(raw: str | None) -> list:
    try:
        value = json.loads(raw) if raw else []
    except ValueError:
        return []
    return value if isinstance(value, list) else []


def job_view(row: dict) -> dict:
    job = dict(row)
    job["tags"] = _json_list(job.get("tags"))
    job["skills"] = _json_list(job.get("skills"))
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
    """Optional site-wide password (WEB_PASSWORD); unset = public."""
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
    response = await call_next(request)
    if not PASSWORD and request.method == "GET" and response.status_code == 200 \
            and "cache-control" not in response.headers:
        response.headers["Cache-Control"] = CACHE
    return response


def _ids(raw: str) -> list[int]:
    ids = []
    for part in raw.split(","):
        if part.strip().isdigit():
            ids.append(int(part))
    return list(dict.fromkeys(ids))[:MAX_IDS]


def _filters(params) -> tuple[str, list, dict]:
    """WHERE clause + args for the list page, and the normalized filter values."""
    f = {
        "q": params.get("q", "").strip()[:200],
        "x": params.get("x", "").strip()[:200],
        "source": params.get("source", "") if params.get("source") in SOURCES else "",
        "family": params.get("family", "") if params.get("family") in FAMILIES else "",
        "status": params.get("status", "") if params.get("status") in STATUSES else "",
        "days": params.get("days", "") if params.get("days") in ("1", "3", "7", "30") else "",
        "ids": "",
    }
    where, args = ["1=1"], []
    for word in f["q"].split()[:8]:
        where.append("(title LIKE ? OR company LIKE ? OR excerpt LIKE ?)")
        args += [f"%{word}%"] * 3
    for word in f["x"].split()[:8]:
        where.append("NOT (COALESCE(title,'') LIKE ? OR COALESCE(company,'') LIKE ?)")
        args += [f"%{word}%"] * 2
    if f["source"]:
        where.append("source = ?")
        args.append(f["source"])
    if f["family"]:
        where.append("job_family = ?")
        args.append(f["family"])
    if f["days"]:
        where.append("scraped_at >= ?")
        args.append((utcnow() - timedelta(days=int(f["days"]))).strftime(TS_FORMAT))
    if f["status"]:
        # The visitor's own marks, sent by app.js from localStorage.
        ids = _ids(params.get("ids", ""))
        f["ids"] = ",".join(map(str, ids))
        where.append(f"id IN ({','.join('?' * len(ids))})" if ids else "0")
        args += ids
    return " AND ".join(where), args, f


@app.get("/", response_class=HTMLResponse)
def index(request: Request, page: int = 1):
    where, args, f = _filters(request.query_params)
    page = max(page, 1)
    total = query(f"SELECT COUNT(*) n FROM public_jobs WHERE {where}", args)[0]["n"]
    rows = query(
        f"SELECT id, source, title, company, location, posted_at, scraped_at, link, tags,"
        f" job_family FROM public_jobs WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
        args + [PAGE_SIZE, (page - 1) * PAGE_SIZE])
    latest = query("SELECT COALESCE(MAX(id), 0) n FROM public_jobs")[0]["n"]
    params = {k: v for k, v in f.items() if v}
    return templates.TemplateResponse(request, "index.html", {
        "jobs": [job_view(r) for r in rows], "f": f, "total": total, "page": page,
        "pages": max(1, -(-total // PAGE_SIZE)), "latest": latest, "query": params,
    })


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int):
    rows = query("SELECT * FROM public_jobs WHERE id = ?", (job_id,))
    if not rows:
        raise HTTPException(404, "الوظيفة مش موجودة أو اتشالت")
    return templates.TemplateResponse(request, "job.html", {"job": job_view(rows[0])})


@app.get("/api/latest")
def latest(since: int = 0):
    """Newest job id and how many arrived after `since` (the list page polls this)."""
    row = query("SELECT COALESCE(MAX(id), 0) top, COALESCE(SUM(CASE WHEN id > ? THEN 1 ELSE 0 END), 0) new"
                " FROM public_jobs", (since,))[0]
    return JSONResponse({"latest": row["top"], "new": row["new"]},
                        headers={"Cache-Control": "public, max-age=0, s-maxage=30"})


@app.get("/stats", response_class=HTMLResponse)
def stats(request: Request):
    now = utcnow()
    since_14 = (now - timedelta(days=14)).strftime(TS_FORMAT)
    since_30 = (now - timedelta(days=30)).strftime(TS_FORMAT)
    today_start = datetime.now(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    total = query("SELECT COUNT(*) n FROM public_jobs")[0]["n"]
    today = query("SELECT COUNT(*) n FROM public_jobs WHERE scraped_at >= ?", (
        today_start.astimezone(timezone.utc).strftime(TS_FORMAT),))[0]["n"]
    per_day = {}
    for row in query("SELECT scraped_at FROM public_jobs WHERE scraped_at >= ?", (since_14,)):
        day = local_time(row["scraped_at"])[:10]
        per_day[day] = per_day.get(day, 0) + 1
    by_source = query("SELECT source, COUNT(*) n FROM public_jobs WHERE scraped_at >= ?"
                      " GROUP BY source ORDER BY n DESC", (since_30,))
    companies = query("SELECT company, COUNT(*) n FROM public_jobs WHERE scraped_at >= ?"
                      " AND COALESCE(company,'') != '' GROUP BY company ORDER BY n DESC LIMIT 15",
                      (since_30,))
    families = query("SELECT job_family, COUNT(*) n FROM public_jobs WHERE scraped_at >= ?"
                     " AND job_family IS NOT NULL GROUP BY job_family ORDER BY n DESC", (since_30,))
    runs = query("SELECT source, status, jobs_found, finished_at FROM public_runs ORDER BY source")
    days = [(now - timedelta(days=i)).replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
            for i in range(13, -1, -1)]
    series = [(d, per_day.get(d, 0)) for d in days]
    return templates.TemplateResponse(request, "stats.html", {
        "total": total, "today": today, "series": series,
        "series_max": max([n for _, n in series] + [1]),
        "by_source": by_source, "companies": companies,
        "families": [(FAMILIES.get(r["job_family"], r["job_family"]), r["n"]) for r in families],
        "runs": [r | {"ago": ago(r["finished_at"])} for r in runs],
    })
