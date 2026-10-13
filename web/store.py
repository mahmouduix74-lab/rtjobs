"""Storage for the public site: a local SQLite file or a Turso database.

The site only reads the denormalized `public_jobs` / `public_runs` tables that
web/sync.py fills from the scraper's database, so the same queries run against
either backend. Turso is reached over its HTTP API (/v2/pipeline) with the
standard library, so the Vercel function needs no extra packages.

    TURSO_DATABASE_URL=libsql://<db>-<org>.turso.io  TURSO_AUTH_TOKEN=...
    PUBLIC_DB=/data/public.db   (used when TURSO_DATABASE_URL is unset)
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS public_jobs (
        id             INTEGER PRIMARY KEY,
        source         TEXT NOT NULL,
        title          TEXT,
        company        TEXT,
        location       TEXT,
        posted_at      TEXT,
        scraped_at     TEXT NOT NULL,
        link           TEXT,
        excerpt        TEXT,
        tags           TEXT,
        job_family     TEXT,
        specialization TEXT,
        skills         TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_public_jobs_scraped ON public_jobs(scraped_at)",
    "CREATE INDEX IF NOT EXISTS idx_public_jobs_family ON public_jobs(job_family)",
    """CREATE TABLE IF NOT EXISTS public_runs (
        source      TEXT PRIMARY KEY,
        status      TEXT,
        jobs_found  INTEGER,
        finished_at TEXT
    )""",
    "CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT)",
]


class StoreError(RuntimeError):
    pass


class SqliteStore:
    def __init__(self, path: str):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def query(self, sql: str, args=()) -> list[dict]:
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(sql, tuple(args)).fetchall()]
        finally:
            conn.close()

    def batch(self, statements: list[tuple[str, tuple]]) -> None:
        """Run the statements in one transaction."""
        conn = self._connect()
        try:
            with conn:
                for sql, args in statements:
                    conn.execute(sql, tuple(args))
        finally:
            conn.close()


def _arg(value) -> dict:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": str(int(value))}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}  # string: no float precision loss
    if isinstance(value, float):
        return {"type": "float", "value": value}
    return {"type": "text", "value": str(value)}


def _cell(cell: dict):
    kind = cell.get("type")
    if kind == "null":
        return None
    if kind == "integer":
        return int(cell["value"])
    if kind == "float":
        return float(cell["value"])
    return cell.get("value")


class TursoStore:
    """Minimal client for Turso's HTTP pipeline API."""

    def __init__(self, url: str, token: str, timeout: float = 15):
        if url.startswith("libsql://"):
            url = "https://" + url[len("libsql://"):]
        self.endpoint = url.rstrip("/") + "/v2/pipeline"
        self.token = token
        self.timeout = timeout

    def _pipeline(self, statements: list[tuple[str, tuple]]) -> list[dict]:
        requests = [{"type": "execute", "stmt": {"sql": sql, "args": [_arg(a) for a in args]}}
                    for sql, args in statements]
        body = json.dumps({"requests": requests + [{"type": "close"}]}).encode()
        req = urllib.request.Request(self.endpoint, data=body, method="POST", headers={
            "Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as res:
                payload = json.load(res)
        except urllib.error.HTTPError as exc:
            raise StoreError(f"Turso HTTP {exc.code}: {exc.read()[:300]!r}") from exc
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            raise StoreError(f"Turso unreachable: {exc}") from exc
        results = []
        for item in payload.get("results", [])[:len(statements)]:
            if item.get("type") != "ok":
                raise StoreError(f"Turso: {(item.get('error') or {}).get('message', item)}")
            results.append(item["response"]["result"])
        return results

    def query(self, sql: str, args=()) -> list[dict]:
        result = self._pipeline([(sql, tuple(args))])[0]
        names = [c.get("name") for c in result["cols"]]
        return [dict(zip(names, (_cell(c) for c in row))) for row in result["rows"]]

    def batch(self, statements: list[tuple[str, tuple]]) -> None:
        """Run the statements in one transaction (one request)."""
        if statements:
            self._pipeline([("BEGIN", ())] + list(statements) + [("COMMIT", ())])


def open_store():
    url = os.environ.get("TURSO_DATABASE_URL", "").strip()
    if url:
        return TursoStore(url, os.environ.get("TURSO_AUTH_TOKEN", "").strip())
    path = os.environ.get("PUBLIC_DB") or os.path.join(os.environ.get("DATA_DIR", "."), "public.db")
    return SqliteStore(path)


def ensure_schema(store) -> None:
    store.batch([(sql, ()) for sql in SCHEMA])
