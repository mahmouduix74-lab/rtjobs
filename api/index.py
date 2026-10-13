"""Vercel entrypoint for the public site.

With TURSO_DATABASE_URL / TURSO_AUTH_TOKEN set (Vercel → Settings →
Environment Variables) it serves the jobs web/sync.py publishes from the
scraper server. Without them it is a demo: each cold start seeds fake jobs
into /tmp, the only writable path on Vercel, which is wiped when the instance
is recycled. The scraper itself can't run on Vercel.
"""

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# Vercel (AWS Lambda) sets TZ=":UTC", which zoneinfo rejects; show Cairo time
# like docker-compose does.
if os.environ.get("TZ", "").startswith(":"):
    os.environ["TZ"] = "Africa/Cairo"
    time.tzset()

if not os.environ.get("TURSO_DATABASE_URL"):
    os.environ.setdefault("DATA_DIR", "/tmp/rtjobs-demo")
    os.makedirs(os.environ["DATA_DIR"], exist_ok=True)
    if not os.path.exists(os.path.join(os.environ["DATA_DIR"], "public.db")):
        # config.py insists on these at import; the seeder never sends anything.
        os.environ.setdefault("TELEGRAM_TOKEN", "demo")
        os.environ.setdefault("TELEGRAM_CHAT_ID", "demo")
        from web import demo_seed, sync
        from web.store import ensure_schema, open_store

        demo_seed.main()
        store = open_store()
        ensure_schema(store)
        sync.sync_once(store, os.path.join(os.environ["DATA_DIR"], "rtjobs.db"))

from web.app import app  # noqa: E402

__all__ = ["app"]
