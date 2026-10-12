"""Vercel entrypoint: the web UI over demo data only.

Vercel's filesystem is read-only apart from /tmp, which is wiped whenever the
instance is recycled, so each cold start seeds fresh demo jobs there and any
marks / notes are lost with it. The scraper itself can't run on Vercel.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DATA_DIR", "/tmp/rtjobs-demo")
os.makedirs(os.environ["DATA_DIR"], exist_ok=True)

if not os.path.exists(os.path.join(os.environ["DATA_DIR"], "rtjobs.db")):
    # config.py insists on these at import; the seeder never sends anything.
    os.environ.setdefault("TELEGRAM_TOKEN", "demo")
    os.environ.setdefault("TELEGRAM_CHAT_ID", "demo")
    from web import demo_seed

    demo_seed.main()

from web.app import app  # noqa: E402

__all__ = ["app"]
