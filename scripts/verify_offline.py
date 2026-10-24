"""Offline regression checks using repository fixtures and a temporary SQLite DB.

Run with .venv/bin/python scripts/verify_offline.py. No browser or network.
"""

import asyncio
import copy
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import signal
import tempfile
import threading
import time
from contextlib import ExitStack, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class SignedInContext:
    """Fake browser context holding a LinkedIn session cookie (li_at)."""
    async def cookies(self, url=None):
        return [{"name": "li_at", "value": "offline"}]


class LinkedInApiPage:
    """Fake signed-in LinkedIn page answering the spider's in-page API fetches.

    `cards` answers job-list calls in order (the last answer repeats);
    `postings` maps job id -> answer (or a list, answered in order; missing =
    404). An answer is (status, body[, final_url]) or an exception to raise
    (e.g. the page navigated mid-call). Every requested URL lands in `calls`.
    """
    url = "https://www.linkedin.com/jobs/search-results/?keywords=jobs"

    def __init__(self, cards, postings=None, cookies=None):
        self.cards, self.postings, self.calls = list(cards), dict(postings or {}), []
        jar = cookies if cookies is not None else [{"name": "li_at", "value": "offline"},
                                                    {"name": "JSESSIONID", "value": '"ajax:0000"'}]
        self.context = SimpleNamespace(cookies=AsyncMock(return_value=jar))

    async def evaluate(self, script, args):
        url = args[0]
        self.calls.append(url)
        if "voyagerJobsDashJobCards" in url:
            answer = self.cards.pop(0) if len(self.cards) > 1 else self.cards[0]
        else:
            answer = self.postings.get(url.split("/jobPostings/")[1].split("?")[0], (404, None))
            if isinstance(answer, list):
                answer = answer.pop(0) if len(answer) > 1 else answer[0]
        if isinstance(answer, Exception):
            raise answer
        status, body, final = (*answer, url) if len(answer) == 2 else answer
        return {"status": status, "url": final, "body": body if isinstance(body, str) else json.dumps(body)}


def linkedin_fixture(name: str, **times) -> dict:
    """A synthetic LinkedIn API response (markup/linkedin/api_<name>_sample.json)
    with its "LISTED_1"-style placeholders replaced by epoch milliseconds."""
    text = (ROOT / f"markup/linkedin/api_{name}_sample.json").read_text()
    for key, value in times.items():
        text = text.replace(f'"{key.upper()}"', str(int(value)))
    return json.loads(text)


def minutes_ago_ms(minutes: float) -> int:
    return int((time.time() - minutes * 60) * 1000)


def isolated_markup(directory) -> str:
    """Copy markup/ (minus runtime snapshots) into the test directory.

    The real markup/ is bind-mounted into production as its snapshot store;
    tests that save evidence there would mix fake snapshots with real ones and
    let the 20-per-kind pruning evict genuine production evidence.
    """
    import shutil
    target = Path(directory) / "markup"
    shutil.copytree(ROOT / "markup", target, ignore=shutil.ignore_patterns("snapshots"), dirs_exist_ok=True)
    return str(target)


def main():
    # Broken fixtures deliberately emit errors; show assertions/PASS lines,
    # not simulated production warnings that could be mistaken for a live outage.
    logging.disable(logging.CRITICAL)
    # Production TZ (compose pins it); the UTC migration checks rely on its DST rules.
    os.environ["TZ"] = "Africa/Cairo"
    time.tzset()
    with tempfile.TemporaryDirectory(prefix="rtjobs-tests-") as directory:
        os.environ.update(PYTHON_DOTENV_DISABLED="1", TELEGRAM_TOKEN="offline",
            TELEGRAM_CHAT_ID="-1001", TELEGRAM_FAILURE_CHAT_ID="-1002",
            OPENROUTER_API_KEY="offline", DATA_DIR=directory, MARKUP_DIR=isolated_markup(directory),
            ENRICHMENT_ENABLED="true", CLASSIFIED_DELIVERY_ENABLED="true", TELEGRAM_CHANNELS_JSON="", LOG_FILE="",
            # Never spawn real board processes (they would launch Chrome and hit
            # the live sites); parallel orchestration is tested with fakes.
            BOARDS_PARALLEL="false")
        with patch("requests.sessions.Session.request", side_effect=AssertionError("Network forbidden")):
            verify(directory)
    subprocess.run([sys.executable, str(ROOT / "scripts/verify_pipeline.py")], check=True)
    subprocess.run([sys.executable, str(ROOT / "scripts/verify_wakeup.py")], check=True)
    print("All offline checks passed.")


def verify(directory):
    from core import db, telegram
    from core.browser import patch_no_load_wait
    from core.classify import Enricher, load_channels
    from core.enrichment_worker import process_job
    from core.scrape_health import ScrapeHealth
    from boards.base import load_board_selectors
    from boards.wuzzuf import scraper as wu
    from boards.indeed import scraper as indeed

    # Simulate the deployed pre-enrichment schema and ensure repeat migrations
    # preserve historical jobs and delivery flags without queuing a backfill.
    conn = sqlite3.connect(Path(directory) / "rtjobs.db")
    conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,"
                 " external_id TEXT NOT NULL, title TEXT, company TEXT, posted_at TEXT, description TEXT,"
                 " link TEXT, extra TEXT, scraped_at TEXT, notified INTEGER DEFAULT 0, UNIQUE(source,external_id))")
    conn.execute("INSERT INTO jobs(source,external_id,description,notified) VALUES ('legacy','old','keep me',1)")
    # Pre-UTC rows hold Cairo wall time: summer (+3) and winter (+2) values.
    conn.execute("INSERT INTO jobs(source,external_id,posted_at,scraped_at) VALUES"
                 " ('legacy','summer','2026-08-09 16:48','2026-08-09 16:48'),"
                 " ('legacy','winter','2026-12-01 12:00','2026-12-01 12:00:00')")
    conn.execute("CREATE TABLE pipeline_state (name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute("INSERT INTO pipeline_state VALUES ('classifier',"
                 " '{\"reason\":\"daily_budget_exhausted\",\"pause_until\":\"2026-08-10 00:00:00\"}', '2026-08-09 17:00:00')")
    conn.commit()
    conn.close()
    db.init_db()
    db.init_db()
    with db.get_db() as conn:
        assert conn.execute("SELECT description,notified FROM jobs WHERE external_id='old'").fetchone()[:] == ("keep me", 1)
        assert conn.execute("SELECT count(*) FROM job_enrichments").fetchone()[0] == 0
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        # Converted exactly once (two init_db calls); posted_at stays local.
        rows = {r[0]: r[1:] for r in conn.execute("SELECT external_id,posted_at,scraped_at,next_notify_at FROM jobs")}
        assert rows["summer"] == ("2026-08-09 16:48", "2026-08-09 13:48:00", "")
        assert rows["winter"] == ("2026-12-01 12:00", "2026-12-01 10:00:00", "")
        state = conn.execute("SELECT value,updated_at FROM pipeline_state WHERE name='classifier'").fetchone()
        assert json.loads(state[0])["pause_until"] == "2026-08-09 21:00:00" and state[1] == "2026-08-09 14:00:00"
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        conn.execute("DELETE FROM jobs WHERE external_id IN ('summer','winter')")
        conn.execute("DELETE FROM pipeline_state")
    # The October fallback repeats local 23:00-24:00; stored UTC stays ordered.
    from core import clock
    from datetime import date
    assert clock.to_local("2026-10-29 20:30:00") == clock.to_local("2026-10-29 21:30:00") == "2026-10-29 23:30:00"
    assert clock.next_local_midnight(date(2026, 10, 28)) == "2026-10-28 21:00:00"  # +3 night
    assert clock.next_local_midnight(date(2026, 10, 29)) == "2026-10-29 22:00:00"  # fallback night, +2
    from datetime import datetime
    assert clock.age_seconds("2026-10-29 20:30:00", datetime.fromisoformat("2026-10-29 21:30:00")) == 3600
    print("PASS UTC storage: one-time local->UTC migration, posted_at kept local, DST-safe deadlines")

    raw = (ROOT / "markup/wuzzuf/wazzuf_guide.txt").read_text()
    html = raw[raw.find("<!DOCTYPE"):]
    entities = wu._extract_state(html)
    jobs, dup = wu._extract_jobs(html, load_board_selectors("wuzzuf"), set(), entities)
    assert len(entities) == len(jobs) == 15 and not dup
    by_id = {wu._job_id_from_slug(e["attributes"]["slug"]): e["attributes"] for e in entities.values()}
    for job in jobs:
        attrs = by_id[job["external_id"]]
        assert wu._strip_html(attrs["description"]) in job["description"]
        assert wu._strip_html(attrs["requirements"]) in job["description"]
    assert wu._extract_state('<script>"job":{"collection": invalid}</script>') == {}
    assert db.save_jobs(jobs, [("wuzzuf", "blocked-id")], enqueue=False) == 15
    assert db.save_jobs(jobs, enqueue=True) == 0
    assert db.seen_ids_for("wuzzuf", [jobs[0]["external_id"], "blocked-id", "new-id"]) == {jobs[0]["external_id"], "blocked-id"}
    remaining, dup = wu._extract_jobs(html, load_board_selectors("wuzzuf"), set(), entities, lookup_seen=True)
    assert not remaining and dup
    print("PASS Wuzzuf full requirements, indexed dedupe, atomic inserts, migrations")

    search = (ROOT / "markup/indeed/first_page.html").read_text()
    cards, seen, missing = indeed._extract_jobs(search, set())
    assert len(cards) == 15 and not seen and not missing
    assert all(j["extra"]["description_truncated"] for j in cards)
    data = indeed._extract_balanced_json(search, indeed._CARDS_MARKER)
    source_cards = data["metaData"]["mosaicProviderJobCardsModel"]["results"]
    for job, source in zip(cards, source_cards):
        assert job["posted_at"] == indeed._parse_pubdate(source.get("createDate") or source.get("pubDate"))
    desc, extra = indeed._extract_detail((ROOT / "markup/indeed/newjob_sample.html").read_text())
    assert desc and extra["latitude"] and extra["longitude"]
    assert extra["salary"]["currency"] == "EGP" and extra["salary"]["min"] == 20134.4
    assert extra["job_types"] == ["Contract"]
    live = (ROOT / "markup/indeed/standalone_job_sample.html").read_text()
    desc, extra = indeed._extract_detail(live, "52628dfe51b7fda6")
    assert len(desc) > 1000 and extra["salary"]["max"] == 40000 and extra["country"] == "EG"
    assert extra["job_types"] == ["FULL_TIME", "CONTRACTOR"]
    assert indeed._extract_detail(live, "wrong-job") == ("", {})
    assert indeed._extract_detail('<script type="application/ld+json">{"@type":"Organization","description":"not a job"}</script>') == ("", {})
    _, fixed_pay = indeed._extract_detail('<script type="application/ld+json">'
        '{"@type":"JobPosting","baseSalary":{"currency":"EGP","value":{"value":9000,"unitText":"MONTH"}},'
        '"validThrough":"2026-10-15T12:30:00+03:00"}</script>')
    assert fixed_pay["salary"]["min"] == fixed_pay["salary"]["max"] == 9000
    assert len(fixed_pay["expire_at"]) == 19 and "T" not in fixed_pay["expire_at"]
    # A failed request with no detail callback must retain the search card.
    pending = copy.deepcopy(cards[0])
    fake_spider = SimpleNamespace(_pending={pending["external_id"]: pending}, _logged_out=False,
        _blocked=False, _queued={pending["external_id"]}, health=ScrapeHealth("indeed"),
        start=lambda: SimpleNamespace(items=[], stats=SimpleNamespace(elapsed_seconds=0)))
    fake_spider.health.check("search_fetch", True)
    with patch.object(indeed, "IndeedJobSpider", return_value=fake_spider), patch.object(telegram, "notify_failure", return_value=True) as alert:
        recovered = indeed.scrape({}, "unused")["items"]
        assert alert.call_count == 0, "one failed observation is not yet an episode"
        indeed.scrape({}, "unused")
        assert alert.call_count == 1 and "detail_fetch" in alert.call_args.args[1]
    assert len(recovered) == 1 and recovered[0]["extra"]["detail_status"] == "fetch_failed"
    print("PASS Indeed old/current layouts, salary/types, snippet flags, failed-detail recovery")

    verify_linkedin_api_parsing()

    client = Enricher()
    channels = load_channels()
    assert set(channels) <= set(client.taxonomy["job_families"])
    # Use synthetic complete destinations for mocked delivery checks; the live
    # checker independently reports missing IDs instead of inventing channels.
    channels = {family: "-100" + str(9000000 + i) for i, family in enumerate(client.taxonomy["job_families"])}
    os.environ["TELEGRAM_CHANNELS_JSON"] = json.dumps(channels)
    import core.classify as classify_module
    classify_module.TELEGRAM_CHANNELS_JSON = json.dumps(channels)
    for family in client.taxonomy["job_families"]:
        assert telegram.destination_for({"job_family": family, "employer_sector": "technology_telecom"}, channels) == channels[family]
    base = {"source": "test", "external_id": "one", "title": "Engineer", "company": "Software Ltd",
            "description": "Develop backend software.", "extra": {}, "link": "https://example.com/a(b)", "posted_at": "2026-10-03 12:00"}
    assert db.save_jobs([base], enqueue=True) == 1
    row = dict(db.pending_enrichments(1)[0])
    assert all(r["id"] != row["id"] for r in db.get_unnotified())
    def fake_classify(batch):
        data = batch[0]
        result = client.fallback(data["id"])
        result["classification"].update(job_family="software_engineering", specialization="backend", routing_confidence="High", needs_review=False)
        return [result], {"total_tokens": 100, "cost": 0.001}
    with patch.object(client, "request_bound", return_value=0.01), patch.object(client, "classify_batch", side_effect=fake_classify) as call:
        first = process_job(client, row)
        assert first["state"] == "ready" and not first["cached"]
        assert call.call_count == 1
    base["external_id"] = "two"
    db.save_jobs([base], enqueue=True)
    second_row = dict(db.pending_enrichments(1)[0])
    # A fresh client simulates a worker restart; cache must survive it.
    fresh = Enricher()
    second = process_job(fresh, second_row)
    assert second["cached"] and second["result"]["job_id"] == second_row["id"]
    altered = dict(second_row, description="Different salary and responsibilities")
    assert client.prepare(altered)[1] != client.prepare(second_row)[1]
    invalid = copy.deepcopy(first["result"])
    invalid["classification"]["specialization"] = "pharmacy"
    try:
        client.validate(invalid, row["id"])
    except ValueError:
        pass
    else:
        raise AssertionError("Cross-family specialization accepted")
    assert not db.reserve_enrichment_spend("1900-01-01", 0.02, 0.01)
    assert db.reserve_enrichment_spend("1900-01-01", 0.009, 0.01)
    assert not db.reserve_enrichment_spend("1900-01-01", 0.002, 0.01)
    # Operational errors remain pending even after repeated failures.
    base.update(external_id="three", description="Unique retry job")
    db.save_jobs([base], enqueue=True)
    retry_row = dict(db.pending_enrichments(1)[0])
    with patch.object(client, "request_bound", side_effect=ValueError("provider unavailable")):
        assert process_job(client, retry_row)["state"] == "pending"
        retry_row["attempts"] = 2
        db.set_pipeline_state("classifier", {})
        assert process_job(client, retry_row)["state"] == "pending"
    print("PASS all 26 job-family routes, queue restart/cache, schema validation, budgets, persistent retries")

    payloads = []
    responses = iter([SimpleNamespace(ok=False, status_code=400, text="parse error"), SimpleNamespace(ok=True)])
    def post(_url, **kwargs):
        payloads.append(kwargs["json"])
        return next(responses)
    with patch.object(telegram._SESSION, "post", side_effect=post):
        assert telegram._send("-1001", "bad markdown")
    assert payloads[0]["parse_mode"] == "MarkdownV2" and "parse_mode" not in payloads[1]
    # Idle gaps drop pooled sockets (Telegram closes them); bursts reuse them.
    with patch.object(telegram._SESSION, "close") as reset:
        telegram._last_request = 0.0
        telegram._session(); telegram._session()
        assert reset.call_count == 1
    # The plain retry is readable text, not leftover MarkdownV2 escapes.
    payloads.clear()
    responses = iter([SimpleNamespace(ok=False, status_code=400, text="parse error"), SimpleNamespace(ok=True)])
    with patch.object(telegram._SESSION, "post", side_effect=post):
        assert telegram._send("-1001", "*A\\.B* \\(x\\)", plain="A.B (x)")
    assert payloads[1]["text"] == "A.B (x)"
    assert telegram._unescape_md("Senior \\(C\\+\\+\\) Dev\\.") == "Senior (C++) Dev."
    # A huge exception text still fits Telegram's 4096-character limit.
    sent = []
    with patch.object(telegram, "TELEGRAM_FAILURE_CHAT_ID", "-1002"), \
            patch.object(telegram, "_send", side_effect=lambda chat, text, plain=None: sent.append((text, plain)) or True):
        telegram.notify_failure("Board crashed", "Call log:\n" + "x.(y)" * 5000, "snap/x.html", "hint")
    # Telegram counts the limit after entity parsing (escapes removed).
    assert len(telegram._unescape_md(sent[0][0])) <= 4096 and len(sent[0][1]) <= 4096
    assert sent[0][1].startswith("🚨 Board crashed") and sent[0][1].endswith("🔧 hint")
    delivered = [r for r in db.get_unnotified(limit=100) if r["id"] == row["id"]]
    with patch.object(telegram, "_send", return_value=True) as send, patch.object(telegram.time, "sleep"):
        assert telegram.notify_jobs(delivered) == 1
        assert send.call_args.args[0] == channels["software_engineering"]
        assert "a%28b%29" in send.call_args.args[1]
    with db.get_db() as conn:
        assert conn.execute("SELECT notified FROM jobs WHERE id=?", (row["id"],)).fetchone()[0] == 1
        assert conn.execute("SELECT description FROM jobs WHERE id=?", (row["id"],)).fetchone()[0] == row["description"]
    # A rate-limit wait is not a failed attempt (no backoff, no retries alert).
    db.defer_notification(second_row["id"], minimum_delay=30, count_attempt=False)
    with db.get_db() as conn:
        assert conn.execute("SELECT notify_attempts FROM jobs WHERE id=?", (second_row["id"],)).fetchone()[0] == 0
    db.defer_notification(second_row["id"], minimum_delay=300)
    with db.get_db() as conn:
        assert conn.execute("SELECT notify_attempts FROM jobs WHERE id=?", (second_row["id"],)).fetchone()[0] == 1
    assert all(r["id"] != second_row["id"] for r in db.get_unnotified(limit=100))
    print("PASS delivery acknowledgement, safe links, plaintext retry and persistent backoff")

    class SyncPage:
        def goto(self, url, **kw): return kw
        def wait_for_load_state(self, state=None, **kw): return state
    sync = SyncPage()
    assert patch_no_load_wait(sync) is None
    wrapper = sync.goto
    assert patch_no_load_wait(sync) is None and sync.goto is wrapper
    assert sync.goto("x")["wait_until"] == "domcontentloaded"
    class AsyncPage:
        async def goto(self, url, **kw): return kw
        async def wait_for_load_state(self, state=None, **kw): return state
    async def check():
        page = AsyncPage()
        await patch_no_load_wait(page)
        wrapper = page.goto
        await patch_no_load_wait(page)
        assert page.goto is wrapper
        assert (await page.goto("x"))["wait_until"] == "domcontentloaded"
    asyncio.run(check())
    from verify_classifier import verify_classifier
    verify_classifier(client, channels)
    from verify_analytics import verify_analytics
    verify_analytics(client)
    client.close()
    fresh.close()
    print("PASS sync/async browser patch contract and repeated setup")

    # Parser alerts survive a process restart. Successful checks reset only
    # the checks actually observed; an all-seen page is not a detail recovery.
    from core import markup
    with patch.object(telegram, "notify_failure", return_value=True) as alert, \
            patch.object(markup, "save_snapshot", return_value="test/snapshot.html"):
        def observation(good):
            health = ScrapeHealth("health-test")
            health.check("search_fetch", True)
            health.check("structure", good, "SSR job data missing", "<main>jobs</main>")
            return health
        broken = observation(False)
        broken.check("structure", True)  # a later good page cannot erase a failure
        broken.report()
        assert alert.call_count == 0, "one failed observation is not yet an episode"
        observation(False).report()
        assert alert.call_count == 1 and alert.call_args.args[2] == "test/snapshot.html"
        observation(False).report()
        assert alert.call_count == 1
        empty = ScrapeHealth("health-test")
        empty.check("search_fetch", True)
        empty.job_fields([])
        empty.report()
        observation(False).report()
        assert alert.call_count == 1
        observation(True).report()
        observation(False).report()
        assert alert.call_count == 2
        observation(True).report()
        alert.return_value = False
        observation(False).report()
        alert.return_value = True
        observation(False).report()
        assert alert.call_count == 4  # failed delivery was retried
        no_callback = ScrapeHealth("no-callback")
        no_callback.report(); no_callback.report()
        assert "search_fetch" in alert.call_args.args[1]
        # A one-off failure that recovers stays silent; a flapping check
        # (3 failures within the hour) alerts even without two in a row.
        calls = alert.call_count
        def seen(source, good):
            health = ScrapeHealth(source); health.check("search_fetch", True)
            health.check("structure", good, "list missing"); health.report()
        seen("blip", False); seen("blip", True)
        assert alert.call_count == calls
        seen("blip", False); seen("blip", True); seen("blip", False)
        assert alert.call_count == calls + 1 and "structure" in alert.call_args.args[1]
        # A confirmed episode whose alert never got through (Telegram down in
        # the same outage, Oct 9) is reported on recovery, retried until sent.
        calls = alert.call_count
        alert.return_value = False
        seen("outage", False); seen("outage", False)
        seen("outage", True)
        assert alert.call_count == calls + 2, "the alert was tried, then the recovery note"
        with db.get_db() as conn:
            assert conn.execute("SELECT pending_note FROM scrape_health_episodes WHERE source='outage' AND check_name='structure'").fetchone()[0]
        alert.return_value = True
        quiet = ScrapeHealth("outage"); quiet.check("search_fetch", True); quiet.report()  # check not observed
        assert "failed and recovered" in alert.call_args.args[0] and "structure: failing from" in alert.call_args.args[1]
        with db.get_db() as conn:
            assert not conn.execute("SELECT pending_note FROM scrape_health_episodes WHERE source='outage' AND check_name='structure'").fetchone()[0]
        calls = alert.call_count
        seen("outage", True)
        assert alert.call_count == calls, "the note is sent once"
        # Rollback safety: images before Oct 10 insert into scrape_health
        # positionally (8 values); the schema must keep accepting that.
        with db.get_db() as conn:
            conn.execute("INSERT INTO scrape_health VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(source,check_name) "
                         "DO UPDATE SET failing=excluded.failing",
                         ("outage", "structure", 1, "old image", None, db.now_str(), db.now_str(), None))
    print("PASS parser-health detection, persistent dedupe, recovery and failed-alert retry")

    # Exercise actual browser callbacks against changed markup, not just the
    # alert helper. No browser is launched and no files/messages are produced.
    class FixturePage:
        url = "https://eg.indeed.com/jobs"
        context = SignedInContext()
        def __init__(self, html): self.html = html
        async def wait_for_selector(self, *args, **kwargs): pass
        async def wait_for_timeout(self, *args): pass
        async def title(self): return "Jobs"
        async def content(self): return self.html
    async def changed_markup():
        raw = (ROOT / "markup/wuzzuf/wazzuf_guide.txt").read_text()
        raw = raw[raw.find("<!DOCTYPE"):]
        w_spider = wu.WuzzufJobSpider(load_board_selectors("wuzzuf"), "http://127.0.0.1:1")
        damaged = raw.replace('"job"', '"renamedJob"')
        assert damaged != raw
        await w_spider.scan_page(FixturePage(damaged))
        assert not w_spider.health.checks["ssr_collection"]["good"]
        for _ in range(2):  # two runs see the same broken page: an episode
            w_spider.health.report()
        i_spider = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        await i_spider.scan_search_page(FixturePage(search.replace('"jobkey"', '"changedJobKey"')))
        assert not i_spider.health.checks["card_identity"]["good"]
        for _ in range(2):
            i_spider.health.report()
        # A valid, all-seen Indeed page produces no new jobs and no alarm.
        good = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        with patch.object(db, "seen_ids_for", return_value={j["external_id"] for j in cards}):
            await good.scan_search_page(FixturePage(search))
        assert not good._page_jobs and all(c["good"] for c in good.health.checks.values())
        # The search scan reads the page as soon as the job-cards blob parses
        # (it used to wait a fixed 2.5s); without the blob it waits the old 2.5s.
        class CountingPage(FixturePage):
            def __init__(self, html): super().__init__(html); self.waited = 0
            async def wait_for_timeout(self, ms): self.waited += ms
        quick = CountingPage(search)
        await indeed.IndeedJobSpider({}, "http://127.0.0.1:1")._search_html(quick)
        assert quick.waited == 0
        empty = CountingPage("<html><body>still loading</body></html>")
        await indeed.IndeedJobSpider({}, "http://127.0.0.1:1")._search_html(empty)
        assert empty.waited == 2500
        # Indeed's ;jsessionid auth bounce (HTTP 400 on a valid session) is
        # undone with Back, never treated as a logout (user finding, Oct 2026).
        bounce = ("https://secure.indeed.com/auth;jsessionid=node01abc.node0?hl=en_US&service=my&co=US"
                  "&continue=https%3A%2F%2Fwww.indeed.com%2F&from=bot-detection-anonymous")
        class BouncedPage(FixturePage):
            def __init__(self, html, back_to):
                super().__init__(html); self.url, self.back_to, self.backs = bounce, back_to, 0
            async def go_back(self, **kwargs):
                assert kwargs.get("wait_until") == "domcontentloaded"
                self.backs += 1; self.url = self.back_to
        bounced = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        page = BouncedPage(search, "https://eg.indeed.com/jobs?q=&sort=date")
        with patch.object(db, "seen_ids_for", return_value=set()):
            await bounced.scan_search_page(page)
        assert page.backs == 1 and not bounced._logged_out and bounced._page_jobs
        stays = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        page = BouncedPage(search, bounce)  # Back does not help: a real logout
        await stays.scan_search_page(page)
        assert stays._logged_out
        # "Access Denied" inside job text is not a block page while job cards exist.
        snippet = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        with patch.object(db, "seen_ids_for", return_value=set()):
            await snippet.scan_search_page(FixturePage(search + "<p>Access Denied handling for IAM roles</p>"))
        assert not snippet._blocked and snippet._page_jobs
        blocked_page = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        await blocked_page.scan_search_page(FixturePage("<html><body>Access Denied</body></html>"))
        assert blocked_page._blocked
        # LinkedIn: a job-list response without the fields we read (LinkedIn
        # answers 200 even for a retired decoration) is an API change.
        from boards.linkedin.scraper import LinkedInJobSpider
        changed = LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1")
        await changed.scan_window(LinkedInApiPage([(200, {"data": {"metadata": {}, "elements": []}})]))
        assert not changed.health.checks["search_structure"]["good"] and "API changed" in changed.health.error
        assert changed.health.status == "degraded"
        for _ in range(2):
            changed.health.report()
    with patch.object(telegram, "notify_failure", return_value=True) as alert, \
            patch.object(markup, "save_snapshot", return_value="test/changed.html"):
        asyncio.run(changed_markup())
        assert alert.call_count == 3
    print("PASS changed LinkedIn/Wuzzuf/Indeed markup triggers error-channel alerts; all-seen is normal")

    # Startup failures must close the run record even before a session exists.
    import boards.linkedin as li_board
    import boards.wuzzuf as wu_board
    import boards.indeed as in_board
    import boards.base as board_base
    with ExitStack() as stack:
        stack.enter_context(patch.object(li_board.login_state, "is_blocked", return_value=False))
        stack.enter_context(patch.object(li_board.login_state, "should_wipe_profile", return_value=False))
        for _module, cls in ((li_board, li_board.LinkedInBoard), (wu_board, wu_board.WuzzufBoard), (in_board, in_board.IndeedBoard)):
            with patch.object(board_base, "chrome_session", side_effect=RuntimeError("startup failed")):
                try:
                    cls().run()
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("Chrome launch failure swallowed")
            with db.get_db() as conn:
                row = conn.execute("SELECT status,finished_at FROM runs WHERE source=? ORDER BY id DESC LIMIT 1", (cls.name,)).fetchone()
                assert row[0] == "error" and row[1]
    print("PASS Chrome startup failure audit for all boards")

    # Shared run() template: errors are recorded + alerted (cycle continues),
    # interrupts keep their precise status, skips never start Chrome.
    from core import telegram as tg
    class Probe(board_base.JobBoard):
        name, title = "wuzzuf", "Probe"
        def __init__(self, outcome, skip=None): self.outcome, self.skip = outcome, skip
        def before_browser(self): return self.skip
        def scrape(self, cdp, record): raise self.outcome
    def last_run():
        with db.get_db() as conn:
            return tuple(conn.execute("SELECT status,error FROM runs WHERE source='wuzzuf' ORDER BY id DESC LIMIT 1").fetchone())
    with patch.object(board_base, "chrome_session", side_effect=lambda *a, **kw: nullcontext("cdp")), \
            patch.object(tg, "notify_failure", return_value=True) as alert:
        assert Probe(RuntimeError("boom")).run() == 0
        assert last_run() == ("error", "boom") and alert.call_count == 0, "a single crash is not yet an episode"
        assert Probe(RuntimeError("boom again")).run() == 0
        assert alert.call_count == 1 and "Probe board failed: boom again" in alert.call_args.args[1]
        assert Probe(RuntimeError("boom again")).run() == 0
        assert alert.call_count == 1, "a board failing every run must alert once per episode"
        try:
            Probe(SystemExit(0)).run()
        except SystemExit:
            pass
        assert last_run() == ("interrupted", "terminated by signal")
    with patch.object(board_base, "chrome_session", side_effect=AssertionError("Chrome must not start")):
        assert Probe(RuntimeError("unused"), skip="blocked").run() == 0
        assert last_run()[0] == "blocked"
    class Forgetful(Probe):
        def scrape(self, cdp, record): return 0
    with patch.object(board_base, "chrome_session", side_effect=lambda *a, **kw: nullcontext("cdp")):
        assert Forgetful(None).run() == 0
    assert last_run()[0] == "error" and "without recording" in last_run()[1]
    # Time budget: a stuck board is stopped (timeout, one alert); waits for a
    # person run inside paused() and never count.
    from core import board_budget
    class Slow(Probe):
        def scrape(self, cdp, record):
            time.sleep(2)
            return 0
    class Patient(Probe):
        def scrape(self, cdp, record):
            with board_budget.paused():
                time.sleep(1.2)
            record.finish("ok")
            return 0
    with patch.object(board_base, "chrome_session", side_effect=lambda *a, **kw: nullcontext("cdp")), \
            patch.object(board_base, "BOARD_TIME_BUDGET_SECONDS", 1), \
            patch.object(tg, "notify_failure", return_value=True) as alert:
        started = time.monotonic()
        assert Slow(None).run() == 0 and time.monotonic() - started < 1.8
        assert last_run()[0] == "timeout" and alert.call_count == 0
        assert Slow(None).run() == 0 and alert.call_count == 1 and "time budget" in alert.call_args.args[1]
        assert Slow(None).run() == 0 and alert.call_count == 1
        assert Patient(None).run() == 0 and last_run()[0] == "ok"
        # Jobs are saved the moment they are scraped: a run stopped by its
        # budget keeps them (it used to lose all of them), and finish_scrape()
        # counts already-saved jobs once.
        def probe_job(n):
            return {"source": "wuzzuf", "external_id": f"save-now-{n}", "title": f"T{n}", "company": "C",
                    "posted_at": "2026-10-09 17:00", "description": "d", "link": f"https://x/{n}", "extra": {}}
        class Stopped(Probe):
            def scrape(self, cdp, record):
                self.save_now(probe_job(1)); self.save_now(probe_job(2))
                time.sleep(2)
                return 0
        class Partial(Probe):
            def scrape(self, cdp, record):
                self.save_now(probe_job(3))
                from core.scrape_health import ScrapeHealth
                return self.finish_scrape(record, ScrapeHealth("wuzzuf"), [probe_job(3), probe_job(4)])
        assert Stopped(None).run() == 0
        with db.get_db() as conn:
            row = conn.execute("SELECT status, jobs_found FROM runs WHERE source='wuzzuf' ORDER BY id DESC LIMIT 1").fetchone()
            kept = conn.execute("SELECT COUNT(*) FROM jobs WHERE external_id IN ('save-now-1','save-now-2')").fetchone()[0]
        assert tuple(row) == ("timeout", 2) and kept == 2, (tuple(row), kept)
        assert Partial(None).run() == 2
        with db.get_db() as conn:
            assert conn.execute("SELECT jobs_found FROM runs WHERE source='wuzzuf' ORDER BY id DESC LIMIT 1").fetchone()[0] == 2
    print("PASS board run() template: error alert once per episode, interrupt status kept, skip before Chrome, no leaked running rows, time budget, jobs saved as scraped")

    # A completed browser/spider with failed data checks must record degraded,
    # while retaining valid partial results. Healthy empty/new runs remain ok.
    class LoggedInSession:
        def __init__(self, **kwargs): self.action = kwargs["page_action"]
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def fetch(self, *args, **kwargs): self.action(object())
    with ExitStack() as stack:
        stack.enter_context(patch.object(li_board.login_state, "is_blocked", return_value=False))
        stack.enter_context(patch.object(li_board.login_state, "should_wipe_profile", return_value=False))
        for module, cls in ((li_board, li_board.LinkedInBoard), (wu_board, wu_board.WuzzufBoard), (in_board, in_board.IndeedBoard)):
            for healthy in (False, True):
                # Called within this iteration, so the loop variables are current.
                def fake_scrape(selectors, cdp_url, health, on_job=None):
                    health.check("search_fetch", True)
                    health.check("search_structure", healthy, "Simulated incomplete page")  # noqa: B023
                    items = [dict(base, source=cls.name, external_id="status-test-" + str(healthy))]  # noqa: B023
                    return {"items": items, "listings": {}, "login_redirect": False, "logged_out": False, "blocked": False}
                with ExitStack() as case:
                    case.enter_context(patch.object(board_base, "chrome_session", side_effect=lambda *a, **kw: nullcontext("unused")))
                    case.enter_context(patch.object(module.scraper, "scrape", side_effect=fake_scrape))
                    if cls.name != "wuzzuf":
                        case.enter_context(patch.object(module, "StealthySession", LoggedInSession))
                        case.enter_context(patch.object(module.login, "ensure_logged_in", return_value=True))
                    assert cls().run() == 1
                with db.get_db() as conn:
                    row = conn.execute("SELECT status,jobs_found,error FROM runs WHERE source=? ORDER BY id DESC LIMIT 1", (cls.name,)).fetchone()
                    assert row[0] == ("ok" if healthy else "degraded") and row[1] == 1
                    assert bool(row[2]) == (not healthy)
    print("PASS loading-screen diagnosis and degraded run status with partial data preserved")
    verify_browser_recovery()
    verify_linkedin_login()
    verify_first_page_and_schedule()
    verify_person_wait()
    verify_browser_launch()
    verify_reposts()
    verify_direct_search()
    verify_cloudflare_fast_path()
    verify_clean_quit()
    verify_scrapling_contract_check()
    verify_naukrigulf()


def verify_reposts():
    """Same board ID with a fresh listing = repost: always recorded, re-sent
    (tagged) only after the 7-day cooldown, classified first when needed."""
    from datetime import datetime, timedelta
    from config import ENRICHMENT_SCHEMA_VERSION
    from core import clock, db, telegram
    from boards.base import load_board_selectors
    from boards.indeed import scraper as indeed
    from boards.linkedin import scraper as li
    from boards.wuzzuf import scraper as wu

    def wall(**delta):
        return (datetime.now() - timedelta(**delta)).strftime("%Y-%m-%d %H:%M")
    base = {"source": "linkedin", "title": "Road Logistics Operational Care Specialist", "company": "Kuehne+Nagel",
            "description": "Logistics operations.", "link": "https://www.linkedin.com/jobs/view/1/", "extra": {}}
    db.save_jobs([dict(base, external_id=key, posted_at=wall(days=22))
                  for key in ("repost-legacy", "repost-recent", "repost-ready")], enqueue=False)
    with db.get_db() as conn:
        ids = dict(conn.execute("SELECT external_id, id FROM jobs WHERE external_id LIKE 'repost-%'").fetchall())
        conn.execute("UPDATE jobs SET notified=1, notified_at=NULL WHERE id=?", (ids["repost-legacy"],))
        conn.execute("UPDATE jobs SET notified=1, notified_at=? WHERE id=?", (clock.after(-2 * 86400), ids["repost-recent"]))
        conn.execute("UPDATE jobs SET notified=1, notified_at=?, destination_chat_id='-1009' WHERE id=?",
                     (clock.after(-20 * 86400), ids["repost-ready"]))
        conn.execute("INSERT INTO job_enrichments (job_id, state, schema_version, job_family, result_json, updated_at, created_at)"
                     " VALUES (?, 'ready', ?, 'supply_chain_procurement_logistics', '{}', '2026-09-16 10:00:00', '2026-09-16 10:00:00')",
                     (ids["repost-ready"], ENRICHMENT_SCHEMA_VERSION))
    fresh = wall(minutes=6)
    report = db.record_listings("linkedin", {"repost-legacy": fresh, "repost-recent": fresh,
                                             "repost-ready": fresh, "never-saved": fresh})
    assert report["reposts"] == 3 and sorted(report["requeued"]) == sorted([ids["repost-legacy"], ids["repost-ready"]])
    with db.get_db() as conn:
        legacy = conn.execute("SELECT notified, reposted_at, destination_chat_id FROM jobs WHERE id=?", (ids["repost-legacy"],)).fetchone()
        assert legacy[0] == 0 and legacy[1] == fresh and legacy[2] is None
        # Unclassified (pre-classifier) reposts are classified before delivery.
        assert conn.execute("SELECT state FROM job_enrichments WHERE job_id=?", (ids["repost-legacy"],)).fetchone()[0] == "pending"
        assert tuple(conn.execute("SELECT notified, reposted_at FROM jobs WHERE id=?", (ids["repost-recent"],)).fetchone()) == (1, None)
        events = dict(conn.execute("SELECT job_id, redelivered FROM job_reposts").fetchall())
        assert events == {ids["repost-legacy"]: 1, ids["repost-recent"]: 0, ids["repost-ready"]: 1}
        assert conn.execute("SELECT updated_at FROM job_enrichments WHERE job_id=?", (ids["repost-ready"],)).fetchone()[0] > "2026-10-01"
    # The same repost on the next runs, stale listings and unknown IDs are not new reposts.
    assert db.record_listings("linkedin", {"repost-legacy": wall(minutes=3), "repost-ready": wall(minutes=1)})["reposts"] == 0
    assert db.record_listings("linkedin", {"repost-recent": wall(days=3)})["reposts"] == 0

    # A re-sent repost carries the tag and its first-seen date.
    rows = [row for row in db.get_unnotified(limit=1000) if row["id"] == ids["repost-ready"]]
    assert rows, "a classified repost must be immediately deliverable"
    with patch.object(telegram, "_send", return_value=True) as send, patch.object(telegram.time, "sleep"), \
            patch("core.classify.load_channels", return_value={}):
        assert telegram.notify_jobs(rows) == 1
    assert "🔁" in send.call_args.args[1] and "first seen" in send.call_args.kwargs["plain"]
    assert f"Reposted {fresh}" in send.call_args.kwargs["plain"]

    # Boards report listing times for KNOWN cards only, without fallbacks.
    # LinkedIn: from the job list itself (no extra request per known job).
    spider = li.LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1")
    listed = minutes_ago_ms(6)
    body = linkedin_fixture("cards", listed_1=listed, listed_2=listed, listed_3=listed)
    with patch.object(db, "seen_ids_for", return_value={"4400000001", "4400000003"}), \
            patch.object(li.asyncio, "sleep", AsyncMock()):
        asyncio.run(spider.scan_window(LinkedInApiPage([(200, body)], {"4400000002": (404, None)})))
    assert spider.listings == {"4400000001": li._wall(listed), "4400000003": li._wall(listed)}
    # LinkedIn must never intercept requests: Playwright disables the HTTP cache
    # on routed pages and the ~3 MB app bundle then downloads every run (Oct 9).
    with patch.object(li, "AsyncStealthySession") as session_cls:
        spider.configure_sessions(MagicMock())
        assert session_cls.call_args.kwargs["disable_resources"] is False
    for source in (ROOT / "boards/linkedin").glob("*.py"):
        assert "disable_resources=True" not in source.read_text(), source
    raw = (ROOT / "markup/wuzzuf/wazzuf_guide.txt").read_text()
    html = raw[raw.find("<!DOCTYPE"):]
    sel, entities = load_board_selectors("wuzzuf"), wu._extract_state(html)
    first, _ = wu._extract_jobs(html, sel, set(), entities)
    listings = {}
    jobs, duplicate = wu._extract_jobs(html, sel, {j["external_id"] for j in first}, entities, listings=listings)
    assert not jobs and duplicate and listings == {j["external_id"]: j["posted_at"] for j in first}
    search = (ROOT / "markup/indeed/first_page.html").read_text()
    fresh_cards, _, _ = indeed._extract_jobs(search, set())
    listings = {}
    jobs, seen, missing = indeed._extract_jobs(search, {j["external_id"] for j in fresh_cards}, listings=listings)
    assert not jobs and seen == len(fresh_cards) and listings
    assert all(listings[key] == next(j["posted_at"] for j in fresh_cards if j["external_id"] == key) for key in listings)
    # Sync login check: a ;jsessionid bounce goes Back instead of starting the
    # emailed-code login; a real auth page still counts as logged out.
    from boards.indeed import login as indeed_login
    class SyncBounce:
        def __init__(self, back_to): self.url, self.back_to = "https://secure.indeed.com/auth;jsessionid=x.node0?from=bot-detection-anonymous", back_to
        def go_back(self, **kwargs): self.url = self.back_to
        def wait_for_timeout(self, _ms): pass
        def title(self): return "Jobs in Egypt"
        def content(self): return "<html></html>"
    with patch.object(indeed_login, "_do_login", side_effect=AssertionError("must not start the code login")):
        assert indeed_login.ensure_logged_in(SyncBounce("https://eg.indeed.com/jobs?q="), {})
    with patch.object(indeed_login, "_cooldown_active", return_value=True):
        assert not indeed_login.ensure_logged_in(SyncBounce("https://secure.indeed.com/auth?from=x"), {})
    print("PASS reposts: fresh listing of a known ID recorded, re-sent after the cooldown (classified first), tagged; all boards report listing times")


def verify_browser_launch():
    """CDP origin allow-list, sandbox switch, busy port, group stop, crash stderr."""
    import socket
    import tempfile as tmp
    from core import browser

    args = browser.chrome_args("chrome", "/profile", 9222, no_sandbox=False)
    origins = next(a for a in args if a.startswith("--remote-allow-origins="))
    assert "*" not in origins and "devtools://devtools" in origins and "http://localhost:9222" in origins
    assert "--no-sandbox" not in args and "--no-sandbox" in browser.chrome_args("chrome", "/p", 9222, no_sandbox=True)

    # A port someone else already serves is refused before anything launches.
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0)); busy.listen()
        with patch.object(browser.subprocess, "Popen", side_effect=AssertionError("must not launch")):
            try:
                browser.launch_cdp_chrome(tmp.mkdtemp(), busy.getsockname()[1])
            except RuntimeError as exc:
                assert "already in use" in str(exc)
            else:
                raise AssertionError("busy CDP port accepted")

    # An early exit reports Chrome's own stderr instead of a bare exit code.
    fake = Path(tmp.mkdtemp()) / "chrome"
    fake.write_text("#!/bin/sh\necho 'Failed to move to new namespace' >&2\nexit 5\n"); fake.chmod(0o755)
    with patch.object(browser, "_find_chrome", return_value=str(fake)):
        try:
            browser.launch_cdp_chrome(tmp.mkdtemp(), 1)
        except RuntimeError as exc:
            assert "code 5" in str(exc) and "new namespace" in str(exc), exc
        else:
            raise AssertionError("dead Chrome reported as started")

    # stop_chrome ends the whole process group (renderer/GPU children too).
    group = subprocess.Popen(["sh", "-c", "sleep 60 & sleep 60"], start_new_session=True)
    time.sleep(0.2)
    browser.stop_chrome(group)
    time.sleep(0.2)
    try:
        os.killpg(group.pid, 0)
        raise AssertionError("Chrome child processes survived stop_chrome")
    except ProcessLookupError:
        pass
    # Idempotent, and a reaped leader is never group-signalled (its id may be reused).
    browser.stop_chrome(group)
    reaped = subprocess.Popen(["true"], start_new_session=True)
    reaped.wait()
    with patch.object(browser.os, "killpg", side_effect=AssertionError("signalled a reaped group")):
        browser.stop_chrome(reaped)
        browser._LIVE.add(reaped)
        browser.kill_live_chrome()
    # A forced exit kills every Chrome group still registered as live.
    group = subprocess.Popen(["sh", "-c", "sleep 60 & sleep 60"], start_new_session=True)
    browser._LIVE.add(group)
    time.sleep(0.2)
    browser.kill_live_chrome()
    group.wait(5)
    time.sleep(0.2)
    try:
        os.killpg(group.pid, 0)
        raise AssertionError("kill_live_chrome left Chrome children running")
    except ProcessLookupError:
        pass
    assert not browser._LIVE
    print("PASS Chrome launch: origin allow-list, sandbox switch, busy port, crash stderr, process-group stop")


def verify_first_page_and_schedule():
    """One search page per run with every new card; overruns chain at once."""
    import fcntl
    import main as entry
    from boards.base import load_board_selectors
    from boards.indeed.scraper import IndeedJobSpider
    from boards.linkedin.scraper import LinkedInJobSpider
    from boards.wuzzuf.scraper import WuzzufJobSpider
    from config import DB_PATH
    from scrapling.spiders import Request

    async def collect(generator):
        return [item async for item in generator]

    # LinkedIn/Wuzzuf: the parsed first page is everything; no next-page request.
    for spider, url in ((LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1"),
                         "https://www.linkedin.com/jobs/search/?sortBy=DD"),
                        (WuzzufJobSpider(load_board_selectors("wuzzuf"), "http://127.0.0.1:1"),
                         "https://wuzzuf.net/search/jobs?q=&start=0")):
        spider._page_jobs = [{"external_id": str(i)} for i in range(25)]
        out = asyncio.run(collect(spider.parse(SimpleNamespace(url=url))))
        assert out == spider._page_jobs and not any(isinstance(item, Request) for item in out)
    # Indeed: every new card gets a detail request (the old cap was 10).
    indeed = IndeedJobSpider(load_board_selectors("indeed"), "http://127.0.0.1:1")
    indeed._page_jobs = [{"external_id": f"k{i}", "extra": {}} for i in range(14)]
    out = asyncio.run(collect(indeed.parse(SimpleNamespace(url="https://eg.indeed.com/jobs"))))
    assert len(out) == 14 and all(isinstance(item, Request) for item in out)
    assert not any(job["extra"].get("detail_status") for job in indeed._page_jobs)

    # Ticks sit on epoch multiples of the interval.
    assert not entry._missed_tick(180 * 1000 + 2, 180 * 1000 + 179, minutes=3)
    assert entry._missed_tick(180 * 1000 + 2, 180 * 1001 + 1, minutes=3)

    def lock_is_free():
        with open(DB_PATH + ".scraper.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)  # raises if still held

    with ExitStack() as stack:
        stack.enter_context(patch.object(entry, "_install_signal_handlers"))
        stack.enter_context(patch.object(entry, "_run_boards", return_value=0))
        chain = stack.enter_context(patch.object(entry, "_start_next_cycle_now", side_effect=lock_is_free))
        missed = stack.enter_context(patch.object(entry, "_missed_tick", return_value=True))
        with patch.object(entry.sys, "argv", ["main.py", "--scheduled"]):
            assert entry.main() == 0 and chain.call_count == 1, "an overrun must chain the next cycle"
        with patch.object(entry.sys, "argv", ["main.py"]):
            entry.main()
        assert chain.call_count == 1, "manual runs never chain"
        missed.return_value = False
        with patch.object(entry.sys, "argv", ["main.py", "--scheduled"]):
            entry.main()
        assert chain.call_count == 1, "a cycle that ended before the next tick waits for ofelia"
    with patch.object(entry.os, "execv") as execv, patch.object(entry.logging, "shutdown"), \
            patch.object(entry.sys, "argv", ["main.py", "--scheduled"]):
        entry._start_next_cycle_now()
    assert execv.call_args.args[1][1:] == ["main.py", "--scheduled"]
    # First signal unwinds via SystemExit; a second one kills our Chrome
    # groups and exits at once instead of raising into the cleanup.
    try:
        entry._handle_term(signal.SIGTERM, None)
        raise AssertionError("first signal must raise SystemExit")
    except SystemExit:
        pass
    from core import browser as chrome
    with patch.object(chrome, "kill_live_chrome") as kill, patch.object(entry.os, "_exit") as hard_exit, \
            patch.object(entry.logging, "shutdown"):
        try:
            entry._handle_term(signal.SIGTERM, None)
        except SystemExit:
            pass  # only because the mocked os._exit returns
    assert kill.called and hard_exit.call_args.args[0] == 128 + signal.SIGTERM
    entry._shutting_down = False
    print("PASS first page only (all cards, no Indeed detail cap), overrun chains the next cycle after releasing the lock")

    # Parallel cycle: every board starts before any finishes; the cycle waits
    # for the slowest; a failing board fails the cycle; signals reach children.
    class FakeProc:
        def __init__(self, seconds, code):
            self.started, self.seconds, self.code, self.signals = time.monotonic(), seconds, code, []
            self.returncode = None
        def poll(self):
            if self.returncode is None and time.monotonic() - self.started >= self.seconds:
                self.returncode = self.code
            return self.returncode
        def send_signal(self, sig): self.signals.append(sig); self.returncode = -sig
        def wait(self, timeout=None): return self.poll()
        def kill(self): self.returncode = -9
    plan = {"linkedin": (0.6, 0), "wuzzuf": (0.1, 0), "indeed": (0.3, 1)}
    spawned = {}
    def spawn(name, batch):
        spawned[name] = FakeProc(*plan[name]); return spawned[name]
    boards = [type(name.title(), (), {"name": name, "enabled": True}) for name in plan]
    with patch.object(entry, "_spawn_board", side_effect=spawn):
        began = time.monotonic()
        assert entry._run_parallel(boards) is True  # indeed exited 1
        assert time.monotonic() - began >= 0.6, "the cycle must wait for the slowest board"
        assert max(p.started for p in spawned.values()) - min(p.started for p in spawned.values()) < 0.1
        plan.update(linkedin=(30, 0), wuzzuf=(30, 0), indeed=(30, 0))
        sleeps = iter([None, SystemExit(0)])
        def interrupted_sleep(_s):
            value = next(sleeps)
            if value: raise value
        with patch.object(entry.time, "sleep", side_effect=interrupted_sleep):
            try:
                entry._run_parallel(boards)
            except SystemExit:
                pass
        assert all(p.signals == [signal.SIGTERM] for p in spawned.values()), "SIGTERM must reach every board"
    # Watchdog: a board stuck past budget+grace (e.g. Playwright swallowed its
    # own timeout) is stopped from outside, Chrome group included, run -> timeout.
    from core import board_budget
    state = Path(tempfile.mkdtemp()) / "board.budget"
    state.write_text(json.dumps({"paused_total": 30, "paused_since": None}))
    assert abs(board_budget.active_seconds(100.0, str(state), now=200.0) - 70) < 1e-6
    state.write_text(json.dumps({"paused_total": 0, "paused_since": 150.0}))
    assert abs(board_budget.active_seconds(100.0, str(state), now=200.0) - 50) < 1e-6
    # The child replaces the file in one step, so the parent never reads it
    # half written (truncate-then-write tore 72% of these reads; one torn read
    # at the end of a person wait killed the board).
    writer = ("import sys, time\n"
              f"sys.path.insert(0, {str(ROOT)!r})\n"
              "from core import board_budget\n"
              "end = time.time() + 1.0\n"
              "while time.time() < end:\n"
              "    board_budget._write_state(123.0)\n")
    proc = subprocess.Popen([sys.executable, "-c", writer], env={**os.environ, board_budget.STATE_ENV: str(state)})
    torn = 0
    while proc.poll() is None:
        torn += not board_budget.waiting_for_person(str(state))
    assert proc.returncode == 0 and torn == 0, f"{torn} torn budget-file reads"
    with patch.dict(os.environ, {board_budget.STATE_ENV: str(state.parent / "gone" / "board.budget")}), \
            patch.object(board_budget, "_write_failed", False), patch.object(board_budget.logger, "warning") as warn:
        board_budget._write_state(1.0)
        board_budget._write_state(None)
        assert warn.call_count == 1, "an unwritable state file is reported once, not swallowed"
    batch = entry.db.start_scrape_batch()
    with entry.db.get_db() as conn:  # earlier tests' Indeed jobs can share this second
        conn.execute("UPDATE jobs SET scraped_at='2026-01-01 00:00:00' WHERE source='indeed'")
    stuck_run = entry.db.start_run("indeed")
    entry.db.save_jobs([{"source": "indeed", "external_id": "killed-run-job", "title": "T", "company": "C",
                         "posted_at": None, "description": "d", "link": "https://x", "extra": {}}])
    plan.update(linkedin=(0.2, 0), wuzzuf=(0.2, 0), indeed=(3600, 0))
    killed_groups = []
    with patch.object(entry, "_spawn_board", side_effect=spawn), patch.object(entry, "BOARD_TIME_BUDGET_SECONDS", 0), \
            patch.object(entry, "_KILL_GRACE_SECONDS", 0.5), \
            patch.object(entry.subprocess, "run", return_value=SimpleNamespace(stdout="424242\n")), \
            patch.object(entry.os, "killpg", side_effect=lambda pid, sig: killed_groups.append(pid)), \
            patch.object(entry, "report_run_checks") as alert:
        began = time.monotonic()
        entry._run_parallel(boards)
        assert time.monotonic() - began < 5, "a stuck board must not hold the cycle"
    assert spawned["indeed"].signals == [signal.SIGTERM] and killed_groups == [424242]
    assert not spawned["linkedin"].signals, "boards within budget are left alone"
    with entry.db.get_db() as conn:
        assert tuple(conn.execute("SELECT status, jobs_found FROM runs WHERE id=?", (stuck_run,)).fetchone()) == ("timeout", 1), \
            "a killed run reports the jobs it saved before the kill"
    assert alert.call_args.args[0] == "indeed" and not alert.call_args.args[1]["time_budget"][0]
    entry.db.finish_scrape_batch(batch)
    # A child attaches to the parent's batch and runs exactly its board.
    seen = {}
    class ChildBoard:
        name, enabled = "wuzzuf", True
        def run(self): seen["batch"] = entry.db.active_batch(); return 0
    with patch.object(entry, "BOARDS", [ChildBoard]), patch.object(entry, "_install_signal_handlers"), \
            patch.object(entry, "_record_config_health"), \
            patch.object(entry.sys, "argv", ["main.py", "--board", "wuzzuf", "--batch-id", "41"]):
        assert entry.main() == 0 and seen["batch"] == 41
    entry.db.set_active_batch(None)
    print("PASS parallel boards: all start together, cycle waits for the slowest, failures and SIGTERM propagate, stuck board killed from outside, budget file never read half written; child joins the batch")

    # A broken selectors.json (live-editable bind mount) skips only that board,
    # alerts once per episode, and the exit code reports the failure.
    ran = []
    class Broken:
        name, enabled = "wuzzuf", True
        def __init__(self): raise ValueError("Expecting ',' delimiter: line 3 column 5")
    class Healthy:
        name, enabled = "indeed", True
        def run(self): ran.append(self.name); return 1
    from core import telegram as tg
    with patch.object(entry, "BOARDS", [Broken, Healthy]), patch.object(entry, "ENRICHMENT_ENABLED", True), \
            patch.object(tg, "notify_failure", return_value=True) as alert:
        assert entry._run_boards() == 1 and ran == ["indeed"]
        assert entry._run_boards() == 1 and ran == ["indeed", "indeed"]
        assert alert.call_count == 1 and "selectors.json" in alert.call_args.args[1]
    with entry.db.get_db() as conn:
        assert conn.execute("SELECT status FROM runs WHERE source='wuzzuf' ORDER BY id DESC LIMIT 1").fetchone()[0] == "config_error"
    with patch.object(entry, "BOARDS", [Healthy]), patch.object(entry, "ENRICHMENT_ENABLED", True):
        assert entry._run_boards() == 0
    print("PASS broken selectors skip only their board, alert once, non-zero exit")


def verify_person_wait():
    """A board waiting for a person (Indeed's emailed code, a LinkedIn
    checkpoint) never holds the cycle: on Oct 9-10 twelve such waits froze
    every board for 130 minutes. The cycle closes without it, later cycles
    keep the grid and skip it, its Chrome survives the stray-Chrome cleanup,
    its run ends signed in, and a signal or the budget still stops it."""
    import main as entry
    import boards.indeed as indeed_board
    import boards.linkedin as linkedin_board
    from core import board_budget, browser

    # Board side: a run that waited for a person ends signed in without
    # scraping (its jobs would land in a closed cycle); a login that needed
    # nobody scrapes as before.
    class Record:
        status = None
        def finish(self, status, **kw):
            self.status = self.status or status
    for cls, flag in ((indeed_board.IndeedBoard, "logged_out"), (linkedin_board.LinkedInBoard, "needs_login")):
        for waits in (True, False):
            board, record, calls = cls(), Record(), []
            def spider(cdp, _calls=calls):
                _calls.append(cdp)
                first = len(_calls) == 1
                return SimpleNamespace(status="ok", error=""), {
                    "items": [], "listings": {}, "blocked": False, "login_redirect": False,
                    "logged_out": False, "needs_login": False, "signed_in": not first, flag: first}  # noqa: B023 (same iteration)
            def login_check(cdp, _waits=waits):
                if _waits:
                    with board_budget.paused():
                        pass
                return True
            with patch.object(board, "_spider", side_effect=spider), \
                    patch.object(board, "_login_check", side_effect=login_check):
                assert board.scrape("cdp", record) == 0
            expected = ("signed_in", 1) if waits else ("ok", 2)
            assert (record.status, len(calls)) == expected, (cls.name, waits, record.status, len(calls))
    assert board_budget.person_waits() >= 2

    # Stray-Chrome cleanup spares a waiting board's whole process group.
    procs = {100: (100, "/opt/google/chrome/chrome --remote-debugging-port=9224 --user-data-dir=/app/indeedprofile"),
             101: (100, "/opt/google/chrome/chrome --type=renderer"),
             200: (200, "/opt/google/chrome/chrome --remote-debugging-port=9222"),
             201: (200, "/opt/google/chrome/chrome --type=gpu-process"),
             300: (300, "/opt/google/chrome/chrome --remote-debugging-port=92240")}
    assert browser._stray_chrome_pids(procs, [9224]) == [200, 201, 300]
    assert browser._stray_chrome_pids(procs, []) == [100, 101, 200, 201, 300]
    with patch.object(browser, "_chrome_processes", return_value=procs), patch.object(browser.os, "kill") as kill, \
            patch.object(browser.subprocess, "run") as pkill, patch("config.KILL_CHROME_ON_START", True):
        browser.kill_stray_chrome(keep_ports=[9224])
        assert sorted(call.args[0] for call in kill.call_args_list) == [200, 201, 300] and not pkill.called
        browser.kill_stray_chrome()
        assert pkill.call_args.args[0] == ["pkill", "-f", "chrome"]

    # Parent side, with real child processes that pause like a board does.
    child = ("import sys, time\n"
             f"sys.path.insert(0, {str(ROOT)!r})\n"
             "from core import board_budget\n"
             "pause, after = float(sys.argv[1]), float(sys.argv[2])\n"
             "if pause:\n"
             "    with board_budget.paused():\n"
             "        time.sleep(pause)\n"
             "time.sleep(after)\n")
    plans, spawned = {}, []
    def spawn(name, batch):
        path = entry._budget_file(name)
        try:
            os.remove(path)
        except OSError:
            pass
        pause, after = plans[name].pop(0) if len(plans[name]) > 1 else plans[name][0]
        proc = subprocess.Popen([sys.executable, "-c", child, str(pause), str(after)],
                                env={**os.environ, board_budget.STATE_ENV: path})
        spawned.append((name, time.time(), proc))
        return proc
    boards = [type(name.title(), (), {"name": name, "enabled": True, "port_offset": offset})
              for offset, name in enumerate(("linkedin", "wuzzuf", "indeed"))]
    period = 2.0
    starts, finishes = [], []
    real_start, real_finish = entry.db.start_scrape_batch, entry.db.finish_scrape_batch
    def start_batch():
        starts.append(time.time()); return real_start()
    def finish_batch(batch_id, interrupted=False):
        finishes.append(time.time()); return real_finish(batch_id, interrupted=interrupted)
    def after_tick():  # start just after a tick so the timeline below is predictable
        while not 0.1 <= time.time() % period <= 0.3:
            time.sleep(0.02)

    def cycle_patches(stack, signals=False):
        stack.enter_context(patch.object(entry, "BOARDS", boards))
        stack.enter_context(patch.object(entry, "BOARDS_PARALLEL", True))
        stack.enter_context(patch.object(entry, "ENRICHMENT_ENABLED", True))
        stack.enter_context(patch.object(entry, "_spawn_board", side_effect=spawn))
        stack.enter_context(patch.object(entry, "_period", return_value=period))
        stack.enter_context(patch.object(entry.db, "start_scrape_batch", side_effect=start_batch))
        stack.enter_context(patch.object(entry.db, "finish_scrape_batch", side_effect=finish_batch))
        stack.enter_context(patch.object(entry.sys, "argv", ["main.py", "--scheduled"]))
        if not signals:
            stack.enter_context(patch.object(entry, "_install_signal_handlers"))
        return (stack.enter_context(patch.object(entry.browser, "kill_stray_chrome")),
                stack.enter_context(patch.object(entry, "_start_next_cycle_now")))

    # Indeed waits 4.5s for its code; LinkedIn and Wuzzuf take 0.1s.
    plans.update(linkedin=[(0, 0.1)], wuzzuf=[(0, 0.1)], indeed=[(4.5, 0)])
    with ExitStack() as stack:
        stray, chain = cycle_patches(stack)
        after_tick()
        began = time.time()
        assert entry.main() == 0
        ended = time.time()
    names = [name for name, _at, _proc in spawned]
    assert finishes[0] - starts[0] < period, "the first cycle must close without the waiting board"
    assert len(starts) >= 3 and names.count("indeed") == 1, (len(starts), names)
    assert names.count("linkedin") == names.count("wuzzuf") == len(starts), "the other boards run every cycle"
    assert all(at % period < 0.9 for at in starts[1:]), "follow-up cycles start on the scheduler's grid"
    indeed_proc = next(proc for name, _at, proc in spawned if name == "indeed")
    assert indeed_proc.returncode == 0, "a waiting board is never killed when its cycle closes"
    port = entry.CHROME_DEBUG_PORT + 2
    assert stray.call_args_list[0].kwargs["keep_ports"] == []
    assert all(call.kwargs["keep_ports"] == [port] for call in stray.call_args_list[1:]), \
        "the waiting board's Chrome must survive the stray cleanup"
    assert ended - began < 4.5 + period, "the process exits once the wait ends (ofelia takes over)"
    assert not chain.called and not entry._detached and not entry._children
    print("PASS person wait: the cycle closes without the waiting board, later cycles keep the 3-minute grid "
          "and skip it, its Chrome is spared, the run ends signed in")

    # A signal while a board waits stops it too (compose would otherwise kill
    # it without cleanup when the container stops).
    spawned.clear(); starts.clear(); finishes.clear()
    plans.update(linkedin=[(0, 0.1)], wuzzuf=[(0, 0.1)], indeed=[(60, 0)])
    previous = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    timer = threading.Timer(1.2, os.kill, (os.getpid(), signal.SIGTERM))
    try:
        with ExitStack() as stack:
            cycle_patches(stack, signals=True)
            after_tick()
            timer.start()
            try:
                entry.main()
                raise AssertionError("SIGTERM must stop the process")
            except SystemExit:
                pass
    finally:
        timer.cancel()
        signal.signal(signal.SIGTERM, previous[0]); signal.signal(signal.SIGINT, previous[1])
        entry._shutting_down = False
    indeed_proc = next(proc for name, _at, proc in spawned if name == "indeed")
    assert indeed_proc.poll() == -signal.SIGTERM, indeed_proc.poll()
    assert not entry._detached and not entry._children
    with entry.db.get_db() as conn:
        assert not conn.execute("SELECT COUNT(*) FROM scrape_batches WHERE status='running'").fetchone()[0]

    # The time budget still applies after the wait (the wait itself never counts).
    spawned.clear()
    plans.update(indeed=[(1.5, 60)])
    killed = []
    with patch.object(entry, "_spawn_board", side_effect=spawn), patch.object(entry, "BOARD_TIME_BUDGET_SECONDS", 0), \
            patch.object(entry, "_KILL_GRACE_SECONDS", 1.0), \
            patch.object(entry.subprocess, "run", return_value=SimpleNamespace(stdout="")), \
            patch.object(entry, "report_run_checks", side_effect=lambda name, checks: killed.append(time.time())):
        assert entry._run_parallel(boards[2:]) is False and "indeed" in entry._detached
        deadline = time.time() + 15
        while entry._detached and time.time() < deadline:
            entry._tend_detached()
            time.sleep(0.1)
    name, spawned_at, proc = spawned[0]
    assert not entry._detached and proc.poll() == -signal.SIGTERM and killed
    assert killed[0] - spawned_at > 1.5, "paused time must not count against the budget"
    entry._children.clear()
    print("PASS person wait: a signal stops the waiting board too; the budget still applies after the wait")

    # deploy.sh: a board already stuck on a login before the switch is
    # unverified, not a reason to roll back; a login that broke after it is.
    deploy = (ROOT / "scripts/deploy.sh").read_text()
    body = deploy.index("\n", deploy.index("<<'PY'")) + 1  # the heredoc starts on the next line
    verdict = deploy[body:deploy.index("\nPY\n", body)]
    def judge(before, cycles):
        path = Path(tempfile.mkdtemp()) / "verdict.db"
        conn = sqlite3.connect(path)
        conn.executescript("CREATE TABLE scrape_batches (id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT);"
                           "CREATE TABLE runs (id INTEGER PRIMARY KEY, source TEXT, status TEXT, error TEXT,"
                           " batch_id INTEGER, started_at TEXT);")
        for source, status in before.items():
            conn.execute("INSERT INTO runs (source, status, started_at) VALUES (?,?,?)",
                         (source, status, "2026-10-10 11:57:00"))
        for index, runs in enumerate(cycles):
            at = f"2026-10-10 12:0{3 * index}:00"
            batch = conn.execute("INSERT INTO scrape_batches (started_at, finished_at) VALUES (?,?)", (at, at)).lastrowid
            for source, status in runs.items():
                conn.execute("INSERT INTO runs (source, status, batch_id, started_at) VALUES (?,?,?,?)",
                             (source, status, batch, at))
        conn.commit(); conn.close()
        return subprocess.run([sys.executable, "-", "2026-10-10 12:00:00", "2", str(path)], input=verdict,
                              capture_output=True, text=True, check=True).stdout.strip()
    ok = {"linkedin": "ok", "wuzzuf": "ok"}
    assert judge({}, [ok, ok]) == "PASS linkedin ok/ok; wuzzuf ok/ok"
    out = judge({"indeed": "login_failed"}, [{**ok, "indeed": "running"}, ok])
    assert out.startswith("PASS") and "unverified" in out and "indeed running" in out, out
    out = judge({"indeed": "ok"}, [{**ok, "indeed": "login_failed"}, {**ok, "indeed": "login_failed"}])
    assert out == "FAIL boards never ok: indeed login_failed/login_failed", out
    assert judge({}, [{**ok, "indeed": "signed_in"}, ok]).startswith("PASS")
    assert judge({}, [{**ok, "linkedin": "degraded"}, {**ok, "linkedin": "degraded"}]).startswith("FAIL")
    assert judge({}, [ok]) == "WAIT 1/2 cycles finished"
    print("PASS deploy verdict: a login already pending before the switch is unverified, a login broken by it rolls back")


def verify_linkedin_api_parsing():
    """LinkedIn's job-list and posting records (synthetic fixtures, real shape)."""
    from boards.base import load_board_selectors
    from boards.linkedin import scraper as li

    listed = [minutes_ago_ms(minutes) for minutes in (3, 20, 50)]
    body = linkedin_fixture("cards", listed_1=listed[0], listed_2=listed[1], listed_3=listed[2])
    window = li._parse_cards(body)
    assert window["total"] == 3 and window["ids"] == ["4400000001", "4400000002", "4400000003"]
    assert not window["unidentified"]
    first = window["cards"]["4400000001"]
    assert (first["title"], first["company"], first["location"]) == ("Data Analyst", "Example Analytics Co", "Cairo, Egypt (Hybrid)")
    assert first["listed_ms"] == listed[0] and first["listed_at"] == li._wall(listed[0]) and first["reposted"] is False
    assert window["cards"]["4400000003"]["reposted"] is True and "reposted" not in window["cards"]["4400000002"]
    # LinkedIn answers 200 even for a retired decoration: the shape is the proof.
    for broken in ({}, None, {"data": []}, {"data": {"elements": []}}, {"data": {"paging": {"total": "3"}, "elements": []}}):
        try:
            li._parse_cards(broken)
        except li.ApiShapeError:
            continue
        raise AssertionError(f"accepted {broken}")
    timeless = copy.deepcopy(body)
    for entity in timeless["included"]:
        entity.pop("footerItems", None)
    assert not any("listed_ms" in card for card in li._parse_cards(timeless)["cards"].values())

    original = minutes_ago_ms(9 * 24 * 60)
    posting = linkedin_fixture("posting", listed_1=listed[0], original_1=original)
    job, problem = li._job_from_posting(posting, "4400000001", first)
    assert not problem and (job["title"], job["company"]) == ("Data Analyst", "Example Analytics Co")
    assert job["description"].startswith("Build dashboards") and job["posted_at"] == li._wall(listed[0])
    assert job["link"] == "https://www.linkedin.com/jobs/view/4400000001/"
    assert job["extra"] == {
        "detail_source": "jobs_api", "detail_location": "Cairo, Cairo, Egypt", "workplace": "Hybrid",
        "job_type": "Full-time", "company_industry": "IT Services and IT Consulting",
        "company_description": "Example Analytics Co builds reporting tools for retailers. It was founded in 2020.",
        "original_listed_at": li._wall(original), "linkedin_reposted": False, "linkedin_state": "LISTED"}, job["extra"]
    # Identity: another job's record is never saved under this ID.
    assert li._job_from_posting(posting, "4400000002", first)[0] is None
    # A company without a LinkedIn page names itself in companyDetails; else the card.
    offsite = copy.deepcopy(posting)
    offsite["data"]["companyDetails"] = {"companyName": "Offsite Ltd"}
    assert li._job_from_posting(offsite, "4400000001", first)[0]["company"] == "Offsite Ltd"
    offsite["data"]["companyDetails"] = {}
    assert li._job_from_posting(offsite, "4400000001", first)[0]["company"] == "Example Analytics Co"
    # No description = a changed decoration: a problem, never an empty job.
    bare = copy.deepcopy(posting)
    del bare["data"]["description"]
    job, problem = li._job_from_posting(bare, "4400000001", first)
    assert job is None and "description present=False" in problem
    assert li._job_from_posting({"data": {}}, "1", {})[0] is None and li._job_from_posting(None, "1", {})[0] is None
    # Card-only job (record failing, job about to leave the window).
    card_job = li._job_from_card("4400000002", window["cards"]["4400000002"], "HTTP 500")
    assert (card_job["title"], card_job["company"], card_job["description"]) == ("Site Engineer", "Example Builders", "")
    assert card_job["extra"]["detail_status"] == "fetch_failed" and card_job["posted_at"] == li._wall(listed[1])
    # The window query follows the configured search page.
    assert li._search_filters() == {"geo_id": "106155005", "distance": "25"}
    url = li.LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1")._cards_url(50)
    assert all(part in url for part in ("timePostedRange:List(r3600)", "geoId:106155005", "distance:List(25)",
                                        "start=50", "count=25", "sortBy:List(DD)")) and "{" not in url
    print("PASS LinkedIn API parsing: window cards with listing times/repost flags, full posting record, identity, shape guardrails, card-only fallback")


def verify_linkedin_login():
    """Rejected credentials stop logins; checkpoints never wipe; one wipe per streak."""
    import boards.linkedin as li_board
    from boards.linkedin import login
    import boards.base as board_base
    from boards.base import load_board_selectors
    from core import login_state, markup
    sel = load_board_selectors("linkedin")

    class Element:
        def __init__(self, text, visible=True): self.text, self.visible = text, visible
        def is_visible(self): return self.visible
        def text_content(self): return self.text
    class Locator:
        def __init__(self, elements): self.elements = elements
        def all(self): return self.elements
    class Page:
        def __init__(self, url, errors=(), redirect_to=None, alerts=(), cookies=()):
            self.url, self.errors, self.redirect_to = url, list(errors), redirect_to
            self.alerts = list(alerts)
            self.context = SimpleNamespace(cookies=lambda _url=None: list(cookies))
        def locator(self, css):
            # Field-level credential errors vs page-level sign-in alerts.
            return Locator(self.errors if css == sel["login"]["credential_error"] else self.alerts)
        def wait_for_url(self, pattern, timeout=None, wait_until=None):
            assert wait_until == "commit", "LinkedIn never fires load"
            if self.redirect_to:
                self.url = self.redirect_to
            if not pattern.search(self.url):
                raise TimeoutError("still on the sign-in form")
        def wait_for_timeout(self, _ms): pass
        def content(self): return "<html></html>"

    with ExitStack() as stack:
        alerts = stack.enter_context(patch.object(login, "notify_failure", return_value=True))
        stack.enter_context(patch.object(markup, "save_snapshot", return_value="test/login.html"))
        stack.enter_context(patch.object(login, "LINKEDIN_EMAIL", "user@example.com"))
        stack.enter_context(patch.object(login, "LINKEDIN_PASSWORD", "old-password"))
        stack.enter_context(patch.object(li_board, "LINKEDIN_EMAIL", "user@example.com"))
        stack.enter_context(patch.object(li_board, "LINKEDIN_PASSWORD", "old-password"))
        login_state.reset_retries()

        # An active session whose /login -> /feed redirect lands after
        # DOMContentLoaded is logged in, never a failure (Sep-Oct snapshots).
        late = Page("https://www.linkedin.com/login", redirect_to="https://www.linkedin.com/feed/")
        assert login.ensure_logged_in(late, sel) and login_state.get_retry_count() == 0
        # A failure path that finds the feed after settling reports success.
        assert login._fail(Page("https://www.linkedin.com/feed/"), "other", "automation error")
        assert login_state.get_retry_count() == 0 and not alerts.called

        # Hidden or empty error placeholders are not a rejection.
        quiet = Page("https://www.linkedin.com/login", errors=[Element("Wrong password", visible=False), Element("  ")])
        assert login._classify(quiet, sel) is None and not login_state.credentials_locked("user@example.com", "old-password")

        # A visible rejection locks logins with one alert; the board skips Chrome.
        rejected = Page("https://www.linkedin.com/login", errors=[Element("Wrong email or password. Try again.")])
        assert login._classify(rejected, sel) is False
        assert alerts.call_count == 1 and "rejected" in alerts.call_args.args[0]
        assert "Wrong email or password" in alerts.call_args.args[1]
        with patch.object(board_base, "chrome_session", side_effect=AssertionError("Chrome must not start")):
            for _ in range(3):
                assert li_board.LinkedInBoard().run() == 0
        with login_state.db.get_db() as conn:
            assert conn.execute("SELECT status FROM runs WHERE source='linkedin' ORDER BY id DESC LIMIT 1").fetchone()[0] == "credentials_rejected"
        assert alerts.call_count == 1, "a rejected login must alert once, not every run"
        assert "old-password" not in login_state.db.get_state(login_state.REJECTED_KEY)
        # Changed credentials release the lock and the streak by themselves.
        assert not login_state.credentials_locked("user@example.com", "new-password")
        assert login_state.db.get_state(login_state.REJECTED_KEY) == ""
        login_state.lock_credentials("user@example.com", "new-password")
        login_state.reset_retries()  # --reset-login also clears it
        assert not login_state.credentials_locked("user@example.com", "new-password")

        def expire_cooldown():
            login_state.db.set_state(login_state.BLOCKED_KEY, 0)

        # A page-level alert ("try again later") is transient: cooldown only,
        # never a credential lock, and it does not earn a profile wipe.
        alerts.reset_mock()
        busy = Page("https://www.linkedin.com/login", alerts=[Element("Unusual activity. Please try again later.")])
        assert login._classify(busy, sel) is False
        assert not login_state.credentials_locked("user@example.com", "new-password")
        assert login_state.get_retry_count() == 1 and login_state.is_blocked() and not alerts.called
        for _ in range(2):
            expire_cooldown()
            login._register_failure("alert", "sign-in alert", error_text="Unusual activity. Please try again later.")
        expire_cooldown()
        assert login_state.max_retries_reached() and not login_state.should_wipe_profile()
        assert "sign-in alert" in alerts.call_args.args[1] and "Unusual activity" in alerts.call_args.args[1]
        login_state.reset_retries()

        # /jobs counts as logged in only with LinkedIn's session cookie.
        assert login._is_logged_in(Page("https://www.linkedin.com/jobs/", cookies=[{"name": "li_at", "value": "x"}]))
        assert not login._is_logged_in(Page("https://www.linkedin.com/jobs/"))
        assert not login._is_logged_in(Page("https://www.linkedin.com/login", cookies=[{"name": "li_at", "value": "x"}]))

        # A checkpoint solved in its last seconds is solved (Oct 23: the page
        # was on /feed a second after the deadline, yet the run counted a
        # failure and cooled down 30 minutes).
        with patch.object(login, "CHECKPOINT_WAIT_SECONDS", 0):
            late_solve = Page("https://www.linkedin.com/checkpoint/challenge/x", redirect_to="https://www.linkedin.com/feed/")
            assert login._handle_checkpoint(late_solve, sel) and login_state.get_retry_count() == 0
            assert not login._handle_checkpoint(Page("https://www.linkedin.com/checkpoint/challenge/x"), sel)
            assert login_state.get_retry_count() == 1
        login_state.reset_retries()

        # Unsolved checkpoints keep cooling down but never wipe the profile.
        for _ in range(4):
            login._register_failure("checkpoint", "checkpoint unresolved")
            expire_cooldown()
        assert login_state.max_retries_reached() and not login_state.should_wipe_profile()
        assert "checkpoint or sign-in alert is involved" in alerts.call_args.args[1]
        login_state.reset_retries()

        # Other failures earn exactly one wipe per streak, then cooldowns only.
        alerts.reset_mock()
        for _ in range(3):
            login._register_failure("other", "automation error")
        assert alerts.call_count == 1 and "wiped once" in alerts.call_args.args[1]
        expire_cooldown()
        assert login_state.should_wipe_profile()
        login_state.mark_profile_wiped()
        for _ in range(5):
            login._register_failure("other", "automation error")
            expire_cooldown()
        assert not login_state.should_wipe_profile()
        assert alerts.call_count == 2 and "already failed" in alerts.call_args.args[1]
        login_state.reset_retries()
    print("PASS LinkedIn login: late feed redirect, credential lock vs transient alerts, cookie-checked /jobs, checkpoint solved at the deadline, checkpoint never wipes, one wipe per streak")



def verify_scrapling_contract_check():
    """Our patches target one scrapling release; any other fails loudly."""
    import importlib.metadata
    from core import browser
    from scrapling.engines._browsers._stealth import StealthySession
    browser.verify_scrapling_contract()
    with patch.object(importlib.metadata, "version", return_value="0.5.0"):
        try:
            browser.verify_scrapling_contract()
            raise AssertionError("a different scrapling release must be refused")
        except RuntimeError as exc:
            assert "scrapling 0.5.0 is installed" in str(exc)
    with patch.object(StealthySession, "_initialize_context", None):
        try:
            browser.verify_scrapling_contract()
            raise AssertionError("a missing internal must be refused")
        except RuntimeError as exc:
            assert "StealthySession._initialize_context is missing" in str(exc)
    print("PASS scrapling contract: version and every patched/called internal checked")


def verify_clean_quit():
    """stop_chrome asks Chrome to quit over CDP first: SIGTERM exits without
    writing cookies (Chrome saves them every 30s or on a clean shutdown)."""
    import http.server
    import socket
    import threading
    from core import browser
    received = []
    ws = socket.socket(); ws.bind(("127.0.0.1", 0)); ws.listen(1)
    ws_port = ws.getsockname()[1]
    def serve_ws():
        conn, _ = ws.accept()
        request = conn.recv(4096)
        assert b"Upgrade: websocket" in request and b"Origin:" not in request
        conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
        frame = conn.recv(4096)
        length, mask = frame[1] & 0x7F, frame[2:6]
        received.append(json.loads(bytes(b ^ mask[i % 4] for i, b in enumerate(frame[6:6 + length]))))
        conn.close()
    class Version(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"webSocketDebuggerUrl": f"ws://127.0.0.1:{ws_port}/devtools/browser/abc"}).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args): pass
    http_server = http.server.HTTPServer(("127.0.0.1", 0), Version)
    threading.Thread(target=http_server.handle_request, daemon=True).start()
    threading.Thread(target=serve_ws, daemon=True).start()
    assert browser._browser_close(http_server.server_address[1])
    time.sleep(0.2)
    assert received and received[0]["method"] == "Browser.close", received
    started = time.monotonic()
    free = socket.socket(); free.bind(("127.0.0.1", 0)); dead_port = free.getsockname()[1]; free.close()
    assert not browser._browser_close(dead_port, timeout=0.5) and time.monotonic() - started < 1.5
    print("PASS clean Chrome quit: Browser.close over CDP (stdlib WebSocket), falls back to signals when unreachable")
def verify_naukrigulf():
    """NaukriGulf: jobs from the search API response the page receives
    (fixture: trimmed public fields of a real response, Oct 2026)."""
    import asyncio
    from datetime import datetime
    import main as entry
    from boards.base import load_board_selectors
    from boards.naukrigulf import NaukriGulfBoard, scraper as ng
    from core import db, telegram
    sel = load_board_selectors("naukrigulf")
    body = json.loads((ROOT / "markup/naukrigulf/search_sample.json").read_text())
    base = sel["search"]["job_link_base"]
    jobs, stats = ng._extract_jobs(body, set(), base)
    assert len(jobs) == 5 and stats == {"entries": 5, "known": 0, "no_id": 0, "no_date": 0, "total": 47}, stats
    first = jobs[0]
    assert first["external_id"] == "091026501533" and first["title"] == "Reservation Agent" and first["company"] == "AccorHotel"
    assert first["link"] == base + "reservation-agent-jobs-in-alexandria-egypt-in-accorhotel-2-to-5-years-n-cd-10050787-jid-091026501533"
    assert first["posted_at"] == datetime.fromtimestamp(1791546800).strftime("%Y-%m-%d %H:%M")
    assert "<" not in first["description"] and first["description"].startswith("Company Description")
    assert "Johnson & Johnson MedTech" in jobs[1]["description"], "HTML entities decoded"
    assert first["extra"]["location"] == "Alexandria - Egypt" and first["extra"]["experience_min"] == "2"
    assert first["extra"]["is_consultant"] and jobs[3]["extra"]["is_easy_apply"] and jobs[3]["extra"]["job_source"] == "EMAIL"
    assert all(job["source"] == "naukrigulf" and job["scraped_at"] for job in jobs)
    # Known jobs are skipped; their exact listing time feeds repost detection.
    listings = {}
    with patch.object(db, "seen_ids_for", return_value={"091026501533", "091026501488"}):
        fresh, stats = ng._extract_jobs(body, set(), base, lookup_seen=True, listings=listings)
    assert len(fresh) == 3 and stats["known"] == 2 and set(listings) == {"091026501533", "091026501488"}
    broken = json.loads(json.dumps(body))
    del broken["jobs"][0]["jobId"]; broken["jobs"][1]["latestPostedDate"] = "soon"
    fresh, stats = ng._extract_jobs(broken, set(), base)
    assert stats["no_id"] == 1 and stats["no_date"] == 1 and len(fresh) == 4
    assert ng._extract_jobs({"error": "x"}, set(), base) == ([], {"entries": None, "known": 0, "no_id": 0, "no_date": 0, "total": None})

    # Live path: the page's own API response is captured and scanned.
    class Response:
        def __init__(self, payload, status=200, url="https://www.naukrigulf.com/spapi/jobapi/search?Limit=30&Offset=0"):
            self.payload, self.status, self.url = payload, status, url
        async def json(self): return self.payload
    class Page:
        url = "https://www.naukrigulf.com/jobs-in-egypt?freshness=1"
        def __init__(self, html="<html><body><div id='root'></div><script>var puppeteer = false;</script></body></html>"):
            self.html, self.handlers = html, {}
        def on(self, event, callback): self.handlers.setdefault(event, []).append(callback)
        async def content(self): return self.html
    async def scan(responses=(), failure=None, html=None):
        spider = ng.NaukriGulfJobSpider(sel, "http://127.0.0.1:1")
        page = Page(html) if html else Page()
        with patch.object(ng, "patch_no_load_wait", AsyncMock()):
            await spider.setup_page(page); await spider.setup_page(page)
        assert len(page.handlers["response"]) == 1, "listeners attach once per reused tab"
        page.handlers["response"][0](Response(body, url="https://www.naukrigulf.com/nglogin/user/isJsLoggedIn"))
        for response in responses:
            page.handlers["response"][0](response)
        if failure:
            page.handlers["requestfailed"][0](SimpleNamespace(url=Response(None).url, failure=failure))
        with patch.object(db, "seen_ids_for", return_value=set()), patch.object(ng, "_API_WAIT_SECONDS", 0.3):
            await spider.scan_page(page)
        return spider
    spider = asyncio.run(scan([Response(body)]))
    assert len(spider._page_jobs) == 5 and spider.health.status == "ok", spider.health.error
    blocked = asyncio.run(scan(failure="net::ERR_HTTP2_PROTOCOL_ERROR"))
    assert not blocked.health.checks["search_api"]["good"] and "Akamai" in blocked.health.error
    assert "HTTP 403" in asyncio.run(scan([Response(None, status=403)])).health.error
    flagged = asyncio.run(scan(html="<script>var puppeteer = true;</script>"))
    assert "automation" in flagged.health.error
    oops = asyncio.run(scan(html="<div>Oops! Something went wrong</div><script>var puppeteer = false;</script>"))
    assert "error screen" in oops.health.error
    assert "never arrived" in asyncio.run(scan()).health.error
    damaged = asyncio.run(scan([Response(broken)]))
    assert not damaged.health.checks["card_identity"]["good"] and not damaged.health.checks["posted_date"]["good"]
    with patch.object(telegram, "notify_failure", return_value=True) as alert:
        blocked.health.report(); blocked.health.report()
        assert alert.call_count == 1 and "naukrigulf" in alert.call_args.args[0].lower()
    # Board wiring: registered, own CDP port, selectors present.
    assert NaukriGulfBoard in entry.BOARDS and NaukriGulfBoard.port_offset == 3
    offsets = [cls.port_offset for cls in entry.BOARDS]
    assert len(offsets) == len(set(offsets)), "every board needs its own CDP port"
    print("PASS NaukriGulf: API jobs parsed (ids, links, exact dates, text), known/repost listings, blocks diagnosed and alerted")


def verify_cloudflare_fast_path():
    """scrapling's solver waits up to 5s for network idle BEFORE looking for a
    challenge; with the fast path a page without one skips it entirely."""
    import asyncio
    from core.browser import install_cloudflare_fast_path
    from scrapling.engines._browsers._stealth import AsyncStealthySession, StealthySession
    install_cloudflare_fast_path(); install_cloudflare_fast_path()  # idempotent
    challenge = "<script>window._cf_chl_opt={cType: 'managed'}</script>"
    clean = "<html><body>jobs</body></html>"
    class SyncPage:
        def __init__(self, contents): self.contents, self.calls = list(contents), []
        def content(self): return self.contents.pop(0) if len(self.contents) > 1 else self.contents[0]
        def wait_for_timeout(self, ms): self.calls.append(("timeout", ms))
    class AsyncPage(SyncPage):
        async def content(self): return SyncPage.content(self)
        async def wait_for_timeout(self, ms): self.calls.append(("timeout", ms))
    def fake_session(asynchronous=False):
        if asynchronous:
            async def idle(page, timeout=None): page.calls.append("networkidle")
        else:
            def idle(page, timeout=None): page.calls.append("networkidle")
        return SimpleNamespace(_detect_cloudflare=StealthySession._detect_cloudflare, _wait_for_networkidle=idle)
    page = SyncPage([clean])
    assert StealthySession._cloudflare_solver(fake_session(), page) is None
    assert page.calls == [("timeout", 1000)], "no challenge: two looks, no 5s network-idle wait"
    page = SyncPage([challenge, clean])  # the real solver then finds it cleared
    StealthySession._cloudflare_solver(fake_session(), page)
    assert "networkidle" in page.calls, "a challenge goes to the real solver"
    page = AsyncPage([clean])
    assert asyncio.run(AsyncStealthySession._cloudflare_solver(fake_session(True), page)) is None
    assert "networkidle" not in page.calls
    page = AsyncPage([clean, challenge, clean])  # appears right after DOMContentLoaded
    asyncio.run(AsyncStealthySession._cloudflare_solver(fake_session(True), page))
    assert "networkidle" in page.calls, "the second look catches a late challenge"
    class Broken(AsyncPage):
        async def content(self): raise RuntimeError("target closed")
    page = Broken([clean])
    try:
        asyncio.run(AsyncStealthySession._cloudflare_solver(fake_session(True), page))
    except RuntimeError:
        pass
    assert "networkidle" in page.calls, "unreadable page: let the real solver decide"
    # Through the real solver's click path: its untimed post-click idle wait
    # (the page default: 120s on Indeed) is capped; the 5s one is unchanged.
    from core.browser import _CF_CLICK_IDLE_MS
    class ClickPage(SyncPage):
        """A managed challenge that clears once the checkbox is clicked."""
        def __init__(self):
            super().__init__([challenge])
            self.mouse, self.idle = self, []
        def content(self): return clean if "click" in self.calls else challenge
        def click(self, x, y, **_kw): self.calls.append("click")
        def frame(self, url=None): return None
        def locator(self, _selector): return SimpleNamespace(last=SimpleNamespace(bounding_box=lambda: {"x": 0, "y": 0}))
    class AsyncClickPage(ClickPage):
        async def content(self): return ClickPage.content(self)
        async def wait_for_timeout(self, ms): self.calls.append(("timeout", ms))
        async def click(self, x, y, **_kw): self.calls.append("click")
        def locator(self, _selector):
            async def box(): return {"x": 0, "y": 0}
            return SimpleNamespace(last=SimpleNamespace(bounding_box=box))
    def click_session(asynchronous=False):
        if asynchronous:
            async def idle(page, timeout=None): page.idle.append(timeout)
            async def stable(*_args): pass
        else:
            def idle(page, timeout=None): page.idle.append(timeout)
            def stable(*_args): pass
        return SimpleNamespace(_detect_cloudflare=StealthySession._detect_cloudflare, _wait_for_networkidle=idle,
                               _challenge_cleared=StealthySession._challenge_cleared, _wait_for_page_stability=stable)
    page = ClickPage()
    assert StealthySession._cloudflare_solver(click_session(), page) is None and "click" in page.calls
    assert page.idle == [5000, _CF_CLICK_IDLE_MS], page.idle
    page = AsyncClickPage()
    assert asyncio.run(AsyncStealthySession._cloudflare_solver(click_session(True), page)) is None
    assert "click" in page.calls and page.idle == [5000, _CF_CLICK_IDLE_MS], page.idle
    print("PASS Cloudflare fast path: solver only for real challenges (two looks), retries untouched, post-click idle wait capped")


def verify_direct_search():
    """LinkedIn goes straight to the search page; the /login check runs only
    when that page is not a signed-in session (it used to run every run)."""
    import asyncio
    import boards.linkedin as li_board
    from boards.linkedin import scraper as li
    from boards.base import load_board_selectors

    # Spider: the search page proves the session (URL + li_at cookie).
    class Context:
        def __init__(self, cookies): self._cookies = cookies
        async def cookies(self, url):
            if isinstance(self._cookies, Exception): raise self._cookies
            return self._cookies
    class SearchPage:
        def __init__(self, url, cookies): self.url, self.context = url, Context(cookies)
    spider = li.LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1")
    search = "https://www.linkedin.com/jobs/search/?sortBy=DD"
    session = [{"name": "li_at", "value": "x"}]
    assert not asyncio.run(spider._signed_out(SearchPage(search, session)))
    assert asyncio.run(spider._signed_out(SearchPage(search, [{"name": "JSESSIONID", "value": "y"}]))), "guest page"
    assert asyncio.run(spider._signed_out(SearchPage("https://www.linkedin.com/authwall?x=1", session)))
    assert asyncio.run(spider._signed_out(SearchPage(search, RuntimeError("cdp")))), "unknown counts as signed out"
    guest = li.LinkedInJobSpider(load_board_selectors("linkedin"), "http://127.0.0.1:1")
    asyncio.run(guest.scan_window(SearchPage(search, [])))
    assert guest._needs_login and not guest._signed_in and not guest.health.checks, "no alert: the board logs in"

    # Board: login check only when needed; retries reset only on a proven session.
    calls = []
    def board_with(results, login_ok=True):
        board = li_board.LinkedInBoard()
        queue = list(results)
        board._spider = lambda cdp: (calls.append("spider"), (SimpleNamespace(status="ok", error=""), queue.pop(0)))[1]
        board._login_check = lambda cdp: (calls.append("login"), login_ok)[1]
        return board
    class Record:
        def __init__(self): self.status = None
        def finish(self, status, **kw): self.status = self.status or status
    signed_in = {"items": [], "login_redirect": False, "needs_login": False, "signed_in": True, "listings": {}}
    signed_out = {"items": [], "login_redirect": False, "needs_login": True, "signed_in": False, "listings": {}}
    with patch.object(li_board.login_state, "reset_retries") as reset, \
            patch.object(li_board.telegram, "notify_failure", return_value=True) as alert:
        record = Record()
        assert board_with([signed_in]).scrape("cdp", record) == 0
        assert calls == ["spider"] and record.status == "ok" and reset.call_count == 1
        calls.clear(); record = Record()
        assert board_with([signed_out, signed_in]).scrape("cdp", record) == 0
        assert calls == ["spider", "login", "spider"] and record.status == "ok"
        calls.clear(); record = Record()
        board_with([signed_out], login_ok=False).scrape("cdp", record)
        assert calls == ["spider", "login"] and record.status == "login_failed"
        calls.clear(); record = Record()
        board_with([signed_out, signed_out]).scrape("cdp", record)
        assert record.status == "session_expired" and alert.call_count == 1
        unproven = dict(signed_in, signed_in=False)  # search never loaded: nothing to reset
        reset.reset_mock(); calls.clear()
        board_with([unproven]).scrape("cdp", Record())
        assert reset.call_count == 0
    # Indeed: one search load per run; the login check only after a logged-out landing.
    import boards.indeed as in_board
    def indeed_with(results, login_ok=True):
        board = in_board.IndeedBoard()
        queue = list(results)
        board._spider = lambda cdp: (calls.append("spider"), (SimpleNamespace(status="ok", error=""), queue.pop(0)))[1]
        board._login_check = lambda cdp: (calls.append("login"), login_ok)[1]
        return board
    fine = {"items": [], "listings": {}, "logged_out": False, "blocked": False}
    out = dict(fine, logged_out=True)
    with patch.object(in_board.login, "clear_episode_flags") as cleared, \
            patch.object(in_board.login, "mark_logged_out") as expired:
        calls.clear(); record = Record()
        indeed_with([fine]).scrape("cdp", record)
        assert calls == ["spider"] and record.status == "ok" and cleared.call_count == 1
        calls.clear(); record = Record()
        indeed_with([out, fine]).scrape("cdp", record)
        assert calls == ["spider", "login", "spider"] and record.status == "ok"
        calls.clear(); record = Record()
        indeed_with([out], login_ok=False).scrape("cdp", record)
        assert calls == ["spider", "login"] and record.status == "login_failed"
        calls.clear(); record = Record()
        indeed_with([out, out]).scrape("cdp", record)
        assert record.status == "session_expired" and expired.call_count == 1
    print("PASS LinkedIn/Indeed straight to search: session proven on the search page; login check only when signed out")


def verify_browser_recovery():
    from boards.linkedin import scraper as li
    from boards.indeed import scraper as indeed
    from boards.base import load_board_selectors
    from core.browser_diagnostics import BrowserDiagnostics
    from core import db

    class Request:
        def __init__(self, kind="script", failure="net::ERR_NETWORK_CHANGED", host="static.licdn.com"):
            self.url = "https://" + host + "/private/path?token=never-log-this"
            self.resource_type = kind
            self.failure = failure
    class EventPage:
        def __init__(self): self.listeners = {}
        def on(self, event, callback): self.listeners.setdefault(event, []).append(callback)
        def emit(self, event, value):
            for callback in self.listeners.get(event, []): callback(value)
    diagnostics = BrowserDiagnostics({"www.linkedin.com", "static.licdn.com"})
    emitter = EventPage()
    diagnostics.attach(emitter)
    diagnostics.attach(emitter)
    assert all(len(callbacks) == 1 for callbacks in emitter.listeners.values())
    req = Request()
    emitter.emit("request", req)
    assert len(diagnostics.pending) == 1
    emitter.emit("requestfailed", req)
    assert not diagnostics.pending and "ERR_NETWORK_CHANGED" in diagnostics.summary()
    assert "private" not in diagnostics.summary() and "never-log-this" not in diagnostics.summary()
    diagnostics.reset()
    for req in (Request("stylesheet"), Request(failure="net::ERR_ABORTED"), Request(host="ads.example.com")):
        emitter.emit("requestfailed", req)
    assert not diagnostics.events
    emitter.emit("response", SimpleNamespace(request=Request("document", host="www.linkedin.com"), status=503))
    assert "HTTP:" in diagnostics.summary() and "503" in diagnostics.summary()
    emitter.emit("response", SimpleNamespace(request=Request("xhr", host="www.linkedin.com"), status=429))
    assert diagnostics.access_status == 429

    sel = load_board_selectors("linkedin")

    def cards_body(ids, total=None, minutes=5, timed=True):
        """A job-list page for these IDs, each listed `minutes` ago."""
        listed = minutes_ago_ms(minutes)
        return {"data": {"paging": {"total": len(ids) if total is None else total, "count": 25, "start": 0},
                         "elements": [{"jobCardUnion": {"*jobPostingCard": f"urn:li:fsd_jobPostingCard:({i},JOBS_SEARCH)"}}
                                      for i in ids]},
                "included": [{"entityUrn": f"urn:li:fsd_jobPostingCard:({i},JOBS_SEARCH)", "jobPostingTitle": f"Job {i}",
                              "primaryDescription": {"text": f"Company {i}"}, "secondaryDescription": {"text": "Cairo, Egypt"},
                              **({"footerItems": [{"type": "LISTED_DATE", "timeAt": listed}]} if timed else {})}
                             for i in ids]}

    def posting(job_id, state="LISTED"):
        body = linkedin_fixture("posting", listed_1=minutes_ago_ms(5), original_1=minutes_ago_ms(5))
        body["data"].update(jobPostingId=int(job_id), entityUrn=f"urn:li:fs_normalized_jobPosting:{job_id}",
                            title=f"Job {job_id}", jobState=state)
        return body

    async def run(page, known=()):
        spider = li.LinkedInJobSpider(sel, "http://127.0.0.1:1")
        spider.on_job = (saved := []).append
        with patch.object(db, "seen_ids_for", side_effect=lambda source, keys: set(keys) & set(known)):
            await spider.scan_window(page)
        return spider, saved

    async def scenarios():
        ids = [str(4480000000 + i) for i in range(30)]
        # The whole window over two pages; only new IDs get a posting record and
        # each is handed to the board (save_now) at once; known IDs report
        # their listing time (repost detection).
        page = LinkedInApiPage([(200, cards_body(ids[:25], total=30)), (200, cards_body(ids[25:], total=30))],
                               {i: (200, posting(i)) for i in ids})
        spider, saved = await run(page, known=ids[:27])
        assert spider.health.status == "ok", spider.health.error
        assert {"search_fetch", "search_structure", "window_complete", "card_ids", "listing_times", "window_filter",
                "detail_fetch", "detail_content"} <= set(spider.health.checks)
        assert [job["external_id"] for job in saved] == ids[27:] and all(job["description"] for job in saved)
        assert ["JobCards" in url for url in page.calls] == [True, True, False, False, False]
        assert "start=25" in page.calls[1] and set(spider.listings) == set(ids[:27])
        # All known: one list call, no records; an empty window (night) is valid.
        page = LinkedInApiPage([(200, cards_body(ids[:3]))])
        spider, saved = await run(page, known=ids[:3])
        assert not saved and len(page.calls) == 1 and spider.health.status == "ok"
        spider, saved = await run(LinkedInApiPage([(200, cards_body([]))]))
        assert not saved and spider.health.status == "ok"

        # Guardrails on the list: shape change, no listing times (jobs still
        # read), ignored time filter, incomplete window, entries without an ID.
        spider, _ = await run(LinkedInApiPage([(200, {"data": {"metadata": {}}})]))
        assert not spider.health.checks["search_structure"]["good"] and "API changed" in spider.health.error
        spider, saved = await run(LinkedInApiPage([(200, cards_body(ids[:3], timed=False))], {i: (200, posting(i)) for i in ids}))
        assert not spider.health.checks["listing_times"]["good"] and len(saved) == 3
        spider, _ = await run(LinkedInApiPage([(200, cards_body(ids[:20], minutes=300))]), known=ids)
        assert not spider.health.checks["window_filter"]["good"] and spider.health.checks["window_complete"]["good"]
        spider, _ = await run(LinkedInApiPage([(200, cards_body(ids[:25], total=40)), (200, cards_body([], total=40))]), known=ids)
        assert not spider.health.checks["window_complete"]["good"] and "25 of the 40" in spider.health.error
        # The gap says which kind it is (2026-10-24: total ran 1-2 ahead for 2 h):
        # a job served on two pages hides another, or LinkedIn counts one it never serves.
        page = LinkedInApiPage([(200, cards_body(ids[:25], total=30)), (200, cards_body(ids[24:29], total=30)),
                                (200, cards_body([], total=30))])
        spider, _ = await run(page, known=ids)
        assert not spider.health.checks["window_complete"]["good"] and len(page.calls) == 3
        assert "29 of the 30" in spider.health.error and "pages served 25+5+0 cards, 1 repeated from an earlier page" \
            f" ({ids[24]})" in spider.health.error, spider.health.error
        # No repeats, list ended, up to 3 short: LinkedIn counts jobs it does not
        # list yet. One relevance-order page looks for them; never an alert.
        short = [(200, cards_body(ids[:25], total=30)), (200, cards_body(ids[25:29], total=30)), (200, cards_body([], total=30))]
        page = LinkedInApiPage(short + [(200, cards_body(ids[:25], total=30))])
        spider, saved = await run(page, known=ids)
        assert spider.health.status == "ok" and spider.health.checks["window_complete"]["good"], spider.health.error
        assert len(page.calls) == 4 and "sortBy:List(DD)" in page.calls[0] and "sortBy:List(R)" in page.calls[3] \
            and "start=0" in page.calls[3] and not saved
        page = LinkedInApiPage(short + [(200, cards_body([ids[29]] + ids[:10], total=30))], {ids[29]: (200, posting(ids[29]))})
        spider, saved = await run(page, known=ids[:29])
        assert spider.health.status == "ok" and [job["external_id"] for job in saved] == [ids[29]], spider.health.error
        page = LinkedInApiPage(short + [(429, "")])
        spider, saved = await run(page, known=ids)
        assert spider.health.status == "ok" and len(page.calls) == 4 and not saved, spider.health.error
        # One past the limit, or a repeat (above), is ours to explain: no probe, alert.
        page = LinkedInApiPage([(200, cards_body(ids[:25], total=30)), (200, cards_body(ids[25:26], total=30)),
                                (200, cards_body([], total=30))])
        spider, _ = await run(page, known=ids)
        assert not spider.health.checks["window_complete"]["good"] and len(page.calls) == 3
        assert "26 of the 30" in spider.health.error and "pages served 25+1+0 cards, 0 repeated" in spider.health.error
        odd = cards_body(ids[:2])
        odd["data"]["elements"].append({"jobCardUnion": {"*jobPostingCard": "urn:li:fsd_promotedCard:1"}})
        spider, _ = await run(LinkedInApiPage([(200, odd)]), known=ids)
        assert not spider.health.checks["card_ids"]["good"]

        # Access: 401 or a redirect to sign-in = the board's login check (no
        # alert); 429 is never retried; a 5xx, a navigation race and a stale
        # CSRF token retry once; a page without JSESSIONID cannot call the API.
        for answer in ((401, ""), (200, cards_body(ids[:1]), "https://www.linkedin.com/checkpoint/challenge/x")):
            spider, _ = await run(LinkedInApiPage([answer]))
            assert spider._needs_login and "search_structure" not in spider.health.checks
        page = LinkedInApiPage([(429, "")])
        spider, _ = await run(page)
        assert len(page.calls) == 1 and "limiting" in spider.health.error
        for first in ((503, ""), RuntimeError("Execution context was destroyed")):
            page = LinkedInApiPage([first, (200, cards_body(ids[:1]))], {ids[0]: (200, posting(ids[0]))})
            spider, saved = await run(page)
            assert len(saved) == 1 and spider.health.status == "ok", spider.health.error
        page = LinkedInApiPage([(403, "CSRF check failed."), (200, cards_body([]))])
        spider, _ = await run(page)
        assert spider.health.status == "ok" and page.context.cookies.await_count >= 3, "token read again"
        spider, _ = await run(LinkedInApiPage([(200, cards_body([]))], cookies=[{"name": "li_at", "value": "x"}]))
        assert "JSESSIONID" in spider.health.error

        # Posting records: a deleted (404) or suspended job is skipped (retried
        # while still listed); a record for another job is unusable and the
        # job is saved from its card; a failing record waits for the next
        # run, unless the job could leave the window first.
        page = LinkedInApiPage([(200, cards_body(ids[:5]))], {
            ids[0]: (404, None), ids[1]: (200, posting(ids[1], state="SUSPENDED")),
            ids[2]: (200, posting(ids[9])), ids[3]: (500, ""), ids[4]: (200, posting(ids[4]))})
        spider, saved = await run(page)
        assert [job["external_id"] for job in saved] == [ids[2], ids[4]]
        assert saved[0]["extra"]["detail_status"] == "fetch_failed" and not saved[0]["description"]
        assert saved[0]["title"] == f"Job {ids[2]}" and saved[1]["description"]
        assert not spider.health.checks["detail_content"]["good"] and not spider.health.checks["detail_fetch"]["good"]
        assert sum(ids[3] in url for url in page.calls) == 2, "a 5xx record is retried once"
        spider, saved = await run(LinkedInApiPage([(200, cards_body(ids[:1], minutes=50))], {ids[0]: (500, "")}))
        assert len(saved) == 1 and saved[0]["extra"]["detail_status"] == "fetch_failed", "saved before it leaves the window"
        page = LinkedInApiPage([(200, cards_body(ids[:4]))], {i: (429, "") for i in ids})
        spider, saved = await run(page)
        assert sum("jobPostings" in url for url in page.calls) == 1 and not saved and "limiting" in spider.health.error
        spider, _ = await run(LinkedInApiPage([(200, cards_body(ids[:2]))], {i: (401, "") for i in ids}))
        assert spider._needs_login
        # Per-run cap, oldest listing first (it leaves the window first).
        body = cards_body(ids[:4])
        for n, entity in enumerate(body["included"]):
            entity["footerItems"][0]["timeAt"] = minutes_ago_ms(10 + n)
        with patch.object(li, "LINKEDIN_MAX_DETAILS_PER_RUN", 2):
            spider, saved = await run(LinkedInApiPage([(200, body)], {i: (200, posting(i)) for i in ids}))
        assert [job["external_id"] for job in saved] == [ids[3], ids[2]]
        view = (ROOT / "markup/indeed/newjob_sample.html").read_text()
        detail_spider = indeed.IndeedJobSpider({}, "http://127.0.0.1:1")
        detail_saved = []
        detail_spider.on_job = detail_saved.append
        key = indeed._jobkey_from_url("https://eg.indeed.com/viewjob?jk=abc123") or "abc123"
        detail_spider._pending[key] = {"source": "indeed", "external_id": key, "extra": {}}
        class DetailPage:
            url = f"https://eg.indeed.com/viewjob?jk={key}"
            async def content(self): return view
        await detail_spider.scan_detail_page(DetailPage(), key=key)
        assert [job["external_id"] for job in detail_saved] == [key]
        failed = li.LinkedInJobSpider(sel, "http://127.0.0.1:1")
        failed.health.check("search_fetch", True)  # earlier page worked
        await failed.on_error(None, RuntimeError("net::ERR_CONNECTION_CLOSED https://private/?token=secret"))
        assert failed.health.status == "degraded" and "ERR_CONNECTION_CLOSED" in failed.health.error
        assert "token" not in failed.health.error

    with patch.object(asyncio, "sleep", new=AsyncMock()):
        asyncio.run(scenarios())
    print("PASS network/HTTP diagnostics; LinkedIn API window: complete paging, repost listings, shape/filter/completeness guardrails, access stops, retries, record identity, card fallback, per-run cap")


if __name__ == "__main__":
    main()
