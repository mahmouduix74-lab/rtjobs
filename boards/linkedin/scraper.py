"""LinkedIn jobs from LinkedIn's own job-search API, read inside a signed-in page.

Since 2026-10-21 `/jobs/search/` redirects to an AI search page (relevance
order, randomized class names), so job cards can no longer be read from the
DOM. Each run opens the search page as before (it proves the session, Gotchas
#5) and then, from INSIDE that page (real Chrome: its cookies, TLS and
headers, the same requests LinkedIn's web app makes):

1. Lists the WHOLE recent window: `voyagerJobsDashJobCards` filtered to
   `timePostedRange` r3600, paged until the collected IDs reach the reported
   total. LinkedIn buckets r3600 by clock hour (it covers everything since the
   start of the previous hour, 60-120 minutes), so a job stays listed for
   20-40 runs: a failed run, or an hour of them, loses nothing. Order inside
   the window does not matter (sortBy=DD is only roughly sorted).
2. Fetches one full posting record per unseen ID: title, description,
   company (+ industry and description), location, job type, workplace and
   listing times.

Known IDs report their current listing time from the list itself (repost
detection, core/db.record_listings): no extra requests.

Guardrails (the API is private and can change without notice): LinkedIn
answers HTTP 200 even for a retired decoration, so a status code proves
nothing. Every response is checked for the fields we use (`search_structure`,
`detail_content`), the window must be complete (`window_complete`), still
time-filtered (`window_filter`) and carry listing times (`listing_times`),
and a posting record must be the job we asked for. Request templates live in
markup/linkedin/selectors.json ("api"): bind-mounted, so a new decoration
version is a file edit, not a rebuild. A posting record that keeps failing is
saved from its list card before it can leave the window.

Verified live 2026-10-23/24 on a copy of the production profile: 10 rounds x 3
minutes collected == total every round, new jobs listed 2-7 minutes after
listedAt, 9/9 of the guest search's last-hour jobs inside our window.
"""

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime
from urllib.parse import parse_qs, urlsplit

from scrapling.fetchers import AsyncStealthySession
from scrapling.spiders import Request, Response, Spider

from config import (LINKEDIN_SEARCH_URL, LINKEDIN_MAX_DETAILS_PER_RUN, LINKEDIN_MAX_WINDOW_PAGES,
                    LINKEDIN_NAVIGATION_TIMEOUT_SECONDS, LINKEDIN_NAVIGATION_RETRY_DELAY_SECONDS)
from core import clock, db, timing
from core.browser import patch_no_load_wait
from core.browser_diagnostics import BrowserDiagnostics
from core.scrape_health import ScrapeHealth
from core.log import configure_spider_logging

logger = logging.getLogger(__name__)

# URL paths that mean "not a signed-in session" (landing page or API redirect).
_SIGNED_OUT_PATHS = ("/login", "/checkpoint", "/authwall", "/security-verification", "/uas/login")
_CARD_URN = re.compile(r"^urn:li:fsd_jobPostingCard:\((\d+),")
# A job stays in the r3600 window for at least 60 minutes (the hour bucket)
# and shows up 2-7 minutes after its listing time. A posting record that is
# still failing 45 minutes after listing is saved from its card instead.
_GIVE_UP_DETAIL_MINUTES = 45
# Cards listed longer ago than this mean the time filter no longer applies.
_WINDOW_MAX_AGE_HOURS = 3
_API_TIMEOUT_MS = 15_000

# In-page fetch: the request goes out from LinkedIn's own page with the
# session's cookies, exactly like the web app's voyager calls. Never raises:
# a network error or timeout comes back as status 0.
_FETCH_JS = """async ([url, csrf, timeoutMs]) => {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const r = await fetch(url, {credentials: 'include', signal: ctrl.signal, headers: {
      'csrf-token': csrf, 'x-restli-protocol-version': '2.0.0',
      'accept': 'application/vnd.linkedin.normalized+json+2.1', 'x-li-lang': 'en_US'}});
    return {status: r.status, url: r.url, body: await r.text()};
  } catch (e) {
    return {status: 0, url: '', body: String((e && e.name) || e)};
  } finally {
    clearTimeout(timer);
  }
}"""


class ApiShapeError(ValueError):
    """A 200 response without the fields we rely on: LinkedIn changed the API."""


def _wall(ms) -> str | None:
    """Epoch milliseconds -> local wall time (the posted_at format), else None."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _text(value) -> str:
    """A voyager TextViewModel's text (or a plain string), stripped."""
    if isinstance(value, dict):
        value = value.get("text")
    return value.strip() if isinstance(value, str) else ""


def _search_filters(url: str = LINKEDIN_SEARCH_URL) -> dict:
    """geoId/distance of the configured search page (one source of truth)."""
    query = parse_qs(urlsplit(url).query)
    return {"geo_id": (query.get("geoId") or ["106155005"])[0],
            "distance": (query.get("distance") or ["25"])[0]}


def _parse_cards(body) -> dict:
    """One job-cards page -> {"total", "ids", "unidentified", "cards"}.

    `ids` keep the response order. `cards` maps job id -> {"title",
    "company", "location", "listed_ms", "listed_at", "reposted"} for the
    cards this decoration describes (any field may be missing). Raises
    ApiShapeError when the page itself is not the shape we read.
    """
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        raise ApiShapeError("the response has no data object")
    paging, elements = data.get("paging"), data.get("elements")
    total = paging.get("total") if isinstance(paging, dict) else None
    if not isinstance(total, int) or not isinstance(elements, list):
        raise ApiShapeError("expected data.paging.total and data.elements; data has keys "
                            + ", ".join(sorted(map(str, data))[:12]))
    ids, urns, unidentified = [], {}, 0
    for element in elements:
        union = element.get("jobCardUnion") if isinstance(element, dict) else None
        urn = union.get("*jobPostingCard") if isinstance(union, dict) else None
        match = _CARD_URN.match(urn or "")
        if not match:
            unidentified += 1
            continue
        ids.append(match.group(1))
        urns[urn] = match.group(1)
    cards: dict[str, dict] = {}
    reposted: dict[str, bool] = {}
    for entity in body.get("included") or []:
        if not isinstance(entity, dict):
            continue
        urn = str(entity.get("entityUrn") or "")
        if urn in urns:
            card = cards.setdefault(urns[urn], {})
            card["title"] = _text(entity.get("jobPostingTitle")) or _text(entity.get("title"))
            card["company"] = _text(entity.get("primaryDescription"))
            card["location"] = _text(entity.get("secondaryDescription"))
            for item in entity.get("footerItems") or []:
                if isinstance(item, dict) and item.get("type") == "LISTED_DATE" and _wall(item.get("timeAt")):
                    card["listed_ms"] = int(item["timeAt"])
                    card["listed_at"] = _wall(item["timeAt"])
        elif urn.startswith("urn:li:fsd_jobPosting:") and isinstance(entity.get("repostedJob"), bool):
            reposted[urn.rsplit(":", 1)[1]] = entity["repostedJob"]
    for job_id, flag in reposted.items():
        if job_id in cards:
            cards[job_id]["reposted"] = flag
    return {"total": total, "ids": ids, "unidentified": unidentified, "cards": cards}


def _job_from_posting(body, job_id: str, card: dict) -> tuple[dict | None, str]:
    """A full posting record (WebFullJobPosting) -> (job, problem).

    job is None with a problem when the record is not usable: another job's
    ID, or no title/description (the decoration changed). `card` fills what
    the record lacks. state is in extra["linkedin_state"] (LISTED,
    SUSPENDED, CLOSED...).
    """
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or not data:
        return None, "the record has no data object"
    record_id = str(data.get("jobPostingId") or str(data.get("entityUrn") or "").rsplit(":", 1)[-1])
    if record_id != str(job_id):
        return None, f"the record is for job {record_id or '?'}, not {job_id}"
    description = data.get("description")
    description = description.get("text") if isinstance(description, dict) else None
    title = data.get("title") if isinstance(data.get("title"), str) else ""
    if not (title or "").strip() or not (description or "").strip():
        return None, (f"title present={bool((title or '').strip())}, description present="
                      f"{bool((description or '').strip())}; record keys: {', '.join(sorted(data)[:15])}")
    included = {e.get("entityUrn"): e for e in body.get("included") or [] if isinstance(e, dict)}
    details = data.get("companyDetails") if isinstance(data.get("companyDetails"), dict) else {}
    company = included.get(details.get("company") or details.get("*companyResolutionResult")) or {}
    names = [included.get(urn, {}).get("localizedName") for urn in data.get("workplaceTypes") or []]
    employment = included.get(data.get("employmentStatus")) or {}
    industries = [str(name) for name in company.get("industries") or [] if name]
    extra = {
        "detail_source": "jobs_api",
        "detail_location": data.get("formattedLocation") or card.get("location") or "",
        "workplace": ", ".join(dict.fromkeys(name for name in names if name)),
        "job_type": data.get("formattedEmploymentStatus") or employment.get("localizedName") or "",
        "company_industry": ", ".join(industries),
        "company_description": re.sub(r"\s+", " ", str(company.get("description") or "")).strip(),
        "original_listed_at": _wall(data.get("originalListedAt")),
        "linkedin_reposted": card.get("reposted"),
        "linkedin_state": data.get("jobState"),
    }
    job = {
        "source": "linkedin",
        "external_id": str(job_id),
        "title": title.strip(),
        "company": (company.get("name") or details.get("companyName") or card.get("company") or "").strip(),
        "posted_at": _wall(data.get("listedAt")) or card.get("listed_at") or datetime.now().strftime("%Y-%m-%d %H:%M"),
        "description": description.strip(),
        "link": f"https://www.linkedin.com/jobs/view/{job_id}/",
        "extra": {key: value for key, value in extra.items() if value not in (None, "")},
        "scraped_at": clock.now_str(),  # UTC; posted_at stays local
    }
    return job, ""


def _job_from_card(job_id: str, card: dict, reason: str) -> dict:
    """A job from its list card alone (title, company, location, listing time).

    Used when the posting record keeps failing and the job is about to leave
    the window, or when the record API changed: a delivered job without its
    description beats a lost one. The classifier sees detail_status.
    """
    return {
        "source": "linkedin",
        "external_id": str(job_id),
        "title": card.get("title") or "",
        "company": card.get("company") or "",
        "posted_at": card.get("listed_at") or datetime.now().strftime("%Y-%m-%d %H:%M"),
        "description": "",
        "link": f"https://www.linkedin.com/jobs/view/{job_id}/",
        "extra": {key: value for key, value in {
            "detail_source": "cards_api", "detail_status": "fetch_failed", "detail_error": reason[:200],
            "detail_location": card.get("location"), "linkedin_reposted": card.get("reposted"),
        }.items() if value not in (None, "")},
        "scraped_at": clock.now_str(),
    }


class LinkedInJobSpider(Spider):
    name = "linkedin_job_spider"

    def __init__(self, selectors: dict, cdp_url: str, *args, **kwargs):
        self.sel = selectors
        self.api = selectors["api"]
        self.cdp_url = cdp_url
        self.seen_ids: set[str] = set()  # IDs saved during this run only
        self._page_jobs: list[dict] = []
        self.listings: dict[str, str] = {}  # known job id -> current listing time
        self.on_job = None  # board.save_now: persist each job as soon as it is scraped
        # Not signed in (landing page, or an API 401/redirect): the board runs
        # its login check and retries (no health alert, the login flow owns that).
        self._needs_login: bool = False
        self._signed_in: bool = False  # the search page proved the session
        self._csrf = ""
        self.health = ScrapeHealth("linkedin")
        self.diagnostics = BrowserDiagnostics({"www.linkedin.com", "linkedin.com", "static.licdn.com"})

        super().__init__(*args, **kwargs)
        configure_spider_logging(self)

    def configure_sessions(self, manager):
        manager.add(
            "stealth",
            AsyncStealthySession(
                cdp_url=self.cdp_url,
                # disable_resources=False: scrapling's resource blocking installs
                # page.route(), and Playwright disables Chrome's HTTP cache on any
                # intercepted page (Gotchas #11).
                disable_resources=False,
                timeout=LINKEDIN_NAVIGATION_TIMEOUT_SECONDS * 1000,
                retries=2,
                retry_delay=LINKEDIN_NAVIGATION_RETRY_DELAY_SECONDS,
                page_setup=self.setup_search_page,
            ),
        )

    async def setup_search_page(self, page):
        # Document commit is enough: the API calls need the page's origin and
        # cookies, not LinkedIn's app (a slow app-bundle download no longer
        # holds the run, Gotchas #11).
        await patch_no_load_wait(page, selector_readiness=True)
        self.diagnostics.reset()
        self.diagnostics.attach(page)

    async def on_error(self, request: Request, error: Exception):
        # Called after scrapling exhausts its navigation attempts.
        self.diagnostics.navigation_error(error)
        self.health.check("search_fetch", False,
                          "Search navigation failed after retries. Evidence: " + self.diagnostics.summary())

    async def _signed_out(self, page) -> bool:
        """The search page is not a signed-in session: a login/checkpoint URL,
        or no li_at session cookie (guests can see a public jobs page at the
        same URL, Gotchas #5). Unknown counts as signed out: a wrong guess
        only costs the old login check, never a scrape of a guest page."""
        if any(part in urlsplit(page.url or "").path.lower() for part in _SIGNED_OUT_PATHS):
            return True
        try:
            cookies = await page.context.cookies("https://www.linkedin.com")
        except Exception:
            return True
        return not any(c.get("name") == "li_at" and c.get("value") for c in cookies)

    async def _csrf_token(self, page) -> str:
        """LinkedIn's CSRF token for API calls: the JSESSIONID cookie, unquoted."""
        try:
            cookies = await page.context.cookies("https://www.linkedin.com")
        except Exception:
            return ""
        return next((str(c.get("value") or "").strip('"') for c in cookies if c.get("name") == "JSESSIONID"), "")

    async def _api(self, page, url: str) -> tuple[int, dict | None, str]:
        """GET a voyager URL from inside the page -> (status, json, problem).

        Status 0 = no response (network error, timeout, the page navigated
        away). Network errors and 5xx get one retry; 401/403/429 never do
        (they need a person or time, and retrying can make them worse). A
        response that ended on a sign-in/checkpoint URL counts as 401.
        """
        problem = ""
        for attempt in range(2):
            try:
                result = await page.evaluate(_FETCH_JS, [url, self._csrf, _API_TIMEOUT_MS])
            except Exception as exc:
                # The page navigated (e.g. a client-side redirect) while the call
                # ran; the next document has the same origin and cookies.
                result, problem = {"status": 0, "url": "", "body": ""}, type(exc).__name__
            status = int(result.get("status") or 0)
            if any(part in urlsplit(result.get("url") or "").path.lower() for part in _SIGNED_OUT_PATHS):
                return 401, None, "redirected to sign-in/checkpoint"
            if status == 200:
                try:
                    return 200, json.loads(result.get("body") or ""), ""
                except ValueError:
                    return 200, None, "the response is not JSON"
            if status == 403 and "CSRF" in str(result.get("body") or "") and not attempt:
                # The session's JSESSIONID changed under us: read it again once.
                self._csrf = await self._csrf_token(page)
                continue
            problem = problem or (f"HTTP {status}" if status else str(result.get("body") or "no response")[:80])
            if (status and status < 500) or attempt:
                return status, None, problem
            await asyncio.sleep(2)
            problem = ""
        return 0, None, problem

    def _cards_url(self, start: int) -> str:
        return self.api["cards_url"].format(start=start, count=self.api.get("page_size", 25),
                                            window=self.api.get("window", "r3600"), **_search_filters())

    async def start_requests(self):
        yield Request(
            LINKEDIN_SEARCH_URL,
            callback=self.parse,
            sid="stealth",
            page_action=self.scan_window,
        )

    async def scan_window(self, page):
        self._page_jobs = []
        if await self._signed_out(page):
            self._needs_login = True
            logger.info("[linkedin] Not signed in on the search page (%s) — the board will run its login check.",
                        urlsplit(page.url or "").path)
            return
        self._signed_in = True
        self.health.check("search_fetch", True)
        with timing.stage("linkedin", "search_page", lambda: {"new_jobs": len(self._page_jobs)}):
            self._csrf = await self._csrf_token(page)
            if not self._csrf:
                self.health.check("search_structure", False,
                                  "The signed-in page has no JSESSIONID cookie, which LinkedIn's API needs as its CSRF token.")
                return
            window = await self._list_window(page)
            if window is None:
                return
            ids, cards = window
            stored = db.seen_ids_for("linkedin", ids)
            # Known jobs: their current listing time feeds repost detection.
            self.listings.update({key: cards[key]["listed_at"] for key in stored if cards.get(key, {}).get("listed_at")})
            new = [key for key in ids if key not in stored and key not in self.seen_ids]
            flagged = sum(1 for key in stored if cards.get(key, {}).get("reposted"))
            logger.info("[linkedin] Window: %s jobs, %s new; listing times for %s of %s known (LinkedIn flags %s"
                        " as reposted).", len(ids), len(new), len(self.listings), len(stored), flagged)
            if new:
                await self._fetch_new(page, new, cards)

    async def _list_window(self, page) -> tuple[list[str], dict] | None:
        """Every job ID in the recent window, with its card; None on failure.

        Pages until the IDs collected reach the first page's total. A job
        added while paging shifts later pages by one (a repeat, not a gap); a
        job removed meanwhile can hide one other job until the next run.
        `served` (cards per page) and `repeated` (IDs an earlier page already
        had) tell those apart from a job LinkedIn counts but never lists.
        """
        ids: list[str] = []
        cards: dict[str, dict] = {}
        served: list[int] = []
        repeated: list[str] = []
        total, unidentified, calls = None, 0, 0
        size = int(self.api.get("page_size", 25))
        with timing.stage("linkedin", "job_list", lambda: {"calls": calls, "total": total, "ids": len(ids),
                                                            "served": served, "repeated": len(repeated)}):
            for start in range(0, size * LINKEDIN_MAX_WINDOW_PAGES, size):
                if calls:
                    await self._pause(0.3, 0.8)
                status, body, problem = await self._api(page, self._cards_url(start))
                calls += 1
                if status == 401:
                    self._needs_login = True
                    logger.info("[linkedin] Job list answered 401 (%s) — the board will run its login check.", problem)
                    return None
                if body is None:
                    self.health.check("search_structure", False,
                                      f"Job list request (start={start}) failed: {problem}. "
                                      + ("LinkedIn is limiting or restricting access; no retry this run. "
                                         if status in (403, 429) else "")
                                      + "Evidence: " + self.diagnostics.summary())
                    return None
                try:
                    parsed = _parse_cards(body)
                except ApiShapeError as exc:
                    self.health.check("search_structure", False,
                                      f"Job list API changed: {exc}. Update api.cards_url in markup/linkedin/selectors.json.")
                    return None
                total = parsed["total"] if total is None else total
                unidentified += parsed["unidentified"]
                fresh = [key for key in parsed["ids"] if key not in cards]
                served.append(len(parsed["ids"]))
                repeated += [key for key in parsed["ids"] if key in cards]
                for key in parsed["ids"]:
                    cards.setdefault(key, {}).update(parsed["cards"].get(key, {}))
                ids += fresh
                if not parsed["ids"] or len(ids) >= total:
                    break
        self.health.check("search_structure", True)
        self._check_window(ids, cards, total, unidentified, calls, served, repeated)
        return ids, cards

    def _check_window(self, ids: list[str], cards: dict, total: int, unidentified: int, calls: int,
                      served: list[int], repeated: list[str]):
        """Guardrails on a listed window: complete, time-filtered, with times."""
        cap = int(self.api.get("page_size", 25)) * LINKEDIN_MAX_WINDOW_PAGES
        self.health.check("window_complete", len(ids) >= total,
                          f"Collected {len(ids)} of the {total} jobs LinkedIn reported for the recent window in "
                          f"{calls} request(s)" + (f" (cap: {cap}; raise LINKEDIN_MAX_WINDOW_PAGES)" if total > cap else "")
                          + f"; pages served {'+'.join(map(str, served))} cards, {len(repeated)} repeated from an earlier page"
                          + (f" ({', '.join(repeated[:5])})" if repeated else "")
                          + ". The rest are read on later runs while they stay listed.")
        self.health.check("card_ids", not unidentified,
                          f"{unidentified} job-list entries had no job ID and were skipped (API change?).")
        timed = [cards[key]["listed_ms"] for key in ids if cards.get(key, {}).get("listed_ms")]
        self.health.check("listing_times", not ids or len(timed) * 10 >= len(ids) * 9,
                          f"Only {len(timed)} of {len(ids)} job-list cards carry a listing time (decoration changed?): "
                          "repost detection is paused; new jobs still get their time from the posting record.")
        cutoff = (time.time() - _WINDOW_MAX_AGE_HOURS * 3600) * 1000
        stale = sum(1 for ms in timed if ms < cutoff)
        self.health.check("window_filter", stale <= max(2, len(timed) // 10),
                          f"{stale} of {len(timed)} cards in the 'recent' window were listed over "
                          f"{_WINDOW_MAX_AGE_HOURS}h ago: LinkedIn may be ignoring the time filter.")

    async def _fetch_new(self, page, new: list[str], cards: dict):
        """One posting record per new job, oldest listing first (it leaves the
        window first), capped per run; the rest wait for the next run."""
        new = sorted(new, key=lambda key: cards.get(key, {}).get("listed_ms") or 0)
        batch, waiting = new[:LINKEDIN_MAX_DETAILS_PER_RUN], new[LINKEDIN_MAX_DETAILS_PER_RUN:]
        failures, shape_problems, inactive, stop = {}, {}, [], ""
        for job_id in batch:
            card = cards.get(job_id, {})
            if not stop:
                await self._pause(0.5, 1.2)
                with timing.stage("linkedin", "detail_api", {"job_id": job_id}):
                    status, body, problem = await self._api(page, self.api["posting_url"].format(job_id=job_id))
                if status == 401:
                    self._needs_login = True
                    logger.info("[linkedin] Posting record answered 401 — the board will run its login check.")
                    break
                if status == 404:
                    inactive.append(job_id)  # deleted since it was listed
                    continue
                if body is not None:
                    job, problem = _job_from_posting(body, job_id, card)
                    if job is None:
                        shape_problems[job_id] = problem
                        self._save(_job_from_card(job_id, card, "record unusable: " + problem))
                        continue
                    if job["extra"].get("linkedin_state", "LISTED") != "LISTED":
                        inactive.append(job_id)  # suspended/closed: retried while it stays listed
                        continue
                    self._save(job)
                    continue
                failures[job_id] = problem
                if status in (403, 429):
                    stop = problem  # no further calls this run
            # The record failed: retry next run unless the job may leave the
            # window first (a card without a title is not worth saving).
            listed = card.get("listed_ms")
            if card.get("title") and (not listed or time.time() * 1000 - listed >= _GIVE_UP_DETAIL_MINUTES * 60_000):
                self._save(_job_from_card(job_id, card, failures.get(job_id) or stop or "not fetched"))
        logger.info("[linkedin] New jobs: %s saved, %s record failures, %s deleted/inactive, %s for the next run.",
                    len(self._page_jobs), len(failures), len(inactive), len(waiting))
        self.health.check("detail_fetch", not failures,
                          f"{len(failures)}/{len(batch)} posting records failed"
                          + (f" (stopped: {stop}; LinkedIn is limiting access)" if stop else "")
                          + ": " + "; ".join(f"{key}: {value}" for key, value in list(failures.items())[:3])
                          + f". Retried next run; saved from the card after {_GIVE_UP_DETAIL_MINUTES} min."
                          + " Evidence: " + self.diagnostics.summary())
        self.health.check("detail_content", not shape_problems,
                          f"{len(shape_problems)} posting record(s) unusable, saved from their cards without a "
                          "description (API changed? update api.posting_url in markup/linkedin/selectors.json): "
                          + "; ".join(f"{key}: {value}" for key, value in list(shape_problems.items())[:2]))
        self.health.job_fields(self._page_jobs)

    def _save(self, job: dict):
        self._page_jobs.append(job)
        self.seen_ids.add(job["external_id"])
        logger.info("  [ok] %s%s", job["title"][:45], "" if job["description"] else " (card only)")
        if self.on_job:
            self.on_job(job)

    async def _pause(self, low: float, high: float):
        pause = random.uniform(low, high)  # human-like spacing between API calls
        timing.record("linkedin", "human_delay", pause)
        await asyncio.sleep(pause)

    async def parse(self, response: Response):
        # One window per run: no follow-up page requests.
        for job in self._page_jobs:
            yield job


def scrape(selectors: dict, cdp_url: str, health: ScrapeHealth | None = None, on_job=None) -> dict:
    """Run the spider. Returns {'items': [...], 'needs_login': bool, ...}.

    `on_job(job)` is called for each job as soon as it is scraped.
    """
    spider = LinkedInJobSpider(selectors=selectors, cdp_url=cdp_url)
    spider.on_job = on_job
    if health is not None:
        spider.health = health
    result = spider.start()
    items = list(result.items)
    if spider._needs_login:
        logger.info(f"[spider] Not signed in; {len(items)} item(s) in {result.stats.elapsed_seconds:.1f}s")
        return {"items": items, "login_redirect": False, "needs_login": True, "signed_in": False,
                "listings": spider.listings}
    if "search_fetch" not in spider.health.checks:
        spider.health.check("search_fetch", False,
                            "Search navigation never reached the parser. Evidence: " + spider.diagnostics.summary())
    elif "search_structure" not in spider.health.checks and spider.health.checks["search_fetch"]["good"]:
        spider.health.check("search_structure", False, "The search callback did not finish checking the page. Check scraper logs.")
    spider.health.report()
    logger.info(
        f"[spider] {len(items)} item(s) scraped in {result.stats.elapsed_seconds:.1f}s"
    )
    return {"items": items, "login_redirect": False, "needs_login": False,
            "signed_in": spider._signed_in, "listings": spider.listings}
