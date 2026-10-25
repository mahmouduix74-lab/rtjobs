# NaukriGulf board

Public job board (no login) for the Gulf region; we scrape the Egypt search:
`https://www.naukrigulf.com/jobs-in-egypt?freshness=1&xz=1_3_5`
(`NAUKRIGULF_SEARCH_URL`; `freshness=1` = posted in the last day). Disabled by
default (`NAUKRIGULF_ENABLED=false`) until a supervised live run passes.
CDP port `CHROME_DEBUG_PORT + 3` (9225); profile volume `naukrigulf_profile`.

## Protection: Akamai Bot Manager

- The edge is Akamai; every page loads its sensor script (`/akam/13/...`) and
  sets `ak_bmsc` / `bm_sv` / `bm_mi` cookies. A trusted client also gets `_abck`.
- Plain HTTP clients (curl, any non-browser) get HTTP/2 stream resets
  (`INTERNAL_ERROR`) or a held-open connection. A fresh automation-driven
  browser (`navigator.webdriver=true`, no profile history) got the shell, then
  `ERR_HTTP2_PROTOCOL_ERROR` on the API calls, then on the document itself:
  the verdict is cached per client and escalates (recon, Oct 2026).
- The server renders `var puppeteer = false;` into the page; when it decides a
  visitor is automation the app never bootstraps (no API call at all).
- Consequences: real Chrome over CDP on a persistent profile (trust history
  accumulates), no resource blocking (Gotchas #11 + a page that never loads
  images/styles is another signal), ONE navigation per run (`retries=1`), and
  never call the API ourselves. Repeated failing visits can also spill onto
  the egress IP and break the operator's own browser on this site.

## Data: the search API response the page receives

The HTML is a splash screen; the app's JS calls
`GET /spapi/jobapi/search?...&Limit=30&Location=Egypt&Offset=0&pageNo=1...`
with its own headers (`appid: 205`, `systemid: 2323`, `client-type: desktop`,
`puppeteer: false`, ...). The spider only listens for that response
(`search.api_path` in `markup/naukrigulf/selectors.json`).

Response (fixture: `markup/naukrigulf/search_sample.json`, 5 trimmed jobs):
- `totalJobsCount` (47 for one day of Egypt jobs in Oct 2026), `jobs[]` (30
  per page), `other.searchCriteria.sortPreference = "date"`, `timestamp`.
- Per job: `jobId` (external ID, e.g. `091026501533`), `designation` (title),
  `company.{name,id}`, `location` ("Alexandria - Egypt"), `description`
  (FULL HTML — no detail page needed), `jobInfo` (truncated summary),
  `experience.{min,max}`, `latestPostedDate` (epoch seconds; "latest" = it
  moves on a refresh, so it feeds repost detection like Wuzzuf's postedAt),
  `jdURL` (relative slug → `https://www.naukrigulf.com/<jdURL>`), `vacancies`,
  `isSponsoredJob`, `isEasyApply`, `isConsultant`, `isConfidentialCompany`,
  `jobSource`.
- Newest first; sponsored jobs can appear out of order, so every job on the
  page is checked (Gotchas #4). At ~100 jobs/weekday (80-145 saved/day Oct 12-22)
  the 30-job first page is far more than one 3-minute interval produces.

## Health checks (`ScrapeHealth("naukrigulf")`)

`search_api` (the response arrived; the alert says why not: Akamai stream
reset, HTTP status, `puppeteer=true`, the app's "Oops! Something went wrong"
screen, or timeout), `search_structure`, `card_identity` (every job has a
jobId), `posted_date` (latestPostedDate readable), plus the shared job fields.

## Live behavior (production since 2026-10-09 22:56)

6,266 runs to 2026-10-24, 5,987 ok; 1,079 jobs saved. What is normal:
- **Empty weekends.** `freshness=1` empties on weekends: the first page has 0 jobs
  from Saturday morning, all of Sunday, and Monday until the first postings
  (Oct 10-12, 17-19, from 07:00 Oct 24). Runs stay `ok` (an empty search window
  is valid) and nothing is saved. Weekdays can also be briefly empty early in the
  morning.
- **`ERR_NETWORK_CHANGED` after every container switch** (each deploy and
  rollback since Oct 9). The scraper uses the host network; docker compose
  brings up the other containers' interfaces while the cycle it started is
  running, and NaukriGulf navigates ~1 s after Chrome starts (the other boards
  wait a few seconds first). The next run is ok and one failure does not alert,
  but three switches within an hour reach FLAP_FAILURES: 2026-10-24 14:52,
  14:54 and 15:05 (deploy, rollback, deploy) sent one `search_fetch` alert.
  Host network changes cause the same error outside deploys (Oct 22-23 nights).
  Fixed (`a89c078`, live 2026-10-24 16:44): a navigation aborted by
  `ERR_NETWORK_CHANGED` gets ONE more visit after 3 s (`boards/naukrigulf/scraper.scrape`,
  fresh spider on the same Chrome); every other failure keeps the one-visit rule.
  A failed navigation now names its error in `search_fetch` (was "completed
  without parsing a search response").
- **Host outages** show as `ERR_NAME_NOT_RESOLVED` on every board at once
  (Oct 21 18:00-22:00, Oct 23 14:00-18:00 Cairo): the machine's DNS, not the site.
- **Akamai soft block once:** Oct 17 01:36-03:27 Cairo, 37 runs with
  `ERR_HTTP2_PROTOCOL_ERROR` (its soft-block signature); it cleared on its own.

## Not yet verified live

- Geo parameters derive from the egress IP; `Location=Egypt` should dominate
  from another region, unverified.
