# Job-family v2 validation — 2026-10-04

During staging, implementation and testing completed with enrichment and classified
delivery disabled. The subsequent cleanup/production request authorizes both;
see the deployment notes for its outcome. During these tests the production scraper
and its schedule were not replaced. All 26
channel IDs, titles and bot posting permissions passed the [read-only audit](channel-audit.md).

## Contract and persistence

`scripts/verify_offline.py` covers all 26 family destinations, family/sector
independence, valid specializations, title seniority preservation, fractional
experience, missing fields, exact batch IDs, result-cache isolation, retry/budget
fallbacks, migration, delivery disabled, malformed mappings and analytics. Existing
LinkedIn/Wuzzuf/Indeed parsing, browser and alert regression checks also pass.

The same suite passed inside the separately built Python 3.13 image
`rtjobs-scraper:job-family-v2` (`138afff0b340`), with networking disabled and no
production volumes mounted. `pip check` found no broken requirements. The local
Python 3.14 compile checks and `git diff --check` passed too. The test image was
not assigned to the scheduled production container.

A SQLite backup of production contained 38,651 raw jobs. Two migration passes
preserved job count, total description length, notification count and queue size;
SQLite quick_check passed. Three existing jobs were then explicitly queued only
in that disposable copy. Production historical jobs were not queued or rewritten.

## Live model checks

Model: `openai/gpt-6-luna` via the configured OpenRouter key.
Prompt/schema/taxonomy version: `cd32295f1c18c612`.

The initial 18-case run chose the intended job family for every case. It passed
17/18 exact assertions. The remaining assertion expected a generic management
specialization despite the fixture explicitly describing country P&L ownership;
the returned `country_regional_management` was consistent with that evidence.
The fixture was split into an unambiguous General Manager case and a Country
Manager case. Both targeted follow-up checks passed. The current 19-case fixture
set is therefore covered by 17 unchanged passing cases plus those two checks.

Coverage includes cross-sector professions, medical/property/technical sales,
DevOps, HSE, construction versus factory maintenance, recruitment and outsourcing,
explicit title seniority, fractional experience, tools versus generic skills,
language requirement/proficiency and unspecified salary currency/period.

Three actual saved postings also passed strict schema validation:

| Source / posting | Job family | Specialization | Employer sector |
|---|---|---|---|
| LinkedIn / Sales Engineer | sales_business_development | technical_presales | manufacturing_industrial |
| Wuzzuf / Senior Accountant | accounting_finance_banking | general_accounting | real_estate_property |
| Indeed / Senior Shopify Developer | software_engineering | frontend | fmcg_consumer_goods |

These are targeted checks, not an accuracy estimate for the full corpus. The
three-row analytics preview is explicitly labelled with its low coverage.

The read-only report scanned 29,186 postings within its 30-day window in 0.891s
with 10.31 MiB peak Python allocations under tracemalloc. The database file's
modification time stayed unchanged. Only three rows had v2 enrichment, so this
measurement does not predict memory use for a fully enriched corpus or include
the optional Plotly HTML rendering.

Provider-reported costs: $0.0052882 initial fixtures, $0.000483275 management
follow-up, $0.0016381 real-posting previews; total **$0.007409575**. The initial
fixture run reported 115,430 cached input tokens. Cache hits depend on the provider;
the SQLite result cache is independent and was verified offline.

No Telegram messages were sent by these checks. Channel checks call only getMe,
getChat and getChatMember, so they do not consume Indeed login-code updates.

## Local artifacts

- `/tmp/rtjobs-job-family-channel-check.json`
- `/tmp/rtjobs-job-family-evaluation.json`
- `/tmp/rtjobs-management-evaluation.json`
- `/tmp/rtjobs-family-validation-7084ag_c/live-preview.json`
- `/tmp/rtjobs-family-validation-7084ag_c/analytics.json`
- `/tmp/rtjobs-family-validation-7084ag_c/dashboard.html`
- `/tmp/rtjobs-family-offline.log`
- `/tmp/rtjobs-family-container-check.log`
- `/tmp/rtjobs-family-validation-7084ag_c/analytics-check.json`

Raw database copies and per-posting reports stay outside version control.

## Routing rules update — 2026-10-24

User decisions (2026-10-24): AI-training gigs follow the expertise they require;
selling titles go to Sales and call-center/customer-service titles to Customer
Service, whatever the duties lean to. Added to `markup/enrichment/prompt.txt`
with them: office cashiers -> Finance/treasury (point-of-sale -> Retail, in
restaurants/hotels -> Hospitality); drivers and couriers -> Logistics; posts that
are not a real vacancy (junk text, a description of an unrelated role, a poster's
own "Owner" listing) -> `other`, never General Management. A short, empty or
boilerplate description, a title fragment, a neighboring role in the same field
and an agency describing its client are explicitly NOT junk (the first draft sent
five real jobs to `other`; the second draft fixed all five). `evaluation.json`
gained 12 cases (ids 20-31) for these rules.

Measured with `openai/gpt-6-luna`, production Enricher on throwaway data, version
`471001db34d1b320` (previous `fc50eb290846c12b`):

| Set | Old prompt | New prompt |
|---|---|---|
| 19 original synthetic cases | 76/76 assertions | 76/76 |
| 12 new synthetic cases | 7/10 families (2 not run) | 12/12 families, 22/22 assertions |
| 94 hand-judged real jobs (new rules as the key) | 82/94 | 92/94 |
| production labels on the same 94 | 80/94 | — |

The real jobs are 60 random plus 43 hard production jobs (Oct 15-24); the key
follows the new rules, so the old prompt's score partly measures the policy
change. 88/103 real jobs kept their family; 11 of the 15 changes are the new
rules. Remaining misses: "Principal Owner" with another organization's description
still went to General Management; "Marketing Communications Specialist" whose
German description is a sales-development role went to `other`; a Sales
Coordinator with order-processing duties went to Sales. A second set of 94
production jobs from the affected categories (AI gigs, drivers, cashiers,
sales/customer-service titles, low-confidence General Management) confirmed the
rules (every actual driver job -> Logistics; the other 4 "driver" titles were a
train-driver trainer, a driver recruiter, a PA-and-driver and van sales reps;
Synthires STEM -> Science, Mercor
finance expert -> Finance, call-center titles -> Customer Service). Cost and
latency unchanged ($0.39 vs $0.375 per 1k jobs, same p50). Total spend $0.23.
