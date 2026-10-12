"""Fill a scratch DATA_DIR with fake jobs to try the web UI without scraping.

    DATA_DIR=/tmp/rtjobs-demo python -m web.demo_seed
    DATA_DIR=/tmp/rtjobs-demo uvicorn web.app:app --port 8000

Uses the scraper's own init_db(), so the schema is exactly production's.
"""

import json
import random
import sqlite3
from datetime import timedelta

from core import db
from core.clock import FORMAT, utcnow

TITLES = [
    ("Senior Python Developer", "software_engineering"), ("Frontend Engineer (React)", "software_engineering"),
    ("Data Analyst", "data_ai_analytics"), ("Machine Learning Engineer", "data_ai_analytics"),
    ("Accountant", "accounting_finance_banking"), ("Sales Executive", "sales_business_development"),
    ("Customer Service Representative", "customer_service_call_center"), ("HR Specialist", "human_resources_recruitment"),
    ("Digital Marketing Specialist", "marketing_ecommerce"), ("Civil Site Engineer", "engineering_construction"),
    ("DevOps Engineer", "it_cloud_cybersecurity"), ("UI/UX Designer", "design_creative"),
    ("Supply Chain Coordinator", "supply_chain_procurement_logistics"), ("Junior Backend Developer (Node.js)", "software_engineering"),
]
COMPANIES = ["Vodafone Egypt", "Valeo", "Instabug", "Swvl", "Fawry", "CIB", "Orange Egypt", "Talabat", "Breadfast", "IBM"]
PLACES = ["Cairo, Egypt", "Giza, Egypt", "Alexandria, Egypt", "New Cairo, Egypt", "Smart Village, Giza"]
SOURCES = ["linkedin", "wuzzuf", "wuzzuf", "linkedin", "indeed"]


def main(count: int = 160):
    random.seed(7)
    db.init_db()
    now = utcnow()
    with db.get_db() as conn:
        for i in range(count):
            title, family = random.choice(TITLES)
            source = random.choice(SOURCES)
            scraped = now - timedelta(minutes=int(random.expovariate(1 / 2500)) + i % 3)
            extra = {"location": random.choice(PLACES)}
            if random.random() < 0.3:
                extra["salary"] = f"{random.randint(10, 60)}K - {random.randint(61, 90)}K EGP"
            if random.random() < 0.5:
                extra["workplace"] = random.choice(["On-site", "Hybrid", "Remote"])
            cur = conn.execute(
                "INSERT OR IGNORE INTO jobs(source, external_id, title, company, posted_at, description, link,"
                " extra, scraped_at, notified) VALUES (?,?,?,?,?,?,?,?,?,1)",
                (source, f"demo-{i}", title, random.choice(COMPANIES), (scraped + timedelta(hours=3)).strftime(FORMAT),
                 f"We are hiring a {title}.\n\nResponsibilities:\n- Build and maintain things\n- Work with the team\n\n"
                 "Requirements:\n- 2+ years of experience\n- Good English",
                 f"https://example.com/{source}/{i}", json.dumps(extra), scraped.strftime(FORMAT)))
            if cur.rowcount and random.random() < 0.85:
                conn.execute(
                    "INSERT INTO job_enrichments(job_id, state, job_family, specialization, result_json, updated_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (cur.lastrowid, "ready", family, None,
                     json.dumps({"tools": random.sample(["Python", "SQL", "Excel", "React", "Docker", "SAP"], 2),
                                 "languages": ["English", "Arabic"]}), now.strftime(FORMAT)))
        for source, status in (("linkedin", "ok"), ("wuzzuf", "ok"), ("indeed", "degraded")):
            conn.execute("INSERT INTO runs(source, status, jobs_found, started_at, finished_at) VALUES (?,?,?,?,?)",
                         (source, status, random.randint(0, 8), (now - timedelta(minutes=2)).strftime(FORMAT),
                          (now - timedelta(minutes=1)).strftime(FORMAT)))
    print(f"seeded {count} demo jobs into {db.DB_PATH}")


if __name__ == "__main__":
    try:
        main()
    except sqlite3.Error as exc:
        raise SystemExit(f"seed failed: {exc}")
