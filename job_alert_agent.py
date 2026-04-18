# Phase-3, Step-1
# What this part does:
# 1. Runs your job alert agent as a standalone Python script
# 2. Collects recent analyst jobs from Google News RSS
# 3. Filters to last 24 hours
# 4. Removes obvious senior-only roles
# 5. Scores and ranks jobs for your target roles
# 6. Prevents duplicate Telegram alerts across runs
# 7. Saves a running CSV database
# 8. Makes the project ready for GitHub Actions scheduling

import os
import requests
import feedparser
import pandas as pd
from urllib.parse import quote
from dateutil import parser as dtparser
from datetime import datetime, timedelta, timezone


import re
from datetime import datetime, timezone

# =========================
# STEP 1: QUALITY + SCORING + ALERT SPLIT
# Drop this into job_alert_agent.py
# =========================

TOP_N = 50
TOP_JOBS_COUNT = 15  # first section in Telegram alert
PRIORITY_LOCATIONS = [
    "toronto", "calgary", "remote", "canada", "greater toronto area", "gta"
]

TARGET_ROLE_KEYWORDS = {
    "Data Analyst": [
        "data analyst", "analytics analyst", "reporting analyst",
        "business intelligence analyst", "bi analyst", "insights analyst"
    ],
    "Business Analyst": [
        "business analyst", "systems analyst", "functional analyst",
        "process analyst", "business systems analyst"
    ]
}

GOOD_EXTRA_KEYWORDS = [
    "sql", "power bi", "tableau", "excel", "dashboard", "reporting",
    "analytics", "kpi", "stakeholder", "requirements", "etl", "python"
]

BAD_TITLE_KEYWORDS = [
    "senior", "sr.", "lead", "principal", "manager", "director", "vp", "vice president",
    "architect", "staff", "head", "president", "consultant", "specialist",
    "scientist", "engineer", "developer", "intern", "co-op", "coop", "student"
]

BAD_CONTENT_KEYWORDS = [
    "news", "article", "press release", "hiring trends", "career advice",
    "how to become", "salary guide", "interview tips", "sponsored", "market update"
]

EXCLUDED_COMPANY_PATTERNS = [
    "youtube", "reddit", "facebook", "instagram", "tiktok"
]

def clean_text(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text

def safe_get(row, key, default=""):
    return row.get(key, default) if isinstance(row, dict) else default

def is_probable_job_post(row):
    title = clean_text(safe_get(row, "title"))
    source = clean_text(safe_get(row, "source"))
    link = clean_text(safe_get(row, "link"))
    summary = clean_text(safe_get(row, "summary"))

    combined = f"{title} {source} {summary} {link}"

    # must have at least one target role signal
    role_hit = any(
        kw in combined
        for role_list in TARGET_ROLE_KEYWORDS.values()
        for kw in role_list
    )
    if not role_hit:
        return False

    # reject obvious junk/news/social links
    if any(bad in combined for bad in BAD_CONTENT_KEYWORDS):
        return False

    if any(site in combined for site in EXCLUDED_COMPANY_PATTERNS):
        return False

    # reject senior / off-target titles
    if any(bad in title for bad in BAD_TITLE_KEYWORDS):
        return False

    return True

def bucket_role(title):
    t = clean_text(title)

    if any(k in t for k in TARGET_ROLE_KEYWORDS["Data Analyst"]):
        return "Data Analyst"
    if any(k in t for k in TARGET_ROLE_KEYWORDS["Business Analyst"]):
        return "Business Analyst"

    # optional broader buckets if you want to keep them
    if "product analyst" in t:
        return "Product Analyst"
    if "operations analyst" in t:
        return "Operations Analyst"
    if "reporting analyst" in t:
        return "Reporting Analyst"
    if "insights analyst" in t:
        return "Insights Analyst"
    if "business intelligence analyst" in t or "bi analyst" in t:
        return "BI Analyst"

    return "Other Analyst"

def score_job(row):
    title = clean_text(safe_get(row, "title"))
    location = clean_text(safe_get(row, "location"))
    summary = clean_text(safe_get(row, "summary"))
    company = clean_text(safe_get(row, "company_name"))
    source = clean_text(safe_get(row, "source"))

    text_all = f"{title} {location} {summary} {company} {source}"

    score = 0

    # strong role match
    if any(k in title for k in TARGET_ROLE_KEYWORDS["Data Analyst"]):
        score += 40
    elif any(k in title for k in TARGET_ROLE_KEYWORDS["Business Analyst"]):
        score += 40
    elif "analyst" in title:
        score += 15

    # extra skill/context signals
    for kw in GOOD_EXTRA_KEYWORDS:
        if kw in text_all:
            score += 4

    # location priority
    if "toronto" in location or "greater toronto area" in location or "gta" in location:
        score += 20
    if "calgary" in location:
        score += 18
    if "remote" in location and "canada" in text_all:
        score += 22
    elif "remote" in location:
        score += 12
    elif "canada" in location:
        score += 10

    # demote vague or low-value postings
    if "contract" in text_all:
        score += 3
    if "temporary" in text_all:
        score -= 3
    if "part-time" in text_all:
        score -= 5

    # demote titles that slipped through
    if any(bad in title for bad in BAD_TITLE_KEYWORDS):
        score -= 50

    return score

def filter_and_rank_jobs(job_rows):
    """
    job_rows: list of dicts
    expected fields if available:
      title, location, summary, link, source, company_name, published
    """
    cleaned = []

    for row in job_rows:
        if not is_probable_job_post(row):
            continue

        title = safe_get(row, "title")
        role_bucket = bucket_role(title)
        fit_score = score_job(row)

        if fit_score < 30:
            continue

        row["role_bucket"] = role_bucket
        row["fit_score"] = fit_score
        row["timestamp"] = datetime.now(timezone.utc).isoformat()

        cleaned.append(row)

    cleaned = sorted(cleaned, key=lambda x: x["fit_score"], reverse=True)
    return cleaned[:TOP_N]

def build_telegram_message(jobs):
    if not jobs:
        return "No strong job matches found in this run."

    top_jobs = jobs[:TOP_JOBS_COUNT]
    additional_jobs = jobs[TOP_JOBS_COUNT:]

    lines = []
    lines.append("📌 Top Job Matches\n")

    for i, job in enumerate(top_jobs, start=1):
        title = safe_get(job, "title", "Unknown Title")
        company = safe_get(job, "company_name", "Unknown Company")
        location = safe_get(job, "location", "Unknown Location")
        score = safe_get(job, "fit_score", 0)
        link = safe_get(job, "link", "")

        lines.append(
            f"{i}. {title}\n"
            f"🏢 {company}\n"
            f"📍 {location}\n"
            f"🎯 Score: {score}\n"
            f"{link}\n"
        )

    if additional_jobs:
        lines.append("\n📎 Additional Matches\n")
        for i, job in enumerate(additional_jobs, start=1):
            title = safe_get(job, "title", "Unknown Title")
            company = safe_get(job, "company_name", "Unknown Company")
            location = safe_get(job, "location", "Unknown Location")
            link = safe_get(job, "link", "")

            lines.append(
                f"{i}. {title} | {company} | {location}\n{link}\n"
            )

    return "\n".join(lines)

# =========================
# HOW TO USE IN YOUR EXISTING FLOW
# =========================
#
# After you collect raw jobs, do this:
#
# ranked_jobs = filter_and_rank_jobs(raw_jobs)
#
# Then continue your existing dedupe using sent_jobs.csv
# Then send Telegram alert with:
#
# message = build_telegram_message(new_jobs_after_dedupe)
# send_telegram_message(message)
#
# And keep saving to job_database.csv exactly as you already do.
#
# Make sure each row has at least these keys if possible:
# title, company_name, location, summary, link, source
#
# =========================
# Phase-3, Step-1
# What this part does:
# Configuration for search, scoring, file outputs, and Telegram
# =========================

KEYWORDS = [
    "data analyst",
    "business analyst",
    "product analyst",
    "business intelligence analyst",
    "reporting analyst",
    "operations analyst",
    "insights analyst",
]

LOCATIONS = [
    "Toronto",
    "Calgary",
    '"Remote Canada"',
    "Ontario",
    "Edmonton",
    "Vancouver",
]

SITE_HINTS = [
    "site:linkedin.com/jobs",
    "site:ca.indeed.com",
    "site:jobs.lever.co",
    "site:boards.greenhouse.io",
    "site:workdayjobs.com",
    "site:jobs.ashbyhq.com",
]

EXCLUDE_SENIOR_TERMS = [
    "senior",
    "lead",
    "principal",
    "manager",
    "director",
    "head",
    "vp",
    "vice president",
    "staff ",
]

PREFER_TERMS = [
    "business analyst",
    "data analyst",
    "product analyst",
    "business intelligence analyst",
    "reporting analyst",
    "operations analyst",
    "insights analyst",
    "analyst",
    "business intelligence",
]

TARGET_ROLE_BONUS = {
    "business analyst": 6,
    "data analyst": 6,
    "product analyst": 5,
    "business intelligence analyst": 5,
    "reporting analyst": 4,
    "operations analyst": 4,
    "insights analyst": 4,
}

CITY_PRIORITY = {
    "Toronto": 5,
    "Calgary": 5,
    '"Remote Canada"': 5,
    "Ontario": 3,
    "Edmonton": 2,
    "Vancouver": 2,
}

TOP_N = 50
SENT_FILE = "sent_jobs.csv"
DATABASE_FILE = "job_database.csv"

# =========================
# Phase-3, Step-1
# What this part does:
# Text normalization, filtering, scoring, company extraction, and role tagging helpers
# =========================

def normalize_text(x):
    return str(x).strip().lower()

def looks_like_job_title(title: str) -> bool:
    t = normalize_text(title)
    good = any(k in t for k in KEYWORDS)
    bad_terms = [
        "salary", "how to", "career", "unemployment", "tips", "pros", "news",
        "hiring right now", "finance enthusiast", "returning to the firm",
        "course", "bootcamp", "certificate", "program", "podcast", "article", "blog",
    ]
    bad = any(b in t for b in bad_terms)
    return good and not bad

def published_within_24h(published_text: str) -> bool:
    try:
        pub_dt = dtparser.parse(published_text)
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        return pub_dt >= now_utc - timedelta(hours=24)
    except Exception:
        return False

def is_too_senior(title: str) -> bool:
    t = normalize_text(title)
    return any(term in t for term in EXCLUDE_SENIOR_TERMS)

def source_type_from_title_and_link(title: str, raw_link: str) -> str:
    title_l = normalize_text(title)
    link_l = normalize_text(raw_link)

    if "linkedin" in title_l or "linkedin.com/jobs" in link_l:
        return "linkedin"
    if "indeed" in title_l or "ca.indeed.com" in link_l:
        return "indeed"
    if "jobs.lever.co" in link_l:
        return "lever"
    if "boards.greenhouse.io" in link_l:
        return "greenhouse"
    if "workdayjobs.com" in link_l:
        return "workday"
    if "ashbyhq" in link_l:
        return "ashby"
    return "other"

def looks_staffing_like(title: str) -> bool:
    t = normalize_text(title)
    staffing_terms = [
        "insight global", "rose international", "systems inc", "solutions inc",
        "consulting llc", "staffing", "recruitment", "talent",
    ]
    return any(term in t for term in staffing_terms)

def extract_company_name(title: str) -> str:
    t = str(title).strip()

    import re
    patterns = [
        r"^(.*?)\s+hiring\s+",
        r"^(.*?)\s*[-–]\s*.*$",
    ]

    for pattern in patterns:
        match = re.match(pattern, t, flags=re.IGNORECASE)
        if match:
            company = match.group(1).strip(" -–|")
            if company:
                return company

    if " in " in t.lower():
        left = re.split(r"\s+in\s+", t, maxsplit=1, flags=re.IGNORECASE)[0].strip(" -–|")
        if left:
            return left

    return "Unknown"

def assign_role_bucket(title: str) -> str:
    t = normalize_text(title)

    if "business intelligence analyst" in t or "bi analyst" in t:
        return "BI Analyst"
    if "business analyst" in t:
        return "Business Analyst"
    if "data analyst" in t:
        return "Data Analyst"
    if "product analyst" in t:
        return "Product Analyst"
    if "reporting analyst" in t:
        return "Reporting Analyst"
    if "operations analyst" in t:
        return "Operations Analyst"
    if "insights analyst" in t:
        return "Insights Analyst"
    return "Other Analyst"

def score_job(row):
    title = normalize_text(row["title"])
    location = row["location"]
    source_type = row["source_type"]

    score = 0

    for phrase, bonus in TARGET_ROLE_BONUS.items():
        if phrase in title:
            score += bonus

    for term in PREFER_TERMS:
        if term in title:
            score += 1

    score += CITY_PRIORITY.get(location, 0)

    source_bonus = {
        "lever": 4,
        "greenhouse": 4,
        "workday": 4,
        "ashby": 4,
        "indeed": 2,
        "linkedin": 2,
        "other": 0,
    }
    score += source_bonus.get(source_type, 0)

    if is_too_senior(title):
        score -= 8

    if looks_staffing_like(title):
        score -= 2

    return score

# =========================
# Phase-3, Step-1
# What this part does:
# Collect jobs quickly from RSS
# =========================

def collect_jobs():
    rows = []

    for keyword in KEYWORDS:
        for location in LOCATIONS:
            search_q = f'"{keyword}" {location} (' + " OR ".join(SITE_HINTS) + ")"
            rss_url = f"https://news.google.com/rss/search?q={quote(search_q)}"
            feed = feedparser.parse(rss_url)

            for entry in feed.entries:
                raw_link = entry.get("link", "").strip()
                title = entry.get("title", "").strip()

                rows.append({
                    "keyword": keyword,
                    "location": location,
                    "title": title,
                    "raw_link": raw_link,
                    "link": raw_link,
                    "published": entry.get("published", "").strip(),
                    "source_type": source_type_from_title_and_link(title, raw_link),
                })

    return pd.DataFrame(rows)

# =========================
# Phase-3, Step-1
# What this part does:
# Filter, enrich, and rank collected jobs
# =========================

def clean_and_rank(df):
    if df.empty:
        return df

    df = df.copy()
    df["is_job_like"] = df["title"].apply(looks_like_job_title)
    df["within_24h"] = df["published"].apply(published_within_24h)
    df["too_senior"] = df["title"].apply(is_too_senior)
    df["staffing_like"] = df["title"].apply(looks_staffing_like)

    filtered_df = (
        df[(df["is_job_like"]) & (df["within_24h"])]
        .drop_duplicates(subset=["title", "raw_link"])
        .reset_index(drop=True)
    )

    if filtered_df.empty:
        return filtered_df

    filtered_df["company_name"] = filtered_df["title"].apply(extract_company_name)
    filtered_df["role_bucket"] = filtered_df["title"].apply(assign_role_bucket)
    filtered_df["fit_score"] = filtered_df.apply(score_job, axis=1)

    final_df = (
        filtered_df[~filtered_df["too_senior"]]
        .sort_values(["fit_score", "staffing_like", "published"], ascending=[False, True, False])
        .reset_index(drop=True)
    )

    return final_df

# =========================
# Phase-3, Step-1
# What this part does:
# Sent jobs storage for Telegram deduplication
# =========================

def load_sent_jobs():
    if not os.path.exists(SENT_FILE):
        return pd.DataFrame(columns=["title", "raw_link"])

    try:
        sent_df = pd.read_csv(SENT_FILE)
    except Exception:
        return pd.DataFrame(columns=["title", "raw_link"])

    if "title" not in sent_df.columns:
        sent_df["title"] = ""

    if "raw_link" not in sent_df.columns:
        if "link" in sent_df.columns:
            sent_df["raw_link"] = sent_df["link"]
        else:
            sent_df["raw_link"] = ""

    return sent_df[["title", "raw_link"]].copy()

def get_new_jobs(final_df):
    sent_df = load_sent_jobs()

    if sent_df.empty:
        return final_df.copy().reset_index(drop=True)

    sent_keys = set(zip(sent_df["title"].astype(str), sent_df["raw_link"].astype(str)))

    new_df = final_df[
        ~final_df.apply(
            lambda row: (str(row["title"]), str(row["raw_link"])) in sent_keys,
            axis=1
        )
    ].copy().reset_index(drop=True)

    return new_df

def save_sent_jobs(new_jobs_df):
    if new_jobs_df.empty:
        print("No new jobs to save in sent_jobs.csv")
        return

    sent_df = load_sent_jobs()
    updated_df = pd.concat(
        [sent_df[["title", "raw_link"]], new_jobs_df[["title", "raw_link"]]],
        ignore_index=True
    ).drop_duplicates().reset_index(drop=True)

    updated_df.to_csv(SENT_FILE, index=False)
    print(f"Saved {len(updated_df)} total records to {SENT_FILE}")

# =========================
# Phase-3, Step-1
# What this part does:
# Full job database for later analysis
# =========================

def load_job_database():
    expected_cols = [
        "run_timestamp_utc", "keyword", "location", "source_type", "company_name",
        "role_bucket", "title", "published", "raw_link", "fit_score",
        "staffing_like", "too_senior",
    ]

    if not os.path.exists(DATABASE_FILE):
        return pd.DataFrame(columns=expected_cols)

    try:
        db_df = pd.read_csv(DATABASE_FILE)
    except Exception:
        return pd.DataFrame(columns=expected_cols)

    for col in expected_cols:
        if col not in db_df.columns:
            db_df[col] = ""

    return db_df[expected_cols].copy()

def update_job_database(new_jobs_df):
    if new_jobs_df.empty:
        print("No new jobs to save in job_database.csv")
        return

    db_df = load_job_database()
    add_df = new_jobs_df.copy()
    add_df["run_timestamp_utc"] = datetime.now(timezone.utc).isoformat()

    keep_cols = [
        "run_timestamp_utc", "keyword", "location", "source_type", "company_name",
        "role_bucket", "title", "published", "raw_link", "fit_score",
        "staffing_like", "too_senior",
    ]

    for col in keep_cols:
        if col not in add_df.columns:
            add_df[col] = ""

    add_df = add_df[keep_cols]

    updated_db = pd.concat([db_df, add_df], ignore_index=True)
    updated_db = updated_db.drop_duplicates(subset=["title", "raw_link"]).reset_index(drop=True)
    updated_db.to_csv(DATABASE_FILE, index=False)
    print(f"Saved {len(updated_db)} total records to {DATABASE_FILE}")

# =========================
# Phase-3, Step-1
# What this part does:
# Telegram alert sender
# =========================

def send_telegram(df):
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    if df.empty:
        message = "📭 No new good-fit analyst jobs found in the last 24 hours."
    else:
        send_df = df.head(TOP_N).copy().reset_index(drop=True)
        lines = [f"📌 Job Alert Agent\nTop {len(send_df)} new best-fit jobs from the last 24 hours\n"]

        for i, row in send_df.iterrows():
            lines.append(
                f"{i+1}. {row['title']}\n"
                f"Company: {row['company_name']}\n"
                f"Role Bucket: {row['role_bucket']}\n"
                f"Search Location: {row['location']}\n"
                f"Source Type: {row['source_type']}\n"
                f"Fit Score: {row['fit_score']}\n"
                f"Published: {row['published']}\n"
                f"{row['link']}\n"
            )

        message = "\n".join(lines)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    chunk_size = 3500
    chunks = [message[i:i+chunk_size] for i in range(0, len(message), chunk_size)]

    for idx, chunk in enumerate(chunks, start=1):
        payload = {"chat_id": chat_id, "text": chunk}
        response = requests.post(url, data=payload, timeout=30)
        print(f"Chunk {idx}: {response.status_code}")
        print(response.text)

# =========================
# Phase-3, Step-1
# What this part does:
# Main run flow
# =========================

def main():
    raw_df = collect_jobs()
    print("Total collected:", len(raw_df))

    final_df = clean_and_rank(raw_df)
    print("Final jobs after filtering:", len(final_df))

    new_jobs_df = get_new_jobs(final_df)
    print("New jobs not sent before:", len(new_jobs_df))

    update_job_database(new_jobs_df)
    send_telegram(new_jobs_df)
    save_sent_jobs(new_jobs_df)

if __name__ == "__main__":
    main()
