"""Shared runtime logic for the Joveo Publisher Intelligence job.

This module keeps the existing behavior intact while making the job reusable
from both a local CLI run (`python brief.py`) and a Vercel HTTP function.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import gspread
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai
from google.oauth2.service_account import Credentials
from tavily import TavilyClient

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# These defaults keep local behavior predictable while still allowing Vercel
# deployments to override everything through environment variables.
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash-lite"
DEFAULT_GOOGLE_SHEET_NAME = "Joveo Intel Logs"
DEFAULT_GOOGLE_WORKSHEET_NAME = "Sheet1"
NEWS_LOOKBACK_DAYS = 7
GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def require_env(name: str) -> str:
    # Centralized required-env validation keeps startup errors explicit and
    # avoids partial runs where one downstream dependency silently fails.
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} not set")
    return value


def get_runtime_config() -> dict[str, str]:
    # This function gathers all runtime configuration in one place so both the
    # CLI flow and the Vercel function use the exact same settings.
    return {
        "slack_webhook_url": require_env("SLACK_WEBHOOK_URL"),
        "gemini_api_key": require_env("GEMINI_API_KEY"),
        "tavily_api_key": require_env("TAVILY_API_KEY"),
        "gemini_model": os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL),
        "google_sheet_name": os.getenv("GOOGLE_SHEET_NAME", DEFAULT_GOOGLE_SHEET_NAME),
        "google_worksheet_name": os.getenv(
            "GOOGLE_WORKSHEET_NAME",
            DEFAULT_GOOGLE_WORKSHEET_NAME,
        ),
    }


def get_gemini_client(config: dict[str, str]) -> genai.Client:
    # The new Google GenAI SDK centers all model calls on a Client object,
    # which is the officially recommended replacement for google.generativeai.
    return genai.Client(api_key=config["gemini_api_key"])


def get_google_service_account_info() -> dict[str, Any]:
    # Google Sheets access is driven by a service-account JSON blob. In Vercel
    # that should come from an env var; locally we still allow a file fallback.
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    # Vercel cannot rely on a local credentials file, so we prefer the env var.
    # The file fallback keeps local development working without changing behavior.
    if raw_json:
        service_account_info = json.loads(raw_json)
    else:
        credentials_path = BASE_DIR / "credentials.json"
        if not credentials_path.exists():
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON not set and credentials.json not found"
            )

        with credentials_path.open(encoding="utf-8") as credentials_file:
            service_account_info = json.load(credentials_file)

    private_key = service_account_info.get("private_key")
    if isinstance(private_key, str):
        # When JSON is copied into an env var, newlines are often escaped. We
        # normalize them here so Google auth receives a valid private key.
        service_account_info["private_key"] = private_key.replace("\\n", "\n")

    return service_account_info


def get_sheet(config: dict[str, str]):
    # This creates an authenticated worksheet client for reading previously
    # posted URLs and appending newly-sent URLs after a successful Slack post.
    credentials = Credentials.from_service_account_info(
        get_google_service_account_info(),
        scopes=GOOGLE_SCOPES,
    )
    client = gspread.authorize(credentials)
    workbook = client.open(config["google_sheet_name"])
    worksheet_name = config.get("google_worksheet_name")

    if worksheet_name:
        return workbook.worksheet(worksheet_name)

    return workbook.sheet1


P0_PUBLISHERS = [
    "employers.io",
    "Joblift",
    "JobGet",
    "Snagajob",
    "Jobcase",
    "Monster",
    "Allthetopbananas",
    "JobRapido",
    "Talent.com",
    "Talroo",
    "ZipRecruiter",
    "OnTimeHire",
    "Indeed",
    "Sercanto",
    "YadaJobs",
    "Hokify",
    "Upward.net",
    "JobCloud",
    "Jooble",
    "Nurse.com",
    "Geographic Solutions",
    "Reed",
    "Jobbsafari.se",
    "Jobbland",
    "Handshake",
    "1840",
]

P1_P2_PUBLISHERS = [
    "JobSwipe",
    "Jobbird.de",
    "Tideri",
    "Manymore.jobs",
    "ClickaJobs",
    "MyJobScanner",
    "Job Traffic",
    "Jobtome",
    "Propel",
    "AllJobs",
    "Jora",
    "EarnBetter",
    "WhatJobs",
    "J-Vers",
    "Adzuna",
    "Galois",
    "Mindmatch.ai",
    "Myjobhelper",
    "TransForce",
    "CV Library",
    "CDLlife",
    "PlacedApp",
    "IrishJobs",
    "Praca.pl",
    "AppJobs",
    "OfferUp",
    "JobsInNetwork",
    "Jobsora",
    "StellenSMS",
    "Dice",
    "SonicJobs",
    "Botson.ai",
    "CMP Jobs",
    "Health Ecareers",
    "Hokify",
    "JobHubCentral",
    "BoostPoint",
    "Jobs In Japan",
    "Daijob.com",
    "GaijinPot",
    "GoWork.pl",
    "deBanenSite.nl",
    "Pracuj.pl",
    "Xing",
    "PostJobFree",
    "Jobsdb",
    "Stellenanzeigen.de",
    "Jobs.at",
    "Jobs.ch",
    "JobUp",
    "Jobwinner",
    "Topjobs.ch",
    "Vetted Health",
    "Arya by Leoforce",
    "Welcome to the Jungle",
    "JobMESH",
    "Bakeca.it",
    "Stack Overflow",
    "Diversity Jobs",
    "Laborum",
    "Curriculum",
    "American Nurses Association",
    "Profesia",
    "CareerCross",
    "Jobs.ie",
    "Nexxt",
    "Resume-Library.com",
    "Women for Hire",
    "Professional Diversity Network",
    "Rabota.bg",
    "Zaplata.bg",
    "Jobnet",
    "New Zealand Jobs",
    "Nationale Vacaturebank",
    "Intermediair",
    "eFinancialCareers",
    "Profession.hu",
    "Job Bank",
    "Personalwerk",
    "Yapo",
    "Karriere.at",
    "SAPO Emprego",
    "Catho",
    "Totaljobs",
    "Handshake",
    "Ladders.com",
    "Gumtree",
    "Instawork",
    "LinkedIn",
    "Facebook",
    "Instagram",
    "Google Ads",
    "Craigslist",
    "Reddit",
    "YouTube",
    "Spotify",
    "Jobbland",
    "Wonderkind",
    "adway.ai",
    "HeyTempo",
    "Otta",
    "Info Jobs",
    "Vagas",
    "Visage Jobs",
    "Hunar.ai",
    "CollabWORK",
    "Arbeitnow",
    "Doximity",
    "VietnamWorks",
    "JobKorea",
    "JobIndex",
    "HH.ru",
    "Consultants 500",
    "YM Careers",
    "Dental Post",
    "Foh and Boh",
    "Study Smarter",
    "Pnet",
    "Remote.co",
    "FATj",
    "Expresso Emprego",
    "Bravado",
]

# Sort P1/P2 alphabetically and split into 3 batches for weekly rotation.
P1_P2_SORTED = sorted(P1_P2_PUBLISHERS)
BATCH_SIZE = len(P1_P2_SORTED) // 3
P1_P2_BATCHES = [
    P1_P2_SORTED[:BATCH_SIZE],
    P1_P2_SORTED[BATCH_SIZE : BATCH_SIZE * 2],
    P1_P2_SORTED[BATCH_SIZE * 2 :],
]


def get_todays_publishers():
    # The weekday schedule decides which publisher group is covered each day.
    # Monday/Thursday cover P0, while Tue/Wed/Fri rotate through P1/P2 batches.
    today = datetime.date.today()
    weekday = today.weekday()  # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri
    week_num = today.isocalendar()[1]

    # Tuesday/Wednesday/Friday rotate through the three P1/P2 batches each week.
    schedule = {
        0: ("P0", P0_PUBLISHERS, "P0 publishers", "P1/P2 Batch 1 Tuesday"),
        1: (
            "P1/P2 Batch 1",
            P1_P2_BATCHES[week_num % 3],
            "P1/P2 Batch 1",
            "P1/P2 Batch 2 Wednesday",
        ),
        2: (
            "P1/P2 Batch 2",
            P1_P2_BATCHES[(week_num + 1) % 3],
            "P1/P2 Batch 2",
            "P1/P2 Batch 3 Friday",
        ),
        3: ("P0", P0_PUBLISHERS, "P0 publishers", "P1/P2 Batch 3 Friday"),
        4: (
            "P1/P2 Batch 3",
            P1_P2_BATCHES[(week_num + 2) % 3],
            "P1/P2 Batch 3",
            "P0 publishers Monday",
        ),
    }

    if weekday not in schedule:
        return None, None, None, None

    label, publishers, coverage_label, next_label = schedule[weekday]
    return label, publishers, coverage_label, next_label


def fetch_news(publishers, tavily_client: TavilyClient):
    # For each publisher in today’s batch, query Tavily for recent strategic
    # signals such as launches, partnerships, layoffs, growth, and funding.
    all_results = []

    for pub in publishers:
        query = (
            f'("{pub}") AND (funding OR acquisition OR hiring OR layoffs OR '
            "product launch OR expansion OR launch OR feature OR product OR "
            "update OR new OR ai OR platform OR tool OR partnership OR "
            "integration OR growth OR strategy) AND (last 7 days)"
        )

        try:
            results = tavily_client.search(
                query=query,
                search_depth="advanced",
                max_results=3,
                days=NEWS_LOOKBACK_DAYS,
            )
            all_results.extend(results["results"])
        except Exception as exc:
            print(f"Search failed for {pub}: {exc}")

    return all_results


def fetch_article_date(url):
    # Some search results do not include reliable published dates, so we fetch
    # the article HTML and inspect common meta tags for a timestamp.
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")

        meta_tags = [
            {"property": "article:published_time"},
            {"name": "article:published_time"},
            {"property": "og:published_time"},
            {"name": "pubdate"},
            {"name": "publish-date"},
        ]

        for tag in meta_tags:
            meta = soup.find("meta", tag)
            if meta and meta.get("content"):
                try:
                    return datetime.datetime.fromisoformat(
                        meta["content"].replace("Z", "+00:00")
                    )
                except ValueError:
                    continue
    except Exception as exc:
        print(f"Failed to fetch date from {url}: {exc}")

    return None


def extract_date_from_text(text):
    # This is a lightweight fallback that looks for ISO-style dates embedded in
    # the search snippet when metadata and HTML parsing do not yield a date.
    if not text:
        return None

    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if not match:
        return None

    try:
        return datetime.datetime.fromisoformat(match.group())
    except ValueError:
        return None


def quick_filter(news):
    # Remove obviously stale URLs by screening for old years in the path before
    # we spend more effort ranking or date-validating the result set.
    filtered = []
    current_year = datetime.datetime.now().year

    for item in news:
        url = item.get("url", "")

        if any(str(year) in url for year in range(2012, current_year - 1)):
            continue

        if any(f"/{year}/" in url for year in range(2012, current_year - 1)):
            continue

        filtered.append(item)

    return filtered


def soft_rank_and_limit(news, limit=15):
    # Tavily can return a broad set of matches. This scoring pass nudges likely
    # strategic updates to the top so the later validation work stays focused.
    def score(item):
        text = (item.get("title", "") + " " + item.get("content", "")).lower()
        score_value = 0

        keywords = [
            "launch",
            "feature",
            "product",
            "update",
            "new",
            "ai",
            "platform",
            "tool",
            "partnership",
            "integration",
            "expansion",
            "growth",
            "hiring",
            "strategy",
        ]

        for keyword in keywords:
            if keyword in text:
                score_value += 1

        score_value += min(len(text) // 200, 3)
        return score_value

    return sorted(news, key=score, reverse=True)[:limit]


def is_current_year_url(url):
    # Current-year URLs are treated as a helpful freshness signal for sites that
    # bake publish year into article paths.
    current_year = datetime.datetime.now().year
    return f"/{current_year}/" in url


def is_aggregator_page(url):
    # These patterns usually point to roundup or tracker pages instead of a
    # single publisher-specific update, so we exclude them from the digest.
    bad_patterns = [
        "mass-layoffs",
        "layoff-tracker",
        "layoffs-tracker",
        "job-cuts",
        "job-losses",
        "companies-that",
        "company-list",
        "list-of",
        "roundup",
        "weekly-roundup",
        "monthly-roundup",
        "latest-updates",
        "industry-updates",
        "market-update",
    ]

    url_lower = url.lower()
    return any(pattern in url_lower for pattern in bad_patterns)


def filter_recent_news(results):
    # Keep only news that appears to be within the configured lookback window.
    # We combine Tavily metadata, HTML parsing, and snippet parsing as fallbacks.
    filtered = []
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=NEWS_LOOKBACK_DAYS
    )

    for item in results:
        url = item.get("url", "")
        if is_aggregator_page(url):
            continue

        # Many publishers include the current year in timely article URLs, so we
        # can keep those without spending an extra request on HTML parsing.
        if is_current_year_url(url):
            filtered.append(item)
            continue

        pub_date = None

        if item.get("published_date"):
            try:
                pub_date = datetime.datetime.fromisoformat(item["published_date"])
            except ValueError:
                pub_date = None

        if not pub_date:
            pub_date = fetch_article_date(url)

        if not pub_date:
            pub_date = extract_date_from_text(item.get("content", ""))

        if pub_date is None:
            continue

        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=datetime.timezone.utc)

        if pub_date < cutoff:
            continue

        filtered.append(item)

    return filtered


def generate_brief(news_data, coverage_label, config: dict[str, str]):
    # Gemini turns the curated result set into the final Slack-ready digest while
    # preserving the requested formatting and limiting output to the strongest items.
    today = datetime.date.today().strftime("%A, %d %B %Y")
    context = "\n\n".join(
        [
            f"TITLE: {item['title']}\nURL: {item['url']}\nCONTENT: {item['content'][:300]}"
            for item in news_data
        ]
    )

    prompt = f"""
You are the Joveo Publisher Intelligence Agent.

Today is {today}.

Below is REAL-TIME news data collected from the web:

{context}

TASK:
From this data, select the TOP 5 most impactful updates relevant to Joveo.

OUTPUT FORMAT:

📡 *Joveo Publisher Intel*
📅 {today}

━━━━━━━━━━━━━━━━━━

For each item:

[Impact Emoji] *[Publisher Name]*
[One sentence insight explaining what happened + why it matters to Joveo]

Source | 🔗 <URL>

(Repeat up to 5 items, each separated by a blank line)

━━━━━━━━━━━━━━━━━━

📊 _Coverage: {coverage_label}_
🔎 _Source: Tavily_

---

IMPACT TAG RULES:
- Use 🔥 for high-impact (funding, major product launches, large layoffs, acquisitions)
- Use ⚠️ for risk signals (declining hiring, layoffs, revenue pressure)
- Use 📈 for growth signals (expansion, hiring surge, new markets)
- Use 🧠 for strategic/product updates

---

FORMATTING RULES:
- Always include the URL as a clickable link using 🔗
- Keep each item visually separated
- Keep it clean and scannable
- Ensure there is a blank line between each item
- Do NOT cluster items together
- Keep formatting clean and readable

RULES:
- Only use the provided data. Order items by impact (highest first) and date (latest to oldest)
- No hallucination
- Max 5 items (Only important ones) - give less if 5 are not very important
- One sentence each

IMPORTANT:
- Focus on important news from the LAST {NEWS_LOOKBACK_DAYS} DAYS
- Ignore any news older than {NEWS_LOOKBACK_DAYS} days, even if provided.
"""

    client = get_gemini_client(config)
    response = client.models.generate_content(
        model=config["gemini_model"],
        contents=prompt,
    )
    return response.text.strip()


def deduplicate_news(results):
    # The same article can appear multiple times across searches, so we dedupe
    # by URL before checking whether it has already been sent in prior runs.
    seen_urls = set()
    unique = []

    for item in results:
        url = item["url"]
        if url in seen_urls:
            continue

        seen_urls.add(url)
        unique.append(item)

    return unique


def post_to_slack(message, config: dict[str, str], retries=3):
    # Slack delivery gets a few retries because transient network failures are
    # common enough that a short retry loop improves reliability significantly.
    for attempt in range(retries):
        try:
            response = requests.post(
                config["slack_webhook_url"],
                json={"text": message},
                timeout=20,
            )
            if response.status_code == 200:
                return True
        except Exception as exc:
            print(f"Attempt {attempt + 1} failed: {exc}")

        time.sleep(2)

    return False


def load_sent_urls(config: dict[str, str]):
    # Column A of the Google Sheet acts as the de-duplication ledger for all
    # URLs that have already been posted to Slack in earlier runs.
    sheet = get_sheet(config)
    urls = sheet.col_values(1)
    return set(urls)


def save_sent_urls(urls, config: dict[str, str]):
    # Only new URLs are appended so the sheet remains compact and future runs
    # can cheaply filter already-sent items before generating a digest.
    sheet = get_sheet(config)
    existing = set(sheet.col_values(1))
    new_urls = [url for url in urls if url not in existing]

    if new_urls:
        sheet.append_rows([[url] for url in new_urls])


def run_publisher_intel():
    # This is the full end-to-end job orchestration used by both local runs and
    # Vercel cron invocations.
    config = get_runtime_config()
    tavily_client = TavilyClient(api_key=config["tavily_api_key"])

    print("Starting Publisher Intel...")
    label, publishers, coverage_label, _ = get_todays_publishers()

    if publishers is None:
        print("Weekend - skipping")
        return {"ok": True, "status": "skipped", "reason": "weekend"}

    print(f"Running schedule: {label}")
    print("Fetching real-time news...")
    news = fetch_news(publishers, tavily_client)
    print(f"Fetched {len(news)} raw items")

    news = quick_filter(news)
    print(f"After quick filter: {len(news)}")

    news = soft_rank_and_limit(news)
    news = filter_recent_news(news)
    print(f"After date filter: {len(news)}")

    news = deduplicate_news(news)
    sent_urls = load_sent_urls(config)
    news = [item for item in news if item["url"] not in sent_urls]
    print(f"Collected {len(news)} news items")

    if not news:
        today_str = datetime.date.today().strftime("%A, %d %B %Y")
        message = f"""
📡 Joveo Publisher Intel - {today_str}

No impactful updates relevant to Joveo were found for {coverage_label} within the last few days.

Researched via: Tavily
Coverage today: {coverage_label}
"""
        post_to_slack(message.strip(), config)
        return {
            "ok": True,
            "status": "no_updates",
            "coverage_label": coverage_label,
            "news_count": 0,
        }

    print("Generating brief...")
    brief = generate_brief(news, coverage_label, config)

    if not brief:
        print("No brief generated - skipping Slack")
        return {"ok": False, "status": "no_brief", "coverage_label": coverage_label}

    print("Posting to Slack...")
    success = post_to_slack(brief, config)

    if not success:
        print("Slack post failed")
    else:
        # Persist sent URLs only after Slack confirms delivery so failed posts
        # can be retried on the next run instead of being marked as complete.
        save_sent_urls([item["url"] for item in news], config)
        print("Slack post success")

    print("Done.")
    return {
        "ok": success,
        "status": "posted" if success else "slack_failed",
        "coverage_label": coverage_label,
        "news_count": len(news),
    }


def main():
    # Local CLI compatibility entrypoint.
    run_publisher_intel()
