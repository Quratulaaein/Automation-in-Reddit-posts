import os
import re
import csv
import time
import json
import math
import argparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Optional: PRAW (fallback)
try:
    import praw
except Exception:
    praw = None

import requests

load_dotenv()

# ----- CONFIG -----
OUTPUT_CSV = os.path.abspath("reddit_14d_leads.csv")
PUSHSHIFT_URL = "https://api.pushshift.io/reddit/search/submission/"
# subreddits and keywords tuned to your services (customize as needed)
SUBREDDITS = [
    "forhire","hireaprogrammer","webdev","entrepreneur","smallbusiness",
    "startups","marketing","DesignJobs","UXDesign","freelance_forhire","india"
]
KEYWORDS = [
    "looking for developer", "looking for designer", "need a website", "need a web developer",
    "need an e-commerce", "need help with shopify", "looking to hire", "hiring a designer",
    "hiring a developer", "logo design", "brand strategy", "social media manager",
    "email marketing", "content strategy", "performance audit", "hire an agency"
]

# regexes
EMAIL_RE = re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
PHONE_RE = re.compile(r'(\+91[\-\s]?[6-9]\d{9}|\b[6-9]\d{9}\b|\+?\d[\d\-\s]{6,}\d)')

# PRAW credentials from .env (only needed for fallback)
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT", "reddit_fetch_14d:v1.0 (by u/unknown)")

# how many results per Pushshift query (max 200)
PUSHSHIFT_SIZE = 200

# Score thresholds / weights (tune if needed)
PRIMARY_WEIGHT = 5
SECONDARY_WEIGHT = 2
CONTACT_WEIGHT = 5
INDIA_WEIGHT = 2

SECONDARY_KEYWORDS = [
    "how much to make a website", "recommend an agency", "copywriter", "content writer",
    "who can build", "anyone build", "help building", "budget"
]

# ----- UTILITIES -----
def epoch_seconds(dt: datetime):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def clean_text(s: str, max_len=2000):
    if not s:
        return ""
    # remove weird control chars, collapse whitespace, replace newlines with space
    cleaned = re.sub(r'[\r\n\t]+', ' ', s)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if len(cleaned) > max_len:
        return cleaned[:max_len] + "..."
    return cleaned

def score_text(text: str):
    t = text.lower()
    score = 0
    for kw in KEYWORDS:
        if kw in t:
            score += PRIMARY_WEIGHT
    for kw in SECONDARY_KEYWORDS:
        if kw in t:
            score += SECONDARY_WEIGHT
    if EMAIL_RE.search(text) or PHONE_RE.search(text):
        score += CONTACT_WEIGHT
    if "india" in t:
        score += INDIA_WEIGHT
    return score

def extract_fields_from_push(item):
    title = item.get("title","")
    selftext = item.get("selftext","")
    created_utc = item.get("created_utc")
    sub = item.get("subreddit")
    pid = item.get("id")
    url = item.get("full_link") or item.get("url") or f"https://reddit.com{item.get('permalink','')}"
    combined = f"{title}\n\n{selftext}"
    emails = EMAIL_RE.findall(combined)
    phones = PHONE_RE.findall(combined)
    cleaned_title = clean_text(title, max_len=400)
    cleaned_body = clean_text(selftext, max_len=3000)
    return {
        "id": pid,
        "created_utc": datetime.utcfromtimestamp(created_utc).isoformat() if created_utc else "",
        "subreddit": sub,
        "title": cleaned_title,
        "body": cleaned_body,
        "score": score_text(combined),
        "emails": ";".join(set(emails)),
        "phones": ";".join(set(phones)),
        "url": url
    }

# ----- PUSHSHIFT QUERY -----
def query_pushshift(keyword, subreddit, after_epoch, before_epoch, size=PUSHSHIFT_SIZE, retries=2, timeout=30):
    params = {
        "q": keyword,
        "subreddit": subreddit,
        "size": size,
        "after": after_epoch,
        "before": before_epoch,
        "sort": "desc"
    }
    headers = {"User-Agent": USER_AGENT}
    attempt = 0
    while attempt <= retries:
        try:
            resp = requests.get(PUSHSHIFT_URL, params=params, headers=headers, timeout=timeout)
            if resp.status_code != 200:
                attempt += 1
                time.sleep(1 + attempt)
                continue
            data = resp.json().get("data", [])
            return data
        except requests.RequestException as e:
            attempt += 1
            time.sleep(1 + attempt)
    return []

# ----- FALLBACK USING PRAW (scans subreddit.new limit=200 and filters by timestamp) -----
def praw_fallback_scan(reddit, subreddit, after_epoch, limit=200):
    results = []
    try:
        sr = reddit.subreddit(subreddit)
        for post in sr.new(limit=limit):
            if post.created_utc and post.created_utc >= after_epoch:
                # convert praw submission to dict similar to pushshift shape
                data = {
                    "id": post.id,
                    "created_utc": int(post.created_utc),
                    "subreddit": str(post.subreddit),
                    "title": post.title,
                    "selftext": post.selftext,
                    "permalink": post.permalink,
                    "full_link": f"https://reddit.com{post.permalink}"
                }
                results.append(data)
    except Exception as e:
        print(f"[praw fallback] error scanning {subreddit}: {e}")
    return results

# ----- MAIN -----
def main(args):
    now = datetime.now(timezone.utc)
    after_dt = now - timedelta(days=14)
    after_epoch = epoch_seconds(after_dt)
    before_epoch = epoch_seconds(now)

    print(f"Fetching posts from {after_dt.isoformat()} to {now.isoformat()} (past 14 days)")
    print("Subreddits:", SUBREDDITS)
    print("Keywords:", KEYWORDS)
    print("Output CSV:", OUTPUT_CSV)

    all_records = {}
    total_found = 0
    pushshift_used = 0
    praw_used = 0

    # prepare PRAW if available (for fallback)
    reddit = None
    if praw and CLIENT_ID and CLIENT_SECRET:
        try:
            reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
            print("[praw] fallback available")
        except Exception as e:
            reddit = None
            print("[praw] init failed:", e)

    for sub in SUBREDDITS:
        for kw in KEYWORDS:
            print(f"\nQuerying pushshift: subreddit={sub} keyword={kw!r} size={PUSHSHIFT_SIZE}")
            data = query_pushshift(kw, sub, after_epoch, before_epoch)
            if data:
                pushshift_used += 1
                print(f"  pushshift returned {len(data)} items")
                for item in data:
                    rec = extract_fields_from_push(item)
                    if not rec["id"]:
                        continue
                    all_records[rec["id"]] = rec
                    total_found += 1
                # small delay to be nice to API
                time.sleep(0.6)
            else:
                print("  pushshift returned 0 items or failed — trying PRAW fallback for this subreddit")
                if reddit:
                    fallback = praw_fallback_scan(reddit, sub, after_epoch, limit=200)
                    print(f"  praw fallback returned {len(fallback)} items")
                    praw_used += 1
                    for item in fallback:
                        rec = extract_fields_from_push(item)
                        if not rec["id"]:
                            continue
                        all_records[rec["id"]] = rec
                    time.sleep(0.4)
                else:
                    print("  no praw available — skipping fallback")

    # convert dict to list and optionally sort by score desc
    records_list = list(all_records.values())
    records_list.sort(key=lambda x: x.get("score", 0), reverse=True)

    # write CSV
    if records_list:
        fieldnames = ["id","created_utc","subreddit","title","body","score","emails","phones","url","saved_at"]
        now_iso = datetime.now(timezone.utc).isoformat()
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records_list:
                r_out = {k: r.get(k, "") for k in fieldnames}
                r_out["saved_at"] = now_iso
                writer.writerow(r_out)
        print(f"\nWrote {len(records_list)} unique records to {OUTPUT_CSV}")
    else:
        print("\nNo records found in the 14-day window with the provided queries.")

    print(f"Summary: total_unique={len(records_list)}, pushshift_queries_used={pushshift_used}, praw_fallbacks_used={praw_used}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch reddit posts from past 14 days into CSV")
    parser.add_argument("--out", help="output csv path (optional)", default=None)
    ns = parser.parse_args()
    if ns.out:
        OUTPUT_CSV = os.path.abspath(ns.out)
    main(ns)
