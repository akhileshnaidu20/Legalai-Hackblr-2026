"""
Scrape top-cited legal sections from Indian Kanoon.
Be respectful - add delays between requests.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import os

OUTPUT_FILE = os.path.expanduser("~/hackblr-legal-ai/data/kanoon_data.json")

# Key searches to seed your legal DB
SEED_QUERIES = [
    "Section 420 IPC cheating",
    "Section 302 IPC murder",
    "Section 498A IPC cruelty",
    "contract breach Indian Contract Act",
    "specific performance",
    "Section 34 IPC common intention",
    "fundamental rights Article 21",
    "Article 14 equality before law",
    "natural justice principles",
    "arbitration agreement validity",
    "landlord tenant eviction",
    "consumer protection deficiency service",
    "negotiable instruments Section 138",
    "bail conditions CrPC",
    "writ petition habeas corpus",
]


def search_indian_kanoon(query, max_results=5):
    """Search Indian Kanoon and extract results."""
    results = []
    url = f"https://indiankanoon.org/search/?formInput={query.replace(' ', '+')}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) LegalAI-Hackathon-Bot/1.0"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"  HTTP {resp.status_code} for query: {query}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")
        result_divs = soup.find_all("div", class_="result")

        for i, div in enumerate(result_divs[:max_results]):
            title_tag = div.find("a", class_="result_title")
            headline = div.find("div", class_="headline")

            if title_tag and headline:
                results.append({
                    "title": title_tag.get_text(strip=True),
                    "content": headline.get_text(strip=True),
                    "source": f"Indian Kanoon - {title_tag.get_text(strip=True)}",
                    "query": query,
                    "type": "case_law",
                    "act_name": "Case Law",
                    "section": "",
                })
    except Exception as e:
        print(f"  Error: {e}")

    return results


def main():
    print("Scraping Indian Kanoon (be patient, respecting rate limits)...")
    all_results = []

    for i, query in enumerate(SEED_QUERIES):
        print(f"[{i+1}/{len(SEED_QUERIES)}] Searching: {query}")
        results = search_indian_kanoon(query)
        all_results.extend(results)
        print(f"  Found {len(results)} results")
        time.sleep(3)  # Be nice to the server

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print(f"\nTotal scraped: {len(all_results)} entries")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()