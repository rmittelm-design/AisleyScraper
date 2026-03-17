from __future__ import annotations

from collections import Counter

import httpx

from aisley_scraper.config import get_settings


def diagnose_staged_runs() -> None:
    settings = get_settings()
    base = settings.supabase_url.rstrip("/") + "/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    response = httpx.get(
        base + "/shopify_stores_staging",
        params={
            "select": "run_id,website,scraped_at",
            "order": "scraped_at.desc",
            "limit": "5000",
        },
        headers=headers,
        timeout=60.0,
    )
    response.raise_for_status()
    rows = response.json()

    run_counts: Counter[str] = Counter()
    latest_scraped_at: dict[str, str] = {}
    sample_websites: dict[str, str] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        run_id = row.get("run_id")
        website = row.get("website")
        scraped_at = row.get("scraped_at")
        if not isinstance(run_id, str) or not run_id:
            continue
        run_counts[run_id] += 1
        if run_id not in latest_scraped_at and isinstance(scraped_at, str):
            latest_scraped_at[run_id] = scraped_at
        if run_id not in sample_websites and isinstance(website, str):
            sample_websites[run_id] = website

    if not run_counts:
        print("staged_runs=NONE")
        return

    print("staged_runs=")
    for run_id, count in run_counts.most_common(20):
        latest = latest_scraped_at.get(run_id, "")
        sample = sample_websites.get(run_id, "")
        print(
            f"  run_id={run_id} staged_stores={count} latest_scraped_at={latest} sample_website={sample}"
        )