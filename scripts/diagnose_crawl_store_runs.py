from __future__ import annotations

from collections import Counter

import httpx

from aisley_scraper.config import get_settings


def _exact_count(base: str, headers: dict[str, str], path: str, params: dict[str, str]) -> int:
    response = httpx.get(
        base + path,
        params={**params, "limit": "1"},
        headers={**headers, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"},
        timeout=30.0,
    )
    response.raise_for_status()
    content_range = response.headers.get("content-range", "")
    if "/" not in content_range:
        return 0
    try:
        return int(content_range.split("/")[-1])
    except ValueError:
        return 0


def main() -> None:
    settings = get_settings()
    base = settings.supabase_url.rstrip("/") + "/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    total_rows = _exact_count(base, headers, "/crawl_store_runs", {"select": "id"})
    pending_rows = _exact_count(
        base,
        headers,
        "/crawl_store_runs",
        {"select": "id", "status": "eq.pending"},
    )
    failed_rows = _exact_count(
        base,
        headers,
        "/crawl_store_runs",
        {"select": "id", "status": "eq.failed"},
    )
    completed_rows = _exact_count(
        base,
        headers,
        "/crawl_store_runs",
        {"select": "id", "status": "eq.completed"},
    )

    products_total = _exact_count(base, headers, "/shopify_products", {"select": "id"})

    latest_rows_resp = httpx.get(
        base + "/crawl_store_runs",
        params={
            "select": "run_id,status,error_message",
            "order": "updated_at.desc",
            "limit": "5000",
        },
        headers=headers,
        timeout=60.0,
    )
    latest_rows_resp.raise_for_status()
    rows = latest_rows_resp.json()

    run_counts = Counter()
    failed_by_run = Counter()
    error_counts = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        run_id = row.get("run_id")
        status = row.get("status")
        if isinstance(run_id, str) and run_id:
            run_counts[run_id] += 1
            if status == "failed":
                failed_by_run[run_id] += 1
        if status == "failed":
            msg = (row.get("error_message") or "").strip()
            if msg:
                error_counts[msg] += 1

    print(f"crawl_store_runs_total={total_rows}")
    print(f"crawl_store_runs_status pending={pending_rows} failed={failed_rows} completed={completed_rows}")
    print(f"shopify_products_total={products_total}")
    print(f"distinct_run_ids_in_recent_rows={len(run_counts)}")
    print("top_runs_by_rows=")
    for run_id, count in run_counts.most_common(5):
        print(f"  {run_id} -> {count}")
    print("failed_by_run=")
    for run_id, count in failed_by_run.most_common(5):
        print(f"  {run_id} -> {count}")
    print("top_failed_error_messages=")
    for msg, count in error_counts.most_common(8):
        short = msg if len(msg) <= 180 else msg[:177] + "..."
        print(f"  {count}x {short}")


if __name__ == "__main__":
    main()
