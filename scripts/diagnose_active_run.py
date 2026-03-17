from __future__ import annotations

from collections import Counter
from pathlib import Path

import httpx

from aisley_scraper.config import get_settings


def _exact_count(base: str, headers: dict[str, str], run_id: str, status: str | None = None) -> int:
    params: dict[str, str] = {
        "select": "id",
        "run_id": f"eq.{run_id}",
        "limit": "1",
    }
    if status:
        params["status"] = f"eq.{status}"

    response = httpx.get(
        base + "/crawl_store_runs",
        params=params,
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
    state_file = Path(".aisley_active_run_id")
    if not state_file.exists():
        print("active_run_id=MISSING")
        return

    run_id = state_file.read_text(encoding="utf-8").strip()
    if not run_id:
        print("active_run_id=EMPTY")
        return

    settings = get_settings()
    base = settings.supabase_url.rstrip("/") + "/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    total = _exact_count(base, headers, run_id)
    pending = _exact_count(base, headers, run_id, "pending")
    failed = _exact_count(base, headers, run_id, "failed")
    completed = _exact_count(base, headers, run_id, "completed")

    rows_resp = httpx.get(
        base + "/crawl_store_runs",
        params={
            "select": "website,error_message",
            "run_id": f"eq.{run_id}",
            "status": "eq.failed",
            "order": "updated_at.desc",
            "limit": "20",
        },
        headers=headers,
        timeout=30.0,
    )
    rows_resp.raise_for_status()
    failed_rows = rows_resp.json()

    msg_counts = Counter()
    for row in failed_rows:
        if not isinstance(row, dict):
            continue
        msg = (row.get("error_message") or "").strip()
        if msg:
            msg_counts[msg] += 1

    print(f"active_run_id={run_id}")
    print(f"run_total={total}")
    print(f"run_status pending={pending} failed={failed} completed={completed}")
    if failed_rows:
        print("recent_failed_websites=")
        for row in failed_rows[:10]:
            if not isinstance(row, dict):
                continue
            website = row.get("website") or ""
            error = (row.get("error_message") or "").strip()
            short = error if len(error) <= 180 else error[:177] + "..."
            print(f"  {website} :: {short}")
    if msg_counts:
        print("failed_error_summary=")
        for msg, count in msg_counts.most_common(5):
            short = msg if len(msg) <= 180 else msg[:177] + "..."
            print(f"  {count}x {short}")


if __name__ == "__main__":
    main()
