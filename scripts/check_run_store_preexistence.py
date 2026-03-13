from __future__ import annotations

import argparse
import datetime as dt

import httpx

from aisley_scraper.config import get_settings


def _iso(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-id", required=True, type=int)
    parser.add_argument("--run-start", required=True)
    parser.add_argument("--run-end", required=True)
    args = parser.parse_args()

    run_start = _iso(args.run_start)
    run_end = _iso(args.run_end)

    settings = get_settings()
    base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    with httpx.Client(timeout=60.0) as client:
        resp = client.get(
            f"{base}/shopify_products",
            params={
                "select": "id,first_seen_at,last_seen_at",
                "store_id": f"eq.{args.store_id}",
                "order": "first_seen_at.asc",
                "limit": "10000",
            },
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()

    in_window = 0
    pre_existing = 0
    first_seen_min: dt.datetime | None = None
    first_seen_max: dt.datetime | None = None

    for row in rows:
        fs_raw = row.get("first_seen_at")
        ls_raw = row.get("last_seen_at")
        if not fs_raw or not ls_raw:
            continue
        fs = _iso(str(fs_raw))
        ls = _iso(str(ls_raw))

        if first_seen_min is None or fs < first_seen_min:
            first_seen_min = fs
        if first_seen_max is None or fs > first_seen_max:
            first_seen_max = fs

        if run_start <= ls <= run_end:
            in_window += 1
            if fs < run_start:
                pre_existing += 1

    print(
        {
            "store_id": args.store_id,
            "total_rows_for_store": len(rows),
            "rows_updated_in_window": in_window,
            "rows_updated_in_window_preexisting_before_run": pre_existing,
            "first_seen_min": first_seen_min.isoformat() if first_seen_min else None,
            "first_seen_max": first_seen_max.isoformat() if first_seen_max else None,
            "run_start": run_start.isoformat(),
            "run_end": run_end.isoformat(),
        }
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
