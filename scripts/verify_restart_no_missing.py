from __future__ import annotations

import datetime as dt
import subprocess
import sys

import httpx

from aisley_scraper.config import get_settings


def _parse_iso(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _fetch_run_rows(
    client: httpx.Client,
    base: str,
    headers: dict[str, str],
    start: dt.datetime,
    end: dt.datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    offset = 0
    page_size = 1000

    while True:
        response = client.get(
            f"{base}/shopify_products",
            params={
                "select": "id,store_id,first_seen_at,last_seen_at,images,supabase_images,gender_probs_csv",
                "last_seen_at": f"gte.{start.isoformat()}",
                "order": "last_seen_at.asc",
                "limit": str(page_size),
                "offset": str(offset),
            },
            headers=headers,
            timeout=60.0,
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break

        # Guard against clock skew and include only rows from this run window.
        for row in page:
            ts_raw = row.get("last_seen_at")
            if not ts_raw:
                continue
            ts = _parse_iso(str(ts_raw))
            if start <= ts <= end:
                rows.append(row)

        if len(page) < page_size:
            break
        offset += page_size

    return rows


def main() -> int:
    settings = get_settings()
    start = dt.datetime.now(dt.UTC)

    crawl_cmd = [
        sys.executable,
        "-m",
        "aisley_scraper.cli",
        "crawl-stores",
        "--limit",
        "1",
    ]
    result = subprocess.run(crawl_cmd)
    if result.returncode != 0:
        print({"crawl_exit_code": result.returncode})
        return result.returncode

    end = dt.datetime.now(dt.UTC)

    base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    with httpx.Client() as client:
        try:
            run_rows = _fetch_run_rows(client, base, headers, start, end)
        except httpx.HTTPError as exc:
            print({"supabase_query_error": str(exc)})
            return 2

    rows_with_images = [r for r in run_rows if len(r.get("images") or []) > 0]
    missing_supa = [r for r in rows_with_images if len(r.get("supabase_images") or []) == 0]
    missing_probs = [r for r in rows_with_images if r.get("gender_probs_csv") is None]
    store_ids = sorted({str(r.get("store_id")) for r in run_rows if r.get("store_id")})
    preexisting_rows = [
        r
        for r in run_rows
        if r.get("first_seen_at") and _parse_iso(str(r.get("first_seen_at"))) < start
    ]

    print(
        {
            "run_window_start": start.isoformat(),
            "run_window_end": end.isoformat(),
            "run_rows": len(run_rows),
            "distinct_store_ids": len(store_ids),
            "sample_store_ids": store_ids[:5],
            "rows_preexisting_before_run": len(preexisting_rows),
            "run_rows_with_images": len(rows_with_images),
            "missing_supabase_images": len(missing_supa),
            "missing_gender_probs": len(missing_probs),
            "sample_missing_ids": [r.get("id") for r in (missing_supa + missing_probs)[:10]],
        }
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
