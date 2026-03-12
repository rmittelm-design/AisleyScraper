from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

import httpx

from aisley_scraper.config import get_settings


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Monitor live Supabase write activity during scraping")
    p.add_argument("--interval-sec", type=int, default=5, help="Polling interval in seconds")
    p.add_argument("--iterations", type=int, default=0, help="Number of polls (0 = run forever)")
    p.add_argument("--limit", type=int, default=200, help="Rows to inspect per table per poll")
    return p


def _ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fetch_rows(
    base: str,
    headers: dict[str, str],
    table: str,
    limit: int,
    select_columns: str,
) -> list[dict[str, object]]:
    resp = httpx.get(
        f"{base}/{table}",
        params={"select": select_columns, "order": "id.desc", "limit": str(limit)},
        headers=headers,
        timeout=20.0,
    )
    resp.raise_for_status()
    rows = resp.json()
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []


def _summarize_products(rows: list[dict[str, object]]) -> dict[str, object]:
    max_id = max((int(r.get("id") or 0) for r in rows), default=0)
    latest_seen = rows[0].get("last_seen_at") if rows else None

    with_images = sum(1 for r in rows if len(r.get("images") or []) > 0)
    with_supa = sum(1 for r in rows if len(r.get("supabase_images") or []) > 0)
    missing_supa = sum(
        1
        for r in rows
        if len(r.get("images") or []) > 0 and len(r.get("supabase_images") or []) == 0
    )
    missing_probs = sum(
        1
        for r in rows
        if len(r.get("images") or []) > 0 and r.get("gender_probs_csv") is None
    )

    return {
        "max_id": max_id,
        "latest_last_seen_at": latest_seen,
        "rows": len(rows),
        "with_images": with_images,
        "with_supabase_images": with_supa,
        "images_missing_supabase_images": missing_supa,
        "images_missing_gender_probs": missing_probs,
    }


def _summarize_stores(rows: list[dict[str, object]]) -> dict[str, object]:
    max_id = max((int(r.get("id") or 0) for r in rows), default=0)
    latest_seen = rows[0].get("last_seen_at") if rows else None
    return {
        "max_id": max_id,
        "latest_last_seen_at": latest_seen,
        "rows": len(rows),
    }


def main() -> int:
    args = _parser().parse_args()
    settings = get_settings()

    base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    poll = 0
    prev_product_max_id = None
    prev_product_seen = None
    prev_store_max_id = None
    prev_store_seen = None

    while True:
        poll += 1

        product_rows = _fetch_rows(
            base,
            headers,
            "shopify_products",
            args.limit,
            "id,last_seen_at,images,supabase_images,gender_probs_csv",
        )
        store_rows = _fetch_rows(
            base,
            headers,
            "shopify_stores",
            args.limit,
            "id,last_seen_at",
        )

        product = _summarize_products(product_rows)
        store = _summarize_stores(store_rows)

        delta_product_max_id = (
            int(product["max_id"]) - int(prev_product_max_id)
            if prev_product_max_id is not None
            else 0
        )
        delta_store_max_id = (
            int(store["max_id"]) - int(prev_store_max_id)
            if prev_store_max_id is not None
            else 0
        )

        product_seen_changed = prev_product_seen is not None and product["latest_last_seen_at"] != prev_product_seen
        store_seen_changed = prev_store_seen is not None and store["latest_last_seen_at"] != prev_store_seen

        print(
            {
                "ts": _ts(),
                "poll": poll,
                "products": {
                    **product,
                    "delta_max_id": delta_product_max_id,
                    "last_seen_changed": product_seen_changed,
                },
                "stores": {
                    **store,
                    "delta_max_id": delta_store_max_id,
                    "last_seen_changed": store_seen_changed,
                },
            }
        )

        prev_product_max_id = int(product["max_id"])
        prev_product_seen = product["latest_last_seen_at"]
        prev_store_max_id = int(store["max_id"])
        prev_store_seen = store["latest_last_seen_at"]

        if args.iterations > 0 and poll >= args.iterations:
            break

        time.sleep(max(1, args.interval_sec))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
