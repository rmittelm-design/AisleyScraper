"""Delete all rows from shopify_products then shopify_stores in ID-range batches."""
from __future__ import annotations

import sys
import time

import httpx

from aisley_scraper.config import get_settings


def delete_table(client: httpx.Client, base: str, headers: dict, table: str, batch: int = 5000) -> None:
    r = client.get(f"{base}/rest/v1/{table}", params={"select": "id", "order": "id.asc", "limit": "1"}, headers=headers)
    rows = r.json()
    if not rows:
        print(f"{table}: empty, nothing to delete.")
        return
    min_id = rows[0]["id"]

    r = client.get(f"{base}/rest/v1/{table}", params={"select": "id", "order": "id.desc", "limit": "1"}, headers=headers)
    max_id = r.json()[0]["id"]
    print(f"{table}: id range {min_id}–{max_id}")

    cur = min_id
    while cur <= max_id:
        end = cur + batch - 1
        r = client.delete(
            f"{base}/rest/v1/{table}",
            params={"id": f"gte.{cur}", "id2": f"lte.{end}"},
            headers=headers,
        )
        # PostgREST doesn't support two params with same name via dict; use raw URL
        url = f"{base}/rest/v1/{table}?id=gte.{cur}&id=lte.{end}"
        r = client.delete(url, headers=headers)
        print(f"  {table} ids {cur}–{end}: status={r.status_code}")
        if r.status_code not in (200, 204):
            print(f"  ERROR: {r.text[:200]}")
        cur = end + 1
        time.sleep(0.1)

    print(f"{table}: done.")


def main() -> int:
    s = get_settings()
    base = s.supabase_url.rstrip("/")
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        # Delete products first (FK references stores)
        delete_table(client, base, headers, "shopify_products")
        delete_table(client, base, headers, "shopify_stores")

    return 0


if __name__ == "__main__":
    sys.exit(main())
