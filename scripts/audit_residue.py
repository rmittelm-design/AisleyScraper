from __future__ import annotations

import httpx

from aisley_scraper.config import get_settings

SYNTHETIC = "0.333333,0.333333,0.333334"


def main() -> int:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1/shopify_products"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }
    prefix = f"{s.supabase_url.rstrip('/')}/storage/v1/object/public/{s.supabase_storage_bucket}/"

    rows = httpx.get(
        base,
        params={
            "select": "id,images,supabase_images,gender_probs_csv",
            "order": "id.desc",
            "limit": "1200",
        },
        headers=headers,
        timeout=30.0,
    ).json()

    synthetic = 0
    non_supabase = 0
    for row in rows:
        if row.get("gender_probs_csv") == SYNTHETIC:
            synthetic += 1
        supa = list(row.get("supabase_images") or [])
        if supa and any(not str(url).startswith(prefix) for url in supa):
            non_supabase += 1

    print({
        "rows_checked": len(rows),
        "synthetic_gender_probs": synthetic,
        "non_supabase_url_rows": non_supabase,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
