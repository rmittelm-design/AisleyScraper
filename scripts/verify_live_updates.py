from __future__ import annotations

import time

import httpx

from aisley_scraper.config import get_settings


def snapshot(base: str, headers: dict[str, str], label: str) -> dict[str, object]:
    rows = httpx.get(
        base + "/shopify_products",
        params={
            "select": "id,last_seen_at,images,supabase_images,gender_probs_csv",
            "order": "id.desc",
            "limit": "200",
        },
        headers=headers,
        timeout=20.0,
    ).json()

    max_id = max((row.get("id", 0) or 0) for row in rows) if rows else 0
    latest_seen = rows[0].get("last_seen_at") if rows else None
    with_images = sum(1 for row in rows if len(row.get("images") or []) > 0)
    with_supa = sum(1 for row in rows if len(row.get("supabase_images") or []) > 0)
    missing_supa = sum(
        1
        for row in rows
        if len(row.get("images") or []) > 0 and len(row.get("supabase_images") or []) == 0
    )
    missing_probs = sum(
        1 for row in rows if len(row.get("images") or []) > 0 and row.get("gender_probs_csv") is None
    )

    payload = {
        "label": label,
        "rows": len(rows),
        "max_id": max_id,
        "latest_last_seen_at": latest_seen,
        "with_images": with_images,
        "with_supabase_images": with_supa,
        "images_missing_supabase_images": missing_supa,
        "images_missing_gender_probs": missing_probs,
    }
    print(payload)
    return payload


def main() -> None:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    first = snapshot(base, headers, "t0")
    time.sleep(10)
    second = snapshot(base, headers, "t+10s")
    print(
        {
            "delta_max_id": int(second["max_id"]) - int(first["max_id"]),
            "last_seen_changed": second["latest_last_seen_at"] != first["latest_last_seen_at"],
        }
    )


if __name__ == "__main__":
    main()
