from __future__ import annotations

import datetime as dt

import httpx

from aisley_scraper.config import get_settings


def _parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def main() -> int:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    r = httpx.get(
        f"{base}/shopify_products",
        params={
            "select": "id,store_id,last_seen_at,first_seen_at,images,supabase_images,gender_probs_csv",
            "order": "id.desc",
            "limit": "5000",
        },
        headers=headers,
        timeout=60.0,
    )
    r.raise_for_status()
    rows = r.json()

    now = dt.datetime.now(dt.UTC)
    missing: list[dict[str, object]] = []
    for row in rows:
        img_n = len(row.get("images") or [])
        supa_n = len(row.get("supabase_images") or [])
        gp_null = row.get("gender_probs_csv") is None
        if img_n > 0 and (supa_n == 0 or gp_null):
            row_out = {
                "id": row.get("id"),
                "store_id": row.get("store_id"),
                "first_seen_at": row.get("first_seen_at"),
                "last_seen_at": row.get("last_seen_at"),
                "img_n": img_n,
                "supa_n": supa_n,
                "gp_null": gp_null,
            }
            missing.append(row_out)

    missing_60m = [
        row
        for row in missing
        if row.get("last_seen_at")
        and (now - _parse(str(row["last_seen_at"]))).total_seconds() <= 3600
    ]

    print(
        {
            "checked_rows": len(rows),
            "missing_total": len(missing),
            "missing_last_60m": len(missing_60m),
            "sample_missing_last_60m": missing_60m[:25],
            "sample_missing_any": missing[:25],
        }
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
