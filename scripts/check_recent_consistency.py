from __future__ import annotations

import datetime as dt

import httpx

from aisley_scraper.config import get_settings


def main() -> None:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    rows = httpx.get(
        base + "/shopify_products",
        params={
            "select": "id,last_seen_at,images,supabase_images,gender_probs_csv",
            "order": "id.desc",
            "limit": "500",
        },
        headers=headers,
        timeout=30.0,
    ).json()

    now = dt.datetime.now(dt.UTC)
    recent: list[dict[str, object]] = []
    for row in rows:
        t = row.get("last_seen_at")
        if not t:
            continue
        try:
            ts = dt.datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        except Exception:
            continue
        if (now - ts).total_seconds() <= 900:
            recent.append(row)

    missing_supa = sum(
        1
        for row in recent
        if len(row.get("images") or []) > 0 and len(row.get("supabase_images") or []) == 0
    )
    missing_probs = sum(
        1
        for row in recent
        if len(row.get("images") or []) > 0 and row.get("gender_probs_csv") is None
    )

    print(
        {
            "recent_15m_rows": len(recent),
            "recent_missing_supabase_images": missing_supa,
            "recent_missing_gender_probs": missing_probs,
        }
    )
    print(
        [
            {
                "id": row.get("id"),
                "last_seen_at": row.get("last_seen_at"),
                "img_n": len(row.get("images") or []),
                "supa_n": len(row.get("supabase_images") or []),
                "gp_null": row.get("gender_probs_csv") is None,
            }
            for row in recent[:10]
        ]
    )


if __name__ == "__main__":
    main()
