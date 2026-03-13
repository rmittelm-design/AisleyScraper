from __future__ import annotations

from urllib.parse import urlparse

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.ingest.csv_loader import load_store_seeds


def normalize(url: str) -> str:
    cleaned = url.strip()
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def main() -> int:
    settings = get_settings()
    seeds = load_store_seeds(settings.input_csv_path, settings)

    base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }

    existing: set[str] = set()
    offset = 0
    page_size = 1000

    with httpx.Client(timeout=60.0) as client:
        while True:
            resp = client.get(
                f"{base}/shopify_stores",
                params={
                    "select": "website",
                    "order": "id.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
                headers=headers,
            )
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            for row in page:
                website = row.get("website")
                if website:
                    existing.add(normalize(str(website)))
            if len(page) < page_size:
                break
            offset += page_size

    for idx, seed in enumerate(seeds):
        n = normalize(seed.store_url)
        if n not in existing:
            print({"index": idx, "seed_url": seed.store_url, "normalized": n})
            return 0

    print({"index": None, "seed_url": None, "normalized": None})
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
