from aisley_scraper.config import get_settings
from aisley_scraper.storage import StorageUploader
import httpx


def main() -> None:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
    }

    row = httpx.get(
        base + "/shopify_products",
        params={
            "select": "id,store_id,product_id,images,supabase_images",
            "order": "id.desc",
            "limit": "1",
        },
        headers=headers,
        timeout=20.0,
    ).json()[0]

    uploader = StorageUploader(s)
    current_source_urls = list(row.get("images") or [])
    existing_supabase_urls = list(row.get("supabase_images") or [])

    synced = uploader.sync_product_images(
        current_source_urls=current_source_urls,
        existing_source_urls=current_source_urls,
        existing_supabase_urls=existing_supabase_urls,
        store_id=int(row["store_id"]),
        product_id=str(row["product_id"]),
    )

    httpx.patch(
        base + "/shopify_products",
        params={"id": f"eq.{row['id']}"},
        json={"supabase_images": synced},
        headers={**headers, "Prefer": "return=minimal"},
        timeout=30.0,
    ).raise_for_status()

    print({"id": row["id"], "synced_count": len(synced)})


if __name__ == "__main__":
    main()
