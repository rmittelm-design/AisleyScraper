from aisley_scraper.config import get_settings
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.models import ProductRecord
import httpx


def main() -> None:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    row = httpx.get(
        base + "/shopify_products",
        params={
            "select": "store_id,product_id,item_name,images,supabase_images",
            "order": "id.desc",
            "limit": "1",
        },
        headers=headers,
        timeout=20.0,
    ).json()[0]

    print(
        {
            "before_supa_n": len(row.get("supabase_images") or []),
            "img_n": len(row.get("images") or []),
            "store_id": row.get("store_id"),
            "product_id": row.get("product_id"),
        }
    )

    repo = SupabaseRestRepository(s)
    probe_urls = [
        "https://example.com/probe1.jpg",
        "https://example.com/probe2.jpg",
    ]

    product = ProductRecord(
        product_id=str(row["product_id"]),
        product_handle=None,
        item_name=str(row.get("item_name") or ""),
        description=None,
        images=list(row.get("images") or []),
        supabase_images=probe_urls,
    )
    repo.upsert_product(int(row["store_id"]), product)

    after = httpx.get(
        base + "/shopify_products",
        params={
            "select": "store_id,product_id,supabase_images",
            "store_id": f"eq.{row['store_id']}",
            "product_id": f"eq.{row['product_id']}",
            "limit": "1",
        },
        headers=headers,
        timeout=20.0,
    ).json()[0]

    print({"after_supabase_images": after.get("supabase_images")})


if __name__ == "__main__":
    main()
