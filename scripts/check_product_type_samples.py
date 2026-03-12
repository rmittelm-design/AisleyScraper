from aisley_scraper.config import get_settings
import httpx


def main() -> None:
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    resp = httpx.get(
        f"{base}/shopify_products",
        params={
            "select": "store_id,product_id,item_name,product_type",
            "product_type": "in.(Mens,Womens)",
            "limit": "10",
            "order": "id.desc",
        },
        headers=headers,
        timeout=20.0,
    )
    print("products_status", resp.status_code)
    rows = resp.json()
    print("rows", len(rows))

    store_ids = sorted({str(row["store_id"]) for row in rows if "store_id" in row})
    by_id: dict[str, str] = {}

    if store_ids:
        stores_resp = httpx.get(
            f"{base}/shopify_stores",
            params={
                "select": "id,website",
                "id": "in.(" + ",".join(store_ids) + ")",
            },
            headers=headers,
            timeout=20.0,
        )
        stores = stores_resp.json()
        by_id = {str(item["id"]): str(item["website"]) for item in stores}

    for row in rows:
        print(
            {
                "store": by_id.get(str(row.get("store_id"))),
                "product_id": row.get("product_id"),
                "item_name": row.get("item_name"),
                "product_type": row.get("product_type"),
            }
        )


if __name__ == "__main__":
    main()
