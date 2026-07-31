from __future__ import annotations

import argparse

import httpx

from aisley_scraper.config import get_settings


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Delete failed websites from staging for a given run")
    p.add_argument("--run-id", required=True)
    p.add_argument("--dry-run", action="store_true")
    return p


def _headers(service_role_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {service_role_key}",
        "apikey": service_role_key,
        "Content-Type": "application/json",
    }


def main() -> int:
    args = _parser().parse_args()
    settings = get_settings()
    base = settings.supabase_url.rstrip("/") + "/rest/v1"
    headers = _headers(settings.supabase_service_role_key)

    with httpx.Client(timeout=30.0) as client:
        failed_resp = client.get(
            f"{base}/crawl_store_runs",
            headers=headers,
            params={
                "select": "website,status,error_message",
                "run_id": f"eq.{args.run_id}",
                "status": "eq.failed",
                "order": "website.asc",
            },
        )
        failed_resp.raise_for_status()
        failed_rows = failed_resp.json()

        websites = [
            row["website"]
            for row in failed_rows
            if isinstance(row, dict) and isinstance(row.get("website"), str) and row["website"]
        ]

        print(f"run_id={args.run_id} failed_websites={len(websites)}")
        if not websites:
            return 0

        for website in websites:
            print(f"website={website}")
            if args.dry_run:
                continue

            for table in ("shopify_products_staging", "shopify_stores_staging"):
                del_resp = client.delete(
                    f"{base}/{table}",
                    headers={**headers, "Prefer": "return=representation"},
                    params={
                        "run_id": f"eq.{args.run_id}",
                        "website": f"eq.{website}",
                    },
                )
                del_resp.raise_for_status()
                payload = del_resp.json() if del_resp.text else []
                count = len(payload) if isinstance(payload, list) else 0
                print(f"  deleted {table}: {count}")

        remaining_resp = client.get(
            f"{base}/shopify_stores_staging",
            headers=headers,
            params={
                "select": "id,website",
                "run_id": f"eq.{args.run_id}",
                "order": "id.asc",
            },
        )
        remaining_resp.raise_for_status()
        remaining = remaining_resp.json()
        print(f"remaining_store_staging_rows_for_run={len(remaining)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
