from __future__ import annotations

import argparse
from dataclasses import asdict
from urllib.parse import urlparse

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.geocoding import geocode_address
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.models import StoreProfile


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Backfill one shopify_stores row with address/lat/long from a CSV/TSV seed file"
        )
    )
    p.add_argument("--store-id", type=int, required=True, help="shopify_stores.id to fix")
    p.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help="Seed file path; defaults to INPUT_CSV_PATH from settings",
    )
    p.add_argument(
        "--csv-has-header",
        type=str,
        choices=["true", "false"],
        default=None,
        help="Override INPUT_CSV_HAS_HEADER for this run",
    )
    p.add_argument(
        "--country-code",
        type=str,
        default="us",
        help="Optional geocoding country code filter (default: us)",
    )
    p.add_argument(
        "--address",
        type=str,
        default=None,
        help="Use this address directly instead of seed-derived address",
    )
    p.add_argument(
        "--force-address-from-seed",
        action="store_true",
        help="Replace existing address with the seed address even if address already exists",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended update without writing to shopify_stores",
    )
    return p


def _rest_headers(service_role_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {service_role_key}",
        "apikey": service_role_key,
        "Content-Type": "application/json",
    }


def _fetch_store_row(
    client: httpx.Client,
    *,
    base_url: str,
    headers: dict[str, str],
    store_id: int,
) -> dict[str, object]:
    resp = client.get(
        f"{base_url}/shopify_stores",
        params={
            "select": "id,website,store_name,store_type,instagram_handle,address,lat,long",
            "id": f"eq.{store_id}",
            "limit": "1",
        },
        headers=headers,
    )
    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise RuntimeError(f"shopify_stores row not found for id={store_id}")
    return rows[0]


def _domain(url: str) -> str:
    return urlparse(url).netloc.strip().lower()


def main() -> int:
    args = _parser().parse_args()
    settings = get_settings()

    if args.csv_path:
        settings = settings.model_copy(update={"input_csv_path": args.csv_path})
    if args.csv_has_header is not None:
        settings = settings.model_copy(
            update={"input_csv_has_header": args.csv_has_header.lower() == "true"}
        )

    seeds = load_store_seeds(settings.input_csv_path, settings)
    seeds_by_domain = {}
    for seed in seeds:
        seed_domain = _domain(seed.store_url)
        if seed_domain and seed_domain not in seeds_by_domain:
            seeds_by_domain[seed_domain] = seed

    base_url = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = _rest_headers(settings.supabase_service_role_key)

    with httpx.Client(timeout=30.0) as client:
        row = _fetch_store_row(
            client,
            base_url=base_url,
            headers=headers,
            store_id=max(1, args.store_id),
        )

    website = row.get("website")
    if not isinstance(website, str) or not website:
        raise RuntimeError("target row missing website")

    seed = seeds_by_domain.get(_domain(website))
    if seed is None:
        raise RuntimeError(f"no matching seed found in {settings.input_csv_path} for website={website}")

    seed_address = (seed.address or "").strip() or None
    override_address = (args.address or "").strip() or None
    if override_address is None and seed_address is None:
        raise RuntimeError(f"matching seed has no address for website={website}")

    existing_address = row.get("address") if isinstance(row.get("address"), str) else None
    if override_address is not None:
        selected_address = override_address
    elif args.force_address_from_seed or not existing_address:
        selected_address = seed_address
    else:
        selected_address = existing_address

    lat_value = row.get("lat")
    long_value = row.get("long")
    existing_lat = float(lat_value) if isinstance(lat_value, (int, float)) else None
    existing_long = float(long_value) if isinstance(long_value, (int, float)) else None

    coords = geocode_address(
        selected_address,
        user_agent=(settings.user_agent or "").strip() or "aisley-scraper/1.0",
        timeout_sec=float(settings.crawl_request_timeout_sec),
        country_codes=[args.country_code] if (args.country_code or "").strip() else None,
    )

    target_lat = existing_lat
    target_long = existing_long
    if coords is not None:
        target_lat, target_long = coords

    store_name = row.get("store_name") if isinstance(row.get("store_name"), str) else None
    store_type = row.get("store_type") if isinstance(row.get("store_type"), str) else None
    instagram_handle = row.get("instagram_handle") if isinstance(row.get("instagram_handle"), str) else None

    if not store_name:
        store_name = seed.store_name or _domain(website)
    if not store_type:
        store_type = "shopify"

    profile = StoreProfile(
        store_name=store_name,
        website=website,
        store_type=store_type,
        instagram_handle=instagram_handle,
        address=selected_address,
        lat=target_lat,
        long=target_long,
    )

    before = {
        "id": row.get("id"),
        "website": website,
        "address": existing_address,
        "lat": existing_lat,
        "long": existing_long,
    }

    if args.dry_run:
        print("Dry run; no database write.")
        print("Before:", before)
        if override_address is not None:
            print("Override address:", override_address)
        print("Seed address:", seed_address)
        print("Planned profile:", asdict(profile))
        return 0

    repo = SupabaseRestRepository(settings)
    written_store_id = repo.upsert_store(profile)

    with httpx.Client(timeout=30.0) as client:
        after = _fetch_store_row(
            client,
            base_url=base_url,
            headers=headers,
            store_id=written_store_id,
        )

    print("Backfill complete")
    print("Before:", before)
    print(
        "After:",
        {
            "id": after.get("id"),
            "website": after.get("website"),
            "address": after.get("address"),
            "lat": after.get("lat"),
            "long": after.get("long"),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
