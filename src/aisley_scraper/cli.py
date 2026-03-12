from __future__ import annotations

import argparse
import asyncio
import logging
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.orchestrator import scrape_many, scrape_many_stream
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.local_output import write_local_results
from aisley_scraper.models import ScrapeResult, StoreSeed
from aisley_scraper.storage import StorageUploader


def _dedupe_seeds_by_domain(seeds: list[StoreSeed]) -> list[StoreSeed]:
    seen_domains: set[str] = set()
    deduped: list[StoreSeed] = []
    for seed in seeds:
        domain = urlparse(seed.store_url).netloc.strip().lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        deduped.append(seed)
    return deduped


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aisley-scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest-stores")
    ingest.add_argument("--csv", required=False)

    crawl = sub.add_parser("crawl-stores")
    crawl.add_argument("--limit", type=int, default=None)

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))


def run_ingest(csv_path: str | None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    path = csv_path or settings.input_csv_path
    seeds = load_store_seeds(path, settings)
    print(f"Loaded {len(seeds)} stores from {path}")
    return 0


def run_crawl(limit: int | None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    seeds = load_store_seeds(settings.input_csv_path, settings)
    seeds = _dedupe_seeds_by_domain(seeds)
    if limit is not None:
        seeds = seeds[:limit]
    else:
        seeds = seeds[: settings.crawl_max_stores_per_run]

    if settings.persistence_target == "local":
        results = asyncio.run(scrape_many(seeds, settings))
        success_count, fail_count = write_local_results(settings.local_output_path, results)
        print(
            f"Crawled {success_count}/{len(results)} stores successfully; "
            f"saved local output to {settings.local_output_path} ({fail_count} failed)"
        )
        return 0

    # Persist in batches so rows start appearing in Supabase before the full crawl ends.
    chunk_size = max(1, settings.crawl_global_concurrency)

    repo = SupabaseRestRepository(settings)
    gender_fetcher = Fetcher(settings)

    try:
        repo.ensure_schema()
        uploader = StorageUploader(settings)

        success_count = 0
        processed_count = 0

        def _persist_store_result(seed: StoreSeed, outcome: ScrapeResult | Exception) -> bool:
            if isinstance(outcome, Exception):
                print(f"FAIL {seed.store_url}: {outcome}")
                return False

            store_id = repo.upsert_store(outcome.store)
            for product in outcome.products:
                existing_image_state = repo.get_product_image_state(store_id, product.product_id)
                if existing_image_state is None:
                    if product.unavailable:
                        continue

                    # Insert early so new rows are visible while uploads continue.
                    repo.upsert_product(store_id, product)

                    if product.images:
                        product.supabase_images = uploader.upload_product_images(
                            product.images,
                            store_id=store_id,
                            product_id=product.product_id,
                        )
                else:
                    existing_images, existing_supabase_images = existing_image_state
                    product.supabase_images = uploader.sync_product_images(
                        current_source_urls=product.images,
                        existing_source_urls=existing_images,
                        existing_supabase_urls=existing_supabase_images,
                        store_id=store_id,
                        product_id=product.product_id,
                    )

                # Backfill missing probabilities for scraped products that have no explicit gender.
                if product.gender_label is None and not product.gender_probs_csv and product.images:
                    asyncio.run(
                        enrich_gender_probabilities_for_products(
                            products=[product],
                            fetcher=gender_fetcher,
                            concurrency=settings.image_validation_concurrency,
                        )
                    )

                repo.upsert_product(store_id, product)

            return True

        async def _persist_batch_stream(batch: list[StoreSeed]) -> tuple[int, int]:
            processed_in_batch = 0
            success_in_batch = 0

            async for seed, outcome in scrape_many_stream(batch, settings):
                processed_in_batch += 1
                persisted_ok = await asyncio.to_thread(_persist_store_result, seed, outcome)
                if persisted_ok:
                    success_in_batch += 1

            return processed_in_batch, success_in_batch

        for start in range(0, len(seeds), chunk_size):
            batch = seeds[start : start + chunk_size]
            processed_in_batch, success_in_batch = asyncio.run(_persist_batch_stream(batch))
            processed_count += processed_in_batch
            success_count += success_in_batch

            print(f"Progress: persisted {processed_count}/{len(seeds)} stores")

        print(f"Crawled {success_count}/{len(seeds)} stores successfully")
        return 0
    finally:
        asyncio.run(gender_fetcher.close())


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest-stores":
        return run_ingest(args.csv)
    if args.command == "crawl-stores":
        return run_crawl(args.limit)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
