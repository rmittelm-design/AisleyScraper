from __future__ import annotations

import argparse
import asyncio
import logging
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.orchestrator import scrape_many
from aisley_scraper.db.repository import Repository
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.local_output import write_local_results
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

    results = asyncio.run(scrape_many(seeds, settings))

    if settings.persistence_target == "local":
        success_count, fail_count = write_local_results(settings.local_output_path, results)
        print(
            f"Crawled {success_count}/{len(results)} stores successfully; "
            f"saved local output to {settings.local_output_path} ({fail_count} failed)"
        )
        return 0

    dsn = settings.supabase_db_dsn
    if not dsn:
        raise RuntimeError("SUPABASE_DB_DSN is required for crawl persistence")

    repo = Repository(dsn)
    repo.ensure_schema()
    uploader = StorageUploader(settings)

    success_count = 0
    for seed, outcome in results:
        if isinstance(outcome, Exception):
            print(f"FAIL {seed.store_url}: {outcome}")
            continue

        store_id = repo.upsert_store(outcome.store)
        for product in outcome.products:
            if product.images:
                product.supabase_images = uploader.upload_product_images(
                    product.images,
                    store_id=store_id,
                    product_id=product.product_id,
                )
            repo.upsert_product(store_id, product)
        success_count += 1

    print(f"Crawled {success_count}/{len(results)} stores successfully")
    return 0


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
