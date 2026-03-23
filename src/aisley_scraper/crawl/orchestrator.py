from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.extract.shopify_products import extract_products_from_products_json
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.extract.store_profile import classify_store
from aisley_scraper.models import ProductRecord, ScrapeResult, StoreSeed
from aisley_scraper.normalize.products import normalize_product


logger = logging.getLogger(__name__)


def _apply_seed_store_metadata(store, seed: StoreSeed):
    if seed.store_name:
        store.store_name = seed.store_name
    if seed.address:
        store.address = seed.address
    return store


async def _fetch_all_products(
    *,
    base: str,
    settings: Settings,
    fetcher: Fetcher,
) -> list[ProductRecord]:
    page_limit = max(1, settings.shopify_products_page_limit)
    max_pages = max(1, settings.shopify_products_max_pages)
    max_items_per_store = max(0, settings.shopify_products_max_items_per_store)

    all_products: list[ProductRecord] = []
    seen_product_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        products_url = f"{base}/products.json?limit={page_limit}&page={page}"
        payload = await fetcher.get_json(products_url)
        extracted = extract_products_from_products_json(payload, settings, base_url=base)

        for product in extracted:
            if product.product_id in seen_product_ids:
                continue
            seen_product_ids.add(product.product_id)
            all_products.append(product)

            if max_items_per_store > 0 and len(all_products) >= max_items_per_store:
                logger.warning(
                    "Reached per-store product cap for %s: collected=%s cap=%s",
                    base,
                    len(all_products),
                    max_items_per_store,
                )
                return all_products

        products_raw = payload.get("products", []) if isinstance(payload, dict) else []
        if not isinstance(products_raw, list) or not products_raw:
            break

    return all_products


async def scrape_store(seed: StoreSeed, settings: Settings, fetcher: Fetcher) -> ScrapeResult:
    base = seed.store_url.rstrip("/")
    homepage = await fetcher.get_text(base)
    store = classify_store(homepage, base, settings)
    store = _apply_seed_store_metadata(store, seed)

    extracted = await _fetch_all_products(base=base, settings=settings, fetcher=fetcher)
    await verify_product_images(products=extracted, fetcher=fetcher, settings=settings)
    await enrich_gender_probabilities_for_products(
        products=extracted,
        fetcher=fetcher,
        concurrency=settings.image_validation_concurrency,
    )
    products = [normalized for p in extracted if p.images if (normalized := normalize_product(p)) is not None]

    return ScrapeResult(store=store, products=products)


async def scrape_many(seeds: list[StoreSeed], settings: Settings) -> list[tuple[StoreSeed, ScrapeResult | Exception]]:
    fetcher = Fetcher(settings)
    semaphore = asyncio.Semaphore(settings.crawl_global_concurrency)

    async def _run(seed: StoreSeed) -> tuple[StoreSeed, ScrapeResult | Exception]:
        async with semaphore:
            try:
                return seed, await scrape_store(seed, settings, fetcher)
            except Exception as exc:
                return seed, exc

    try:
        return await asyncio.gather(*[_run(seed) for seed in seeds])
    finally:
        await fetcher.close()


async def scrape_many_stream(
    seeds: list[StoreSeed], settings: Settings, *, include_postprocess: bool = True
) -> AsyncIterator[tuple[StoreSeed, ScrapeResult | Exception]]:
    fetcher = Fetcher(settings)
    semaphore = asyncio.Semaphore(settings.crawl_global_concurrency)

    async def _run(seed: StoreSeed) -> tuple[StoreSeed, ScrapeResult | Exception]:
        async with semaphore:
            try:
                if include_postprocess:
                    return seed, await scrape_store(seed, settings, fetcher)

                base = seed.store_url.rstrip("/")
                homepage = await fetcher.get_text(base)
                store = classify_store(homepage, base, settings)
                store = _apply_seed_store_metadata(store, seed)

                extracted = await _fetch_all_products(base=base, settings=settings, fetcher=fetcher)
                products = [normalized for p in extracted if p.images if (normalized := normalize_product(p)) is not None]
                return seed, ScrapeResult(store=store, products=products)
            except Exception as exc:
                return seed, exc

    tasks = [asyncio.create_task(_run(seed)) for seed in seeds]
    task_to_seed = {task: seed for task, seed in zip(tasks, seeds)}
    total_tasks = len(tasks)
    completed_tasks = 0
    stall_interval = int(getattr(settings, "crawl_stall_log_interval_sec", 60) or 0)
    try:
        pending: set[asyncio.Task[tuple[StoreSeed, ScrapeResult | Exception]]] = set(tasks)
        while pending:
            if stall_interval > 0:
                done, pending = await asyncio.wait(
                    pending,
                    timeout=stall_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    sample_pending = [task_to_seed[t].store_url for t in list(pending)[:3]]
                    logger.warning(
                        "Crawl still in progress: completed=%s/%s pending=%s sample_pending=%s",
                        completed_tasks,
                        total_tasks,
                        len(pending),
                        sample_pending,
                    )
                    continue
            else:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                completed_tasks += 1
                yield await task
    finally:
        for task in list(pending if "pending" in locals() else tasks):
            if not task.done():
                task.cancel()
        await fetcher.close()
