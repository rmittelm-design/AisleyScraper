from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.extract.shopify_products import extract_products_from_products_json
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.extract.store_profile import classify_store
from aisley_scraper.models import ProductRecord, ScrapeResult, StoreSeed
from aisley_scraper.normalize.products import enforce_attribute_policy


async def scrape_store(seed: StoreSeed, settings: Settings, fetcher: Fetcher) -> ScrapeResult:
    base = seed.store_url.rstrip("/")
    homepage = await fetcher.get_text(base)
    store = classify_store(homepage, base, settings)

    products: list[ProductRecord] = []
    products_url = f"{base}/products.json?limit={settings.shopify_products_page_limit}&page=1"
    try:
        payload = await fetcher.get_json(products_url)
        extracted = extract_products_from_products_json(payload, settings, base_url=base)
        await verify_product_images(products=extracted, fetcher=fetcher, settings=settings)
        await enrich_gender_probabilities_for_products(
            products=extracted,
            fetcher=fetcher,
            concurrency=settings.image_validation_concurrency,
        )
        products = [enforce_attribute_policy(p) for p in extracted if p.images]
    except Exception:
        products = []

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
    seeds: list[StoreSeed], settings: Settings
) -> AsyncIterator[tuple[StoreSeed, ScrapeResult | Exception]]:
    fetcher = Fetcher(settings)
    semaphore = asyncio.Semaphore(settings.crawl_global_concurrency)

    async def _run(seed: StoreSeed) -> tuple[StoreSeed, ScrapeResult | Exception]:
        async with semaphore:
            try:
                return seed, await scrape_store(seed, settings, fetcher)
            except Exception as exc:
                return seed, exc

    tasks = [asyncio.create_task(_run(seed)) for seed in seeds]
    try:
        for task in asyncio.as_completed(tasks):
            yield await task
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await fetcher.close()
