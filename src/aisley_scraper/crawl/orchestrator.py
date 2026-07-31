from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.extract.policies import fetch_shipping_returns
from aisley_scraper.extract.shopify_products import extract_products_from_products_json
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


async def _attach_shipping_returns(store, base: str, fetcher: Fetcher, settings: Settings, homepage_html: str | None) -> None:
    """Best-effort capture of the store's returns/shipping policy text + url."""
    try:
        text, url = await fetch_shipping_returns(
            base, fetcher, settings, homepage_html=homepage_html
        )
        store.shipping_returns = text
        store.shipping_returns_url = url
    except Exception as exc:
        logger.warning("Failed to capture shipping/returns for %s: %s", base, exc)


async def _fetch_all_products(
    *,
    base: str,
    settings: Settings,
    fetcher: Fetcher,
) -> tuple[list[ProductRecord], bool]:
    """Return ``(kept_products, catalog_complete)``.

    ``catalog_complete`` is True only when pagination reached a genuine end of
    the catalog — a well-formed products.json with an explicitly-empty products
    list, or a store that repeats pages (all unique products already seen). It
    is False when pagination stopped for any reason that leaves an unknown tail:
    a fetch error mid-run, the per-store item cap, an anomalous 200 lacking a
    products array (a WAF/block), or exhausting max_pages. Callers must NOT
    reconcile removals (mark absent products unavailable) on an incomplete
    scrape — the un-scraped tail is not actually gone.
    """
    page_limit = max(1, settings.shopify_products_page_limit)
    max_pages = max(1, settings.shopify_products_max_pages)
    max_items_per_store = max(0, settings.shopify_products_max_items_per_store)

    # Returns products that PASS filtering (kids/non-apparel/cosmetics + the
    # min-images rule). The per-store cap counts only these kept products, so
    # filtered-out items don't consume the budget — i.e. the cutoff is applied
    # AFTER filtering. CLIP image validation runs later and may reduce further.
    kept: list[ProductRecord] = []
    seen_product_ids: set[str] = set()
    complete = False

    for page in range(1, max_pages + 1):
        products_url = f"{base}/products.json?limit={page_limit}&page={page}"
        try:
            payload = await fetcher.get_json(products_url)
        except Exception as exc:  # noqa: BLE001 - end-of-catalog, keep what we have
            # Some stores return a 4xx (or non-JSON) PAST their last page instead of
            # an empty list, and a few rate-limit deep pagination. Treat that as the
            # end of the catalog and keep whatever we've already scraped rather than
            # failing the entire store (which previously dropped big shops like
            # kith/feature/charmingcharlie that 400'd at a deep page).
            if page == 1:
                raise  # genuine failure on the first page -> let the store fail
            logger.warning(
                "Stopping pagination for %s at page %s (%s); keeping %s products so far",
                base,
                page,
                exc,
                len(kept),
            )
            break
        extracted = extract_products_from_products_json(payload, settings, base_url=base)

        new_this_page = 0
        for product in extracted:
            if product.product_id in seen_product_ids:
                continue
            new_this_page += 1
            seen_product_ids.add(product.product_id)
            normalized = normalize_product(product)
            if normalized is None:
                continue
            kept.append(normalized)

            if max_items_per_store > 0 and len(kept) >= max_items_per_store:
                logger.warning(
                    "Reached per-store product cap (after filtering) for %s: kept=%s cap=%s",
                    base,
                    len(kept),
                    max_items_per_store,
                )
                # Truncated by the cap -> catalog is NOT fully known.
                return kept, False

        products_raw = payload.get("products", []) if isinstance(payload, dict) else []
        if not isinstance(products_raw, list) or not products_raw:
            # Genuine end only when the response is a well-formed products.json
            # with an explicitly-empty list. A 200 lacking the key (block/anomaly)
            # is NOT a real end, so leave complete=False.
            if isinstance(payload, dict) and "products" in payload:
                complete = True
            break
        # Some stores ignore the ?page param and return the SAME products on every
        # page; without this guard they'd paginate to max_pages (or until a 4xx).
        # Stop once a page contributes no new product ids.
        if new_this_page == 0:
            logger.warning(
                "No new products on page %s for %s; stopping (store repeats pages)",
                page,
                base,
            )
            complete = True
            break

    return kept, complete


async def scrape_store(seed: StoreSeed, settings: Settings, fetcher: Fetcher) -> ScrapeResult:
    base = seed.store_url.rstrip("/")
    homepage = await fetcher.get_text(base)
    store = classify_store(homepage, base, settings)
    store = _apply_seed_store_metadata(store, seed)
    await _attach_shipping_returns(store, base, fetcher, settings, homepage)

    # _fetch_all_products already returns filtered + capped products.
    products, complete = await _fetch_all_products(base=base, settings=settings, fetcher=fetcher)
    # CLIP image validation trims each product's images to validated ones and
    # drops products left with none (mutates the list in place).
    await verify_product_images(products=products, fetcher=fetcher, settings=settings)

    return ScrapeResult(store=store, products=products, scrape_complete=complete)


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
                await _attach_shipping_returns(store, base, fetcher, settings, homepage)

                # _fetch_all_products already returns filtered + capped products
                # (image validation is deferred to phase 2 in this path).
                products, complete = await _fetch_all_products(
                    base=base, settings=settings, fetcher=fetcher
                )
                return seed, ScrapeResult(
                    store=store, products=products, scrape_complete=complete
                )
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
