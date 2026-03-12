from __future__ import annotations

import asyncio
import logging
from pathlib import PurePosixPath
from urllib.parse import urlparse

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.image_validation import ImageValidationFailure, validate_and_normalize_upload
from aisley_scraper.models import ProductRecord

logger = logging.getLogger(__name__)


def _filename_from_url(image_url: str) -> str:
    path = urlparse(image_url).path
    name = PurePosixPath(path).name
    if name:
        return name
    return "image.jpg"


async def _verify_single_image_url(
    image_url: str,
    fetcher: Fetcher,
    max_retries: int,
) -> bool:
    attempts = max(1, max_retries + 1)
    for attempt in range(1, attempts + 1):
        try:
            content = await fetcher.get_bytes(image_url)
            validate_and_normalize_upload(content=content, filename=_filename_from_url(image_url))
            return True
        except (httpx.HTTPError, ImageValidationFailure, TimeoutError) as exc:
            if attempt >= attempts:
                logger.info("Image validation rejected %s: %s", image_url, exc)
                return False
            await asyncio.sleep(0.25 * attempt)
        except Exception as exc:
            logger.info("Image validation failed unexpectedly for %s: %s", image_url, exc)
            return False
    return False


async def verify_product_images(
    *,
    products: list[ProductRecord],
    fetcher: Fetcher,
    settings: Settings,
) -> None:
    if not settings.image_validation_enabled:
        return

    unique_urls = {
        image_url.strip()
        for product in products
        for image_url in product.images
        if image_url and image_url.strip()
    }
    if not unique_urls:
        return

    semaphore = asyncio.Semaphore(settings.image_validation_concurrency)
    verdicts: dict[str, bool] = {}

    async def _run(image_url: str) -> None:
        async with semaphore:
            verdicts[image_url] = await _verify_single_image_url(
                image_url=image_url,
                fetcher=fetcher,
                max_retries=settings.image_validation_max_retries,
            )

    await asyncio.gather(*(_run(url) for url in unique_urls))

    for product in products:
        product.images = [url for url in product.images if verdicts.get(url.strip(), False)]

    # Keep only products that still have at least one validated image.
    products[:] = [product for product in products if product.images]
