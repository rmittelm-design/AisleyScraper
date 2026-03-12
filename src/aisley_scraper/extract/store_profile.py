from __future__ import annotations

import re
from bs4 import BeautifulSoup

from aisley_scraper.config import Settings
from aisley_scraper.models import StoreProfile

_INSTAGRAM_RE = re.compile(r"instagram\.com/([A-Za-z0-9_.-]+)", re.IGNORECASE)


def _extract_instagram(html: str) -> str | None:
    match = _INSTAGRAM_RE.search(html)
    if not match:
        return None
    handle = match.group(1).strip("/@ ")
    return handle or None


def _extract_address(soup: BeautifulSoup) -> str | None:
    addr = soup.find("address")
    if addr and addr.get_text(strip=True):
        return addr.get_text(" ", strip=True)
    return None


def classify_store(html: str, website: str, settings: Settings) -> StoreProfile:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else website

    lower = html.lower()
    online_signals = any(signal in lower for signal in ["/cart", "/checkout", "add-to-cart", "products"]) 
    address = _extract_address(soup)
    instagram = _extract_instagram(html)

    if settings.classify_require_ecom_signal:
        store_type = "online" if online_signals else "offline"
    else:
        store_type = "online" if online_signals or not address else "offline"

    if store_type == "offline":
        instagram = None

    return StoreProfile(
        store_name=title,
        website=website,
        store_type=store_type,
        instagram_handle=instagram,
        address=address,
    )
