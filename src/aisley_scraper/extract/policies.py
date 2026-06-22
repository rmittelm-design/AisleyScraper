from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher

logger = logging.getLogger(__name__)

# Returns/refund policy: canonical Shopify + common paths, in priority order.
_RETURNS_PATHS: tuple[str, ...] = (
    "/policies/refund-policy",
    "/pages/returns",
    "/pages/return-policy",
    "/pages/returns-exchanges",
    "/pages/refund-policy",
    "/pages/returns-policy",
)

# Shipping/delivery policy: canonical Shopify + common paths, in priority order.
_SHIPPING_PATHS: tuple[str, ...] = (
    "/policies/shipping-policy",
    "/pages/shipping",
    "/pages/shipping-policy",
    "/pages/delivery",
    "/pages/shipping-info",
)

# Combined pages that cover both shipping AND returns — used as a fallback for
# whichever category still has no dedicated page.
_COMBINED_PATHS: tuple[str, ...] = (
    "/pages/shipping-returns",
    "/pages/shipping-and-returns",
    "/pages/returns-shipping",
    "/pages/shipping-returns-policy",
)

_RETURNS_KW = re.compile(r"return|refund|exchange", re.IGNORECASE)
_SHIPPING_KW = re.compile(r"shipping|delivery|dispatch", re.IGNORECASE)
_ANY_KW = re.compile(r"return|refund|exchange|shipping|delivery|dispatch", re.IGNORECASE)

# (category, dedicated paths, keyword matcher) — we capture one page per category.
_CATEGORIES: tuple[tuple[str, tuple[str, ...], re.Pattern[str]], ...] = (
    ("returns", _RETURNS_PATHS, _RETURNS_KW),
    ("shipping", _SHIPPING_PATHS, _SHIPPING_KW),
)

_MAX_CHARS_PER_POLICY = 4000
_MIN_POLICY_CHARS = 80


def _clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "nav", "svg"]):
        tag.decompose()
    main = soup.find("main") or soup.body or soup
    text = main.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _homepage_policy_links(homepage_html: str, base_url: str) -> list[tuple[str, str]]:
    """Return (url, match_text) for homepage links that look policy-related.

    match_text combines href + visible label so each category can decide whether
    a link is relevant to it (returns vs shipping).
    """
    try:
        soup = BeautifulSoup(homepage_html, "html.parser")
    except Exception:
        return []
    base_netloc = urlparse(base_url).netloc.lower()
    links: list[tuple[str, str]] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        label = anchor.get_text(" ", strip=True)
        if not href:
            continue
        match_text = f"{href} {label}"
        if not _ANY_KW.search(match_text):
            continue
        absolute = urljoin(base_url + "/", href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        # Stay on the store's own domain.
        if parsed.netloc and parsed.netloc.lower() != base_netloc:
            continue
        normalized = absolute.split("#")[0].rstrip("/")
        if normalized in seen:
            continue
        seen.add(normalized)
        links.append((normalized, match_text))
    return links


async def fetch_shipping_returns(
    base_url: str,
    fetcher: Fetcher,
    settings: Settings,
    *,
    homepage_html: str | None = None,
) -> tuple[str | None, str | None]:
    """Locate BOTH the store's returns policy and its shipping policy.

    For each category (returns, shipping) we try the canonical Shopify/common
    paths first, then combined shipping+returns pages, then matching homepage
    links — and keep the first page whose cleaned text actually reads like that
    category's policy. The two are captured independently (separate char
    budgets) so a long return policy can't crowd out the shipping policy.

    Returns ``(cleaned_text, source_url)`` where cleaned_text has labelled
    sections (RETURNS / SHIPPING) and source_url joins the page URLs with " | ".
    Returns ``(None, None)`` when neither policy is found.
    """
    base = base_url.rstrip("/")
    homepage_links = _homepage_policy_links(homepage_html, base) if homepage_html else []

    # Cache cleaned text per URL so a combined page (or a repeated candidate) is
    # fetched at most once across both categories.
    page_cache: dict[str, str | None] = {}

    async def _page_text(url: str) -> str | None:
        if url in page_cache:
            return page_cache[url]
        try:
            html = await fetcher.get_text(url)
        except Exception:
            page_cache[url] = None
            return None
        text = _clean_text(html)
        page_cache[url] = text
        return text

    found: dict[str, tuple[str, str]] = {}  # category -> (url, text)

    for category, paths, keyword in _CATEGORIES:
        candidates: list[str] = [f"{base}{p}" for p in paths]
        candidates += [f"{base}{p}" for p in _COMBINED_PATHS]
        candidates += [url for url, match_text in homepage_links if keyword.search(match_text)]

        seen: set[str] = set()
        for url in candidates:
            if url in seen:
                continue
            seen.add(url)
            text = await _page_text(url)
            if not text or len(text) < _MIN_POLICY_CHARS:
                continue
            if not keyword.search(text):
                continue
            found[category] = (url, text[:_MAX_CHARS_PER_POLICY])
            break

    returns = found.get("returns")
    shipping = found.get("shipping")
    if not returns and not shipping:
        return None, None

    # If both resolved to the same combined page, emit it once.
    if returns and shipping and returns[0] == shipping[0]:
        return f"SHIPPING & RETURNS:\n{returns[1]}", returns[0]

    parts: list[str] = []
    urls: list[str] = []
    if returns:
        parts.append(f"RETURNS:\n{returns[1]}")
        urls.append(returns[0])
    if shipping:
        parts.append(f"SHIPPING:\n{shipping[1]}")
        urls.append(shipping[0])
    return "\n\n".join(parts), " | ".join(urls)
