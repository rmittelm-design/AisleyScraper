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

# Containers that hold the real policy/page body on Shopify themes, best first.
# Without this we fall back to <body> and a mega-nav can eat the whole char
# budget before any policy text is reached.
_CONTENT_SELECTORS: tuple[str, ...] = (
    "div.shopify-policy__body",  # canonical Shopify /policies/* pages
    "div.rte",                   # standard Shopify rich-text page body
    "article",
    "div.page-content",
    "div.main-content",
    "div.container.main",
    "section.page",
    "div.page",
    "main",
)

# Non-content chrome to drop before extracting.
_CHROME_TAGS = (
    "script", "style", "noscript", "header", "nav", "svg",
    "footer", "form", "aside", "iframe", "select",
)
# Substring class/id matches: themes name menus wildly (e.g. doors.nyc uses
# `mega-menu-container`, which a plain `.mega-menu` selector does NOT match).
_CHROME_SELECTORS = (
    '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]',
    '[class*="mega-menu"]', '[class*="site-nav"]', '[class*="main-nav"]',
    '[class*="navbar"]', '[class*="navigation"]', '[class*="dropdown-menu"]',
    '[class*="site-header"]', '[class*="site-footer"]',
    '[id*="mega-menu"]', '[id*="site-nav"]',
    ".breadcrumb", ".cart", ".newsletter",
)

# Overlays/interstitials that leak in as "policy" text: cart drawers ("added to
# your bag"), hero sliders ("Pause slideshow"), promo bars, cookie notices.
# Stripped ONLY when they contain no policy phrasing — themes reuse these class
# names for real content (e.g. a policy inside a `.banner` wrapper), and blanket
# removal deleted legitimate policies.
_SOFT_CHROME_SELECTORS = (
    '[class*="modal"]', '[class*="popup"]', '[class*="drawer"]',
    '[class*="slideshow"]', '[class*="slider"]', '[class*="carousel"]',
    '[class*="announcement"]', '[class*="promo"]', '[class*="banner"]',
    '[class*="cookie"]', '[class*="toast"]', '[class*="notification"]',
    '[class*="skip-to"]', '[class*="skip-link"]',
)

# Storefront navigation/boilerplate — if this dominates the head of a candidate
# it is a menu, not a policy.
_NAV_MARKERS = re.compile(
    r"\b(shop all|all clothing|new arrivals|best sellers|shopping cart|add to cart|"
    r"my account|wish ?list|newsletter|subscribe|gift cards?|size guide|store locator|"
    # UI chrome that leaks in when a theme renders overlays inline
    r"skip to (?:content|main)|pause slideshow|play slideshow|close menu|"
    r"added to your (?:bag|cart)|this modal|opens in a new window|"
    r"your cart is empty|continue shopping)\b",
    re.IGNORECASE,
)

# Substantive policy phrasing. Deliberately EXCLUDES bare "shipping"/"returns",
# which appear as nav link labels on every page — requiring these prevents a
# navigation menu from being mistaken for a policy body.
_POLICY_SIGNALS = re.compile(
    r"\b("
    # timeframes
    r"\d+\s*(?:business\s*|calendar\s*)?days?|business day|"
    # returns / refunds
    r"refund(?:ed|able)?|exchange[sd]?|restocking|store credit|original packaging|"
    r"final sale|unworn|unused|return label|proof of purchase|receipt|"
    # shipping (phrases, never the bare nav label "shipping"/"delivery")
    r"free\s+(?:standard\s+|express\s+|ground\s+)?(?:shipping|delivery|returns?)|"
    r"flat[\s-]?(?:rate|fee)|ships?\s+within|dispatch(?:ed)?|"
    r"(?:calculated|shown|displayed)\s+at\s+checkout|"
    r"international\s+(?:shipping|delivery|orders?)|"
    r"orders?\s+over\s+\$?\d+|tracking\s+(?:number|information)|"
    r"carrier|customs|duties"
    r")\b",
    re.IGNORECASE,
)


def _normalize(node) -> str:
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def _policy_signal_count(text: str) -> int:
    """Number of DISTINCT substantive policy phrases present."""
    return len({m.group(0).lower() for m in _POLICY_SIGNALS.finditer(text)})


def _looks_like_policy(text: str) -> bool:
    """True when text reads like an actual policy body, not a nav menu.

    A storefront menu contains the words "Shipping" and "Returns" (link labels)
    and easily clears a naive keyword check, so require at least two distinct
    substantive policy phrases (e.g. "30 days" + "refund").
    """
    return _policy_signal_count(text) >= 2


_MAX_CANDIDATE_CHARS = 8000


def _candidate_score(text: str) -> float:
    """Policy-signal DENSITY (distinct signals per 1000 chars), nav-penalised.

    Density — not a raw count — is what separates a policy from a menu: a
    sprawling nav blob accumulates a few incidental signals across thousands of
    characters, while a real policy is short and signal-dense.
    """
    signals = _policy_signal_count(text)
    if signals < 2:
        return 0.0
    density = signals / max(1.0, len(text) / 1000.0)
    if _NAV_MARKERS.search(text[:400]):
        density *= 0.1
    return density


def stored_policy_is_weak(text: str | None, *, min_score: float = 2.0) -> bool:
    """True when an ALREADY-STORED shipping_returns value looks like boilerplate.

    Uses signal density, not a raw count: a 4000-char navigation blob usually
    contains two incidental policy words somewhere, so a presence check reports
    it as fine. Density separates a menu (many chars, few signals) from a real
    policy (few chars, many signals).

    Deliberately does NOT apply ``_candidate_score``'s nav penalty. That penalty
    exists to rank competing blocks *within one page*; against a stored value it
    misfires on genuine policies that merely mention a nav-ish phrase (e.g.
    "gift cards are non-returnable"), which flagged real 13-signal policies as
    junk.
    """
    if not text or not text.strip():
        return True
    signals = _policy_signal_count(text)
    if signals < 2:
        return True
    density = signals / max(1.0, len(text) / 1000.0)
    return density < min_score


def _clean_text(html: str) -> str:
    """Extract the policy body, preferring the densest real content block.

    Tries known content containers first, then (for unfamiliar themes) scans
    block elements, picking the highest policy-signal density. This keeps a
    page's mega-nav from crowding out the policy. Falls back to the full body
    when nothing looks like a policy, so callers can still reject it.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(list(_CHROME_TAGS)):
        tag.decompose()
    for selector in _CHROME_SELECTORS:
        for node in soup.select(selector):
            node.decompose()
    # Soft chrome: only remove when it carries no policy phrasing, so a policy
    # wrapped in a themed `.banner`/`.promo` container survives.
    for selector in _SOFT_CHROME_SELECTORS:
        for node in soup.select(selector):
            if _policy_signal_count(_normalize(node)) < 2:
                node.decompose()

    candidates: list[str] = []
    for selector in _CONTENT_SELECTORS:
        for node in soup.select(selector):
            text = _normalize(node)
            if len(text) >= _MIN_POLICY_CHARS:
                candidates.append(text)

    # Unknown theme: no known container scored — scan block elements instead.
    if not any(_candidate_score(t) > 0 for t in candidates):
        for node in soup.find_all(("div", "section", "article")):
            text = _normalize(node)
            if _MIN_POLICY_CHARS <= len(text) <= _MAX_CANDIDATE_CHARS:
                candidates.append(text)

    best_text = ""
    best_score = 0.0
    for text in candidates:
        score = _candidate_score(text)
        if score > best_score or (score == best_score > 0 and 0 < len(text) < len(best_text)):
            best_text, best_score = text, score

    if best_text and best_score > 0:
        return best_text
    body = soup.body or soup
    return _normalize(body)


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
            # A storefront nav mentions "shipping"/"returns" on every page, so
            # the keyword check alone accepts menus. Require real policy phrasing.
            if not _looks_like_policy(text):
                logger.debug("Skipping %s for %s: no policy phrasing (likely nav)", url, category)
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
