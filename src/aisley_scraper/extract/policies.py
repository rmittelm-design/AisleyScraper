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
    # Generically-named policy pages: many stores put shipping AND returns on a
    # single page whose slug says neither (e.g. a tempo's /pages/online-store-policy).
    "/pages/online-store-policy",
    "/pages/store-policy",
    "/pages/store-policies",
    "/pages/policies",
    "/pages/our-policies",
    "/pages/customer-care",
    "/pages/customer-service",
    "/pages/faq",
    "/pages/help",
)

# "final sale" / "all sales are final" IS a returns policy — a store that
# accepts no returns often never writes the word "return" (nostandingnyc.com).
_RETURNS_KW = re.compile(
    r"return|refund|exchange|final sale|all sales?[\s\w]{0,12}final|no returns",
    re.IGNORECASE,
)
_SHIPPING_KW = re.compile(r"shipping|delivery|dispatch", re.IGNORECASE)

# Generically-named policy links ("Online Store Policy", "Customer Care",
# "Terms"). Such a page usually covers BOTH shipping and returns, so it is a
# candidate for either category; the per-category text check below still
# decides whether the page actually covers that category.
_GENERIC_POLICY_KW = re.compile(
    r"polic(?:y|ies)|customer (?:care|service)|\bfaq\b|help", re.IGNORECASE
)

# Legal/compliance pages that a generic "…policy" match otherwise drags in.
# A privacy notice or T&C page is not a shipping/returns policy, but it clears
# the phrasing gate (it mentions "30 days", "receipt", ...), so exclude by name.
_EXCLUDE_KW = re.compile(
    r"privacy|cookie|gdpr|ccpa|accessibility|legal|imprint|"
    r"terms[\s\-_]*(?:of[\s\-_]*(?:service|use)|and[\s\-_]*conditions)|"
    r"do[\s\-_]*not[\s\-_]*sell",
    re.IGNORECASE,
)

# A genuine policy leads with returns/shipping wording. Used to avoid excluding
# a real refund policy that merely *mentions* "Terms" or links to a /legal/ URL.
_POLICY_LEAD = re.compile(
    r"^\W*(refund|return|shipping|delivery|exchange|order|store polic)", re.IGNORECASE
)


def _is_legal_page_text(text: str) -> bool:
    """True when text reads as a privacy/T&C/accessibility page, not a policy."""
    head = re.sub(r"^[A-Z][A-Z &]+:\s*", "", (text or "").lstrip())
    if _POLICY_LEAD.match(head):
        return False
    return bool(_EXCLUDE_KW.search(head[:200]))
# Link discovery must be broader than the category keywords, or a page named
# only "…-policy" is never even considered.
_ANY_KW = re.compile(
    r"return|refund|exchange|shipping|delivery|dispatch|"
    r"polic(?:y|ies)|terms|customer (?:care|service)|\bfaq\b|help",
    re.IGNORECASE,
)

# (category, dedicated paths, keyword matcher) — we capture one page per category.
_CATEGORIES: tuple[tuple[str, tuple[str, ...], re.Pattern[str]], ...] = (
    ("returns", _RETURNS_PATHS, _RETURNS_KW),
    ("shipping", _SHIPPING_PATHS, _SHIPPING_KW),
)

# At or below this length, a single policy phrase means a terse-but-real
# policy ("All of our pieces are final sale."). Density is meaningless at
# this size: the per-1000-char divisor is floored at 1.0.
_TERSE_POLICY_MAX_CHARS = 800
# How many accepted pages to score per category before settling on the best.
_MAX_CANDIDATES_TO_SCORE = 3
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
    "form", "aside", "iframe", "select",
)
# NOTE: <footer> is deliberately NOT hard-stripped. Some stores publish their
# only policy in a collapsible footer section (greatlabels.com keeps its entire
# return policy there), so footers are stripped conditionally below.
# Substring class/id matches: themes name menus wildly (e.g. doors.nyc uses
# `mega-menu-container`, which a plain `.mega-menu` selector does NOT match).
_CHROME_SELECTORS = (
    '[role="navigation"]', '[role="banner"]',
    '[class*="mega-menu"]', '[class*="site-nav"]', '[class*="main-nav"]',
    '[class*="navbar"]', '[class*="navigation"]', '[class*="dropdown-menu"]',
    '[class*="site-header"]',
    '[id*="mega-menu"]', '[id*="site-nav"]',
    ".breadcrumb", ".cart", ".newsletter",
)

# Overlays/interstitials that leak in as "policy" text: cart drawers ("added to
# your bag"), hero sliders ("Pause slideshow"), promo bars, cookie notices.
# Stripped ONLY when they contain no policy phrasing — themes reuse these class
# names for real content (e.g. a policy inside a `.banner` wrapper), and blanket
# removal deleted legitimate policies.
_SOFT_CHROME_SELECTORS = (
    # Footer variants are conditional, never hard-stripped: a store may publish
    # its only policy in a collapsible footer section. `[role="contentinfo"]`
    # and `site-footer` used to be hard-stripped, which deleted greatlabels.com's
    # entire return policy before extraction ever ran.
    "footer", '[class*="footer"]', '[role="contentinfo"]', '[class*="site-footer"]',
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
    r"your cart is empty|continue shopping|"
    r"choosing a selection results in|full page refresh)\b",
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
    and easily clears a naive keyword check, so require two distinct substantive
    phrases (e.g. "30 days" + "refund") — OR one phrase that dominates a short
    text, which is how a terse policy reads ("All of our pieces are final sale.").
    A long navigation blob can never satisfy the second case: its density is low.
    """
    signals = _policy_signal_count(text)
    if signals >= 2:
        return True
    if signals == 1:
        # A lone signal is only convincing in short text that isn't UI chrome
        # ("Choosing a selection results in a full page refresh.").
        return len(text) <= _TERSE_POLICY_MAX_CHARS and not _NAV_MARKERS.search(text)
    return False


_MAX_CANDIDATE_CHARS = 8000
# A container scoring at least this is clearly the policy body; below it we
# also scan loose blocks (cheap insurance for unusual layouts).
_STRONG_CANDIDATE_SCORE = 4.0


# Shipping/delivery terms. Counting mentions separates a page that IS about
# shipping (many) from a returns page that merely name-drops it once ("return
# shipping is at your cost"). Used only to make the shipping slot shipping-aware.
_SHIPPING_TERM = re.compile(
    r"\bship\w*|deliver\w*|dispatch\w*|freight|postage|courier|carrier", re.IGNORECASE
)


def _shipping_richness(text: str) -> int:
    return len(_SHIPPING_TERM.findall(text or ""))


def _candidate_score(text: str, *, category: str | None = None) -> float:
    """Policy-signal DENSITY (distinct signals per 1000 chars), nav-penalised.

    Density — not a raw count — is what separates a policy from a menu: a
    sprawling nav blob accumulates a few incidental signals across thousands of
    characters, while a real policy is short and signal-dense.

    For the ``shipping`` category the score is made shipping-AWARE: a dense
    RETURNS page mentions "shipping" once and would otherwise out-score a store's
    dedicated shipping/delivery page for the shipping slot (shopalexis' refund
    page beating /pages/alexis-shipping-return-policy; boden's beating
    /pages/orders-deliveries). Weighting by how shipping-rich the text is lets a
    real shipping page win while a returns page (one mention) barely moves.
    """
    signals = _policy_signal_count(text)
    if signals < 2:
        return 0.0
    density = signals / max(1.0, len(text) / 1000.0)
    if _NAV_MARKERS.search(text[:400]):
        density *= 0.1
    if category == "shipping":
        density *= 1.0 + min(_shipping_richness(text), 12) / 2.0
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
    # Wrong content type: a privacy notice / T&C page can be dense in policy
    # phrasing yet is not a shipping or returns policy. Strip the section label
    # first so "RETURNS:\nPRIVACY POLICY ..." is still detected.
    if _is_legal_page_text(text):
        return True
    if not _looks_like_policy(text):
        return True
    # Short policy-like text is fine as-is; density only discriminates once a
    # value is long enough for navigation padding to be the explanation.
    if len(text) <= _TERSE_POLICY_MAX_CHARS:
        return False
    density = _policy_signal_count(text) / (len(text) / 1000.0)
    if density >= min_score:
        return False
    # Low density on its own does NOT mean junk: a wordy, genuine policy
    # (pookieandsebastian: 11 signals over 8000 chars) scores low. What low
    # density plus a NAVIGATION lead indicates is menu padding.
    return bool(_NAV_MARKERS.search(text[:400]))


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

    # Scan block elements when no known container scored *well*. A weak-but-
    # nonzero container (e.g. a homepage <main>) previously suppressed this,
    # hiding policies published outside the main content — such as a collapsible
    # footer section.
    if max((_candidate_score(t) for t in candidates), default=0.0) < _STRONG_CANDIDATE_SCORE:
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
        # Never follow privacy/T&C/legal links — they read like policies but
        # are not shipping or returns policies.
        if _EXCLUDE_KW.search(match_text):
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


# Terms-of-service / T&C pages. Rejected as a whole (legal boilerplate), but a
# store may leave its dedicated returns page empty and publish the real returns
# policy inside T&C (dansonjewelers.com), so as a LAST resort we lift just the
# returns section out of one of these.
_TC_PATHS: tuple[str, ...] = (
    "/policies/terms-of-service",
    "/policies/terms-and-conditions",
    "/pages/terms-of-service",
    "/pages/terms-and-conditions",
    "/pages/terms",
)

# Heading that begins the returns/refunds section within a T&C page.
_RETURN_SECTION_START = re.compile(
    r"returns?\s*(?:&|and)\s*exchanges?|exchanges?\s*(?:&|and)\s*returns?|"
    r"returns?\s+(?:&|and)\s+refunds?|returns?\s+policy|refund\s+policy|"
    r"return\s*(?:&|and)\s*exchange|exchange\s+and\s+return",
    re.IGNORECASE,
)

# Unrelated legal sections that mark the END of the returns section in a T&C page.
_TC_SECTION_END = re.compile(
    r"privacy\s+polic|governing\s+law|intellectual\s+propert|"
    r"limitation\s+of\s+liabilit|disclaimer|indemnif|dispute\s+resolution|"
    r"terms\s+of\s+use|acceptance\s+of\s+terms|prohibited\s+use|"
    r"changes\s+to\s+(?:these\s+)?terms",
    re.IGNORECASE,
)


def _extract_returns_from_legal(text: str | None) -> str | None:
    """Lift the returns/refunds section out of a T&C page body, or None.

    The T&C page as a whole is legal boilerplate (rejected), but the returns
    section inside it is a real policy. Extract from its heading up to the next
    unrelated legal section (or the char cap). Because the result now LEADS with
    returns wording it clears ``_is_legal_page_text``; we still require real
    policy phrasing so a bare mention ("no refunds under these Terms") is not
    mistaken for a policy.
    """
    if not text:
        return None
    start = _RETURN_SECTION_START.search(text)
    if not start:
        return None
    section = text[start.start() :]
    end = _TC_SECTION_END.search(section, 200)
    if end:
        section = section[: end.start()]
    section = section[:_MAX_CHARS_PER_POLICY].strip()
    if len(section) < _MIN_POLICY_CHARS:
        return None
    if not _looks_like_policy(section) or _is_legal_page_text(section):
        return None
    return section


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
        # Category-specific links first, then generically-named policy pages
        # ("Online Store Policy"), which often carry both policies.
        candidates += [url for url, match_text in homepage_links if keyword.search(match_text)]
        candidates += [
            url
            for url, match_text in homepage_links
            if not keyword.search(match_text) and _GENERIC_POLICY_KW.search(match_text)
        ]

        seen: set[str] = set()
        scored: list[tuple[float, str, str]] = []
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
            # A privacy notice / T&C page clears the phrasing gate ("30 days",
            # "receipt") but is not a shipping or returns policy.
            if _is_legal_page_text(text):
                logger.debug("Skipping %s for %s: reads as privacy/legal page", url, category)
                continue
            # Score accepted candidates and keep the best rather than the
            # first: a store may expose the same policy on several pages of
            # differing quality (pookieandsebastian's /pages/return-policy is
            # tighter than its /policies/refund-policy).
            scored.append((_candidate_score(text, category=category), url, text))
            if len(scored) >= _MAX_CANDIDATES_TO_SCORE:
                break
        if scored:
            scored.sort(key=lambda item: -item[0])
            best_score_, best_url, best_txt = scored[0]
            # Stored uncapped; the cap is applied at emit time so a combined
            # page (which stands in for BOTH categories) gets both budgets.
            found[category] = (best_url, best_txt)

        # Last resort: the policy may be published inline ON the homepage rather
        # than on any dedicated page — commonly a collapsible footer section
        # (greatlabels.com keeps its whole return policy there).
        if category not in found and homepage_html:
            text = _clean_text(homepage_html)
            if (
                text
                and len(text) >= _MIN_POLICY_CHARS
                and keyword.search(text)
                and _looks_like_policy(text)
                and not _is_legal_page_text(text)
            ):
                found[category] = (base, text)

    # Last resort for returns: the store left its dedicated returns page empty
    # but published the policy inside terms-of-service. Lift just the returns
    # section (the T&C page as a whole is rejected above as legal boilerplate).
    if "returns" not in found:
        for path in _TC_PATHS:
            section = _extract_returns_from_legal(await _page_text(f"{base}{path}"))
            if section:
                found["returns"] = (f"{base}{path}", section)
                break

    returns = found.get("returns")
    shipping = found.get("shipping")
    if not returns and not shipping:
        return None, None

    # If both resolved to the same combined page, emit it once.
    if returns and shipping and returns[0] == shipping[0]:
        combined = returns[1][: _MAX_CHARS_PER_POLICY * 2]
        return f"SHIPPING & RETURNS:\n{combined}", returns[0]

    parts: list[str] = []
    urls: list[str] = []
    if returns:
        parts.append(f"RETURNS:\n{returns[1][:_MAX_CHARS_PER_POLICY]}")
        urls.append(returns[0])
    if shipping:
        parts.append(f"SHIPPING:\n{shipping[1][:_MAX_CHARS_PER_POLICY]}")
        urls.append(shipping[0])
    return "\n\n".join(parts), " | ".join(urls)
