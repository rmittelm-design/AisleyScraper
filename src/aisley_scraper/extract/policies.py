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


_RATE_TIER = (
    r"(?:standard|expedited|express|ground|economy|priority|overnight|"
    r"next[\s-]?day|two[\s-]?day|2[\s-]?day|first[\s-]?class|flat[\s-]?rate|"
    r"usps|ups|fedex|dhl)"
)
_RATE_ROW_RE = re.compile(r"(?i)(" + _RATE_TIER + r"[^$]{0,90}?)\$\s?(\d+(?:\.\d{2})?)\b")


def _merge_missing_rate_lines(best_txt: str, all_texts: list[str | None]) -> str:
    """Append shipping rate-table rows (method + $ cost) present on ANY candidate
    page but absent from the chosen text.

    The extractor keeps only the single best-scoring shipping page, so a concrete
    rate table on a lower-scoring candidate — e.g. a nav-heavy /pages/shipping-info
    whose density lost to a clean /policies/shipping-policy that has no rates —
    is otherwise dropped, and the stored policy loses real shipping prices
    (marinelayer's Standard $5 / Expedited $20, lemsshoes' $5.95). This grafts
    those cost rows back on. Guards against free-shipping *thresholds* ("free over
    $X", "$X+ orders"), which are not shipping costs; skips $0 rows and any amount
    already represented in the chosen text.
    """
    have = {a.replace(" ", "") for a in re.findall(r"\$\s?\d+(?:\.\d{2})?", best_txt)}
    rows: list[str] = []
    seen: set[tuple[str, str]] = set()
    for text in all_texts:
        if not text:
            continue
        for m in _RATE_ROW_RE.finditer(text):
            if float(m.group(2)) <= 0:  # $0 = free, not a cost
                continue
            before = text[max(0, m.start() - 24) : m.start()].lower()
            if "free" in before or "complimentary" in before or "over" in before:
                continue  # a free-shipping threshold, not a rate
            if text[m.end() : m.end() + 4].lstrip().startswith("+"):
                continue  # "$X+ orders" threshold
            label = re.sub(r"\s+", " ", m.group(1)).strip(" *–-,:")
            if "free" in label.lower() or "over" in label.lower():
                continue
            amt = f"${m.group(2)}"
            if amt.replace(" ", "") in have:
                continue  # cost already in the chosen text
            key = (label.lower(), amt)
            if key in seen:
                continue
            seen.add(key)
            rows.append(f"{label} {amt}")
    if not rows:
        return best_txt
    return best_txt.rstrip() + "\nShipping rates: " + "; ".join(rows) + "."


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
        # A tabbed/accordion "Shipping & Returns" page keeps each tab in its own
        # container; picking only the densest one drops the other tab (Alexis'
        # shipping-rates panel was lost, leaving returns text in the shipping
        # slot). Concatenate every STRONG, distinct policy block so both survive;
        # the returns/shipping split happens downstream at emit time.
        strong = sorted({t for t in candidates if _candidate_score(t) >= _STRONG_CANDIDATE_SCORE},
                        key=len, reverse=True)
        kept: list[str] = []
        for t in strong:
            if not any(t in k for k in kept):  # drop blocks nested in a longer kept one
                kept.append(t)
        if len(kept) > 1:
            return "\n\n".join(kept)[: _MAX_CANDIDATE_CHARS * 3]
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

# STRONG section boundaries that end the returns discussion. Only structural
# legal-section wording that starts a NEW section — NOT bare warranty phrasing
# ("without warranties of any kind", "we disclaim any warranty of our own"),
# which lawyers routinely interleave BETWEEN a returns-eligibility statement and
# the returns-remedy/refund-timeline paragraph. "as-is" needs the legal
# continuation, so resale "sold/provided as-is, where-is" item notes never stop
# the run.
# RELIABLE boundaries — this wording never appears inside a genuine returns
# policy, so it always ends the returns run (even mid-run, cutting a following
# refund-mentioning liability clause).
_STOP_RUN = re.compile(
    r"as[\s-]is[\"']?\s+(?:and\s+)?(?:as[\s-]available|basis|without\s+warrant)|"
    r"disclaimer\s+of\s+warrant|limitation\s+of\s+liabilit|in\s+no\s+event\s+shall|"
    r"shall\s+not\s+be\s+liable|force\s+majeure|severabilit|entire\s+agreement|"
    r"indemnif|arbitration|intellectual\s+propert|privacy\s+polic|"
    r"acceptance\s+of\s+terms|prohibited\s+use|"
    r"third[\s-]party\s+link|user\s+comments|errors,?\s+inaccuracies|optional\s+tools",
    re.IGNORECASE,
)
# Trailing-buffer boundaries — the above PLUS ambiguous phrases ("governing law",
# "dispute resolution") that may occur as sentence-initial returns prose ("governing
# law does not limit your return rights"). They must not break the run mid-stream,
# but they do bound the trailing buffer so a real "Governing Law" heading right
# after the returns run (flattened, unpunctuated) is not appended.
_STOP_BUF = re.compile(
    _STOP_RUN.pattern + r"|governing\s+law|dispute\s+resolution",
    re.IGNORECASE,
)


def _extract_returns_from_legal(text: str | None) -> str | None:
    """Lift the returns/refunds section out of a T&C page body, or None.

    The T&C page as a whole is legal boilerplate (rejected), but the returns
    section inside it is a real policy. We keep the CONTIGUOUS run of returns
    content from its heading — extending across a short gap (an interleaved
    warranty note) but stopping at a strong section boundary — so the remedy/
    refund-timeline tail is not lost, while trailing legal boilerplate (which
    carries no return vocabulary) is excluded. Because the result LEADS with
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
    hits = list(_RET_CONTENT.finditer(section))
    if not hits:
        return None
    # Extend through the contiguous returns discussion: keep advancing while the
    # next return mention is close (<600 chars) AND no strong section boundary
    # intervenes.
    end = hits[0].end()
    for m in hits[1:]:
        gap = section[end : m.start()]
        if len(gap) > 900 or _STOP_RUN.search(gap):
            break
        end = m.end()
    # Trailing buffer to finish the last sentence — but never past the next strong
    # boundary (e.g. a governing-law heading in flattened, unpunctuated text).
    stop = _STOP_BUF.search(section, end)
    buf_end = min(stop.start(), end + 120) if stop else end + 120
    section = section[:buf_end][:_MAX_CHARS_PER_POLICY].strip()
    if len(section) < _MIN_POLICY_CHARS:
        return None
    if not _looks_like_policy(section) or _is_legal_page_text(section):
        return None
    return section


# Classify a policy BLOCK as shipping- vs returns-dominant, to split a combined
# page's concatenated blocks (from _clean_text) into the correct labelled section
# (Alexis' shipping-rates tab vs its returns tab).
_SHIP_CONTENT = re.compile(
    r"free\s+shipping|flat[\s-]?rate|ships?\s+within|dispatch|calculated\s+at\s+checkout|"
    r"\d+\s*[-–]?\s*\d*\s*business\s*days?|ground\s+shipping|express\s+shipping|"
    r"standard\s+shipping|2nd\s*day|next\s*day|overnight|expedited|tracking\s+(?:number|info)|"
    r"\bcarrier\b|customs|duties|delivery\s+(?:time|estimate|window|option)|p\.?\s?o\.?\s*box|"
    r"shipping\s+(?:cost|rate|fee|method|option)|shipping\s+is\s+\$",
    re.IGNORECASE,
)
_RET_CONTENT = re.compile(
    r"\breturn|\brefund|\bexchange|store\s+credit|restocking|final\s+sale|\brma\b",
    re.IGNORECASE,
)


_SHIP_RATE_ANCHOR = re.compile(
    r"domestic\s+shipping|shipping\s+rates?|shipping\s+(?:&|and)\s+(?:delivery|handling)|"
    r"shipping\s+(?:is|costs?)\s+\$|free\s+shipping|shipping\s+guidelines|"
    r"delivery\s+(?:information|options|rates?|times?)|"
    r"(?:ground|standard|express|expedited|overnight)\s+shipping\s+is\s+\$",
    re.IGNORECASE,
)
# Footer / social / account chrome that a shipping window should not be made of;
# used to penalize a full-page window anchored on a footer benefits band or an
# account UI rather than the real shipping section.
_SHIP_WIN_CHROME = re.compile(
    r"instagram|facebook|pinterest|tiktok|youtube|linkedin|\bcareers\b|"
    r"©|\(c\)\s*\d|all\s+rights\s+reserved|newsletter|"
    r"add\s+to\s+cart|shopping\s+cart|my\s+account|create\s+account|reset\s+your\s+password",
    re.IGNORECASE,
)


def _full_visible_text(html: str) -> str:
    h = re.sub(r"<(script|style|noscript|svg|template)[\s\S]*?</\1>", " ", html, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", h)
    txt = (txt.replace("&amp;", "&").replace("&#39;", "'").replace("&nbsp;", " ")
              .replace("&ndash;", "-").replace("&rsquo;", "'").replace("&quot;", '"'))
    return re.sub(r"\s+", " ", txt).strip()


def _shipping_from_full_page(html: str) -> str | None:
    """Lift a shipping window from a page's FULL visible text, for when the real
    shipping section sits outside any known policy container (Alexis' rates tab
    is diluted by a long ship-to-countries list, so the density cleaner drops it)."""
    txt = _full_visible_text(html)
    # Take the EARLIEST shipping-rate anchor whose window reads as shipping. Trim
    # the 3200-char forward window at the first footer/social/account chrome (so
    # the result isn't padded with it) and require the trimmed window to be
    # shipping-dominant (its shipping content >= its return vocabulary). This skips
    # a top "free shipping" promo bar (on a combined page its window is
    # returns/menu-heavy), trims away a footer benefits band, and still recovers a
    # real rate block diluted by a long ship-to-countries list. When nothing
    # qualifies it returns None rather than emit returns/chrome as the shipping
    # policy. The clean-container clobber is prevented by the call-site gate.
    for m in _SHIP_RATE_ANCHOR.finditer(txt):
        seg = txt[m.start(): m.start() + 3200]
        cm = _SHIP_WIN_CHROME.search(seg)
        if cm:
            seg = seg[: cm.start()]
        seg = seg.strip()
        sh = len(_SHIP_CONTENT.findall(seg))
        rt = len(_RET_CONTENT.findall(seg))
        if sh >= 3 and sh >= rt:
            return seg[:_MAX_CHARS_PER_POLICY]
    return None


def _category_text(text: str, category: str) -> str:
    """From a possibly-combined policy body (blocks joined by ``_clean_text``),
    keep only the blocks relevant to ``category``. No-op for a single block, so a
    normal single-policy page is unchanged."""
    blocks = [b.strip() for b in text.split("\n\n") if b.strip()]
    if len(blocks) <= 1:
        return text
    kept: list[str] = []
    for b in blocks:
        ship = len(_SHIP_CONTENT.findall(b))
        ret = len(_RET_CONTENT.findall(b))
        if category == "shipping":
            if ship >= 2 and ship >= ret:
                kept.append(b)
        elif ret > ship:  # returns
            kept.append(b)
    return "\n\n".join(kept).strip() or text


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

    # Cache cleaned text (and raw html) per URL so a combined page (or a repeated
    # candidate) is fetched at most once across both categories.
    page_cache: dict[str, str | None] = {}
    raw_cache: dict[str, str | None] = {}

    async def _raw(url: str) -> str | None:
        if url in raw_cache:
            return raw_cache[url]
        try:
            html = await fetcher.get_text(url)
        except Exception:
            html = None
        raw_cache[url] = html
        return html

    async def _page_text(url: str) -> str | None:
        if url in page_cache:
            return page_cache[url]
        html = await _raw(url)
        text = _clean_text(html) if html else None
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
            if category == "shipping":
                # A concrete rate table (Standard $5 / Expedited $20 / …) often
                # lives on a candidate that lost the density scoring (a nav-heavy
                # /pages/shipping-info). Harvest its cost rows so they aren't
                # dropped just because a cleaner, rate-free page outscored it.
                best_txt = _merge_missing_rate_lines(
                    best_txt, [t for _sc, _u, t in scored]
                )
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

    # The shipping slot's container text can be the wrong tab (Alexis' rates sit
    # in a non-standard container the cleaner drops, leaving return text here).
    # ONLY when that container is not already real shipping — it is
    # returns-dominant or nearly shipping-empty — recover a shipping window from
    # the page's full visible text. This keeps a clean, shipping-rich container
    # from being clobbered by a chrome-polluted full-page window (the guard on
    # match count alone is defeated by a "free shipping" banner + nav, which add
    # their own shipping-word matches).
    if "shipping" in found:
        surl, stext = found["shipping"]
        ship_hits = len(_SHIP_CONTENT.findall(stext))
        ret_hits = len(_RET_CONTENT.findall(stext))
        if ship_hits < 2 or ret_hits > ship_hits:
            html = await _raw(surl)
            win = _shipping_from_full_page(html) if html else None
            if win and len(_SHIP_CONTENT.findall(win)) > ship_hits:
                found["shipping"] = (surl, win)

    returns = found.get("returns")
    shipping = found.get("shipping")
    if not returns and not shipping:
        return None, None

    # If both resolved to the same combined page AND to the SAME text, emit it
    # once. (When the shipping slot was re-derived from a different part of the
    # page — Alexis' rates tab via the full-text fallback — the texts differ, so
    # fall through to separate, correctly-split sections instead of collapsing to
    # the returns text.)
    if returns and shipping and returns[0] == shipping[0] and returns[1] == shipping[1]:
        combined = returns[1][: _MAX_CHARS_PER_POLICY * 2]
        return f"SHIPPING & RETURNS:\n{combined}", returns[0]

    parts: list[str] = []
    urls: list[str] = []
    if returns:
        rtext = _category_text(returns[1], "returns")
        parts.append(f"RETURNS:\n{rtext[:_MAX_CHARS_PER_POLICY]}")
        urls.append(returns[0])
    if shipping:
        stext = _category_text(shipping[1], "shipping")
        parts.append(f"SHIPPING:\n{stext[:_MAX_CHARS_PER_POLICY]}")
        urls.append(shipping[0])
    return "\n\n".join(parts), " | ".join(urls)
