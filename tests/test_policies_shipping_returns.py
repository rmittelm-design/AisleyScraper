import asyncio

from aisley_scraper.config import Settings
from aisley_scraper.extract import policies


def _settings() -> Settings:
    return Settings(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="k",
        SUPABASE_STORAGE_BUCKET="b",
        SUPABASE_STORAGE_PATH="p",
    )


class _FakeFetcher:
    """Serves canned HTML for specific URLs and 404s everything else."""

    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages
        self.requested: list[str] = []

    async def get_text(self, url: str) -> str:
        self.requested.append(url)
        if url in self.pages:
            return self.pages[url]
        raise RuntimeError(f"404 {url}")


_RETURN_HTML = (
    "<html><body><main>Our return policy: items may be returned or exchanged "
    "within 30 days for a full refund. Refunds are issued to the original "
    "payment method once the return is received.</main></body></html>"
)
_SHIP_HTML = (
    "<html><body><main>Shipping policy: orders ship within 2 business days. "
    "We offer free standard delivery on US orders over $100. International "
    "shipping rates are calculated at checkout.</main></body></html>"
)
_COMBINED_HTML = (
    "<html><body><main>Shipping & Returns: we ship within 2 days (free delivery "
    "over $100) and accept returns or exchanges within 30 days for a refund."
    "</main></body></html>"
)


def test_captures_both_returns_and_shipping_from_canonical_paths():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": _RETURN_HTML,
            f"{base}/policies/shipping-policy": _SHIP_HTML,
        }
    )
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    assert text is not None and url is not None
    assert "RETURNS:" in text and "SHIPPING:" in text
    assert "refund" in text.lower() and "delivery" in text.lower()
    assert f"{base}/policies/refund-policy" in url
    assert f"{base}/policies/shipping-policy" in url


def test_long_return_policy_does_not_crowd_out_shipping():
    base = "https://shop.example.com"
    long_return = (
        "<html><body><main>Return policy refund exchange "
        + ("blah " * 5000)
        + "</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": long_return,
            f"{base}/policies/shipping-policy": _SHIP_HTML,
        }
    )
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    # Shipping must still be captured despite the huge return policy.
    assert "SHIPPING:" in text
    assert f"{base}/policies/shipping-policy" in url
    # Return section is capped, not unbounded.
    assert len(text) < 12000


def test_combined_page_emitted_once():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher({f"{base}/pages/shipping-returns": _COMBINED_HTML})
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    assert text.startswith("SHIPPING & RETURNS:")
    assert url == f"{base}/pages/shipping-returns"


def test_returns_none_when_no_policy_pages():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher({})  # everything 404s
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    assert text is None and url is None


# A store (dansonjewelers.com) whose dedicated returns page is empty but whose
# real return policy lives inside terms-of-service, wrapped in legal boilerplate.
_TC_WITH_RETURNS_HTML = (
    "<html><body><main>"
    "These Terms of Service govern your use of this website; by accessing it you "
    "agree to be bound by these terms and conditions and all applicable laws. "
    "RETURN POLICY. Your purchase, in new condition with the original receipt, "
    "may be exchanged or returned for store credit within seven days of the "
    "original purchase date. No returns are accepted after 7 days. Sale or "
    "clearance merchandise is not returnable or exchangeable. "
    "GOVERNING LAW. These terms are governed by the laws of the state of X, and "
    "any dispute shall be resolved in its courts."
    "</main></body></html>"
)
_TC_PURE_LEGAL_HTML = (
    "<html><body><main>"
    "These Terms of Service govern your use of this website. By accessing this "
    "site you agree to these terms and conditions. GOVERNING LAW: these terms "
    "are governed by the laws of the state. LIMITATION OF LIABILITY: we are not "
    "liable for indirect damages. INTELLECTUAL PROPERTY: all content is ours."
    "</main></body></html>"
)


def test_returns_lifted_from_terms_of_service_when_dedicated_page_empty():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher(
        {
            # dedicated returns page exists but is empty (below _MIN_POLICY_CHARS)
            f"{base}/policies/refund-policy": "<html><body><main></main></body></html>",
            f"{base}/policies/terms-of-service": _TC_WITH_RETURNS_HTML,
        }
    )
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None
    assert "RETURNS:" in text
    assert "return policy" in text.lower() and "seven days" in text.lower()
    assert url == f"{base}/policies/terms-of-service"
    # only the returns section is lifted, not the unrelated legal boilerplate
    assert "governing law" not in text.lower()


def test_pure_legal_terms_page_is_not_captured_as_returns():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher({f"{base}/policies/terms-of-service": _TC_PURE_LEGAL_HTML})
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is None and url is None


def test_dedicated_returns_page_preferred_over_terms_fallback():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": _RETURN_HTML,  # "within 30 days"
            f"{base}/policies/terms-of-service": _TC_WITH_RETURNS_HTML,  # "seven days"
        }
    )
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert "RETURNS:" in text and url == f"{base}/policies/refund-policy"
    # the T&C fallback must NOT run when a real returns page exists
    assert "seven days" not in text.lower()


def test_shipping_scoring_favors_shipping_rich_text_over_returns():
    """Shipping-slot scoring must prefer a shipping-rich page over a returns page
    that merely name-drops 'shipping' (the shopalexis/boden bug)."""
    returns = (
        "Return policy: items may be returned or exchanged within 30 days for a full "
        "refund to the original payment method. Return shipping is at your cost."
    )
    shipping = (
        "Shipping policy: free standard shipping on US orders over $100. Express and "
        "international shipping are calculated at checkout. Orders are dispatched with "
        "tracking; delivery times vary by carrier and customs may apply."
    )
    # For the shipping category the shipping-rich text wins clearly...
    assert policies._candidate_score(shipping, category="shipping") > policies._candidate_score(
        returns, category="shipping"
    )
    # ...while the category-less (returns) score does not apply the shipping boost.
    assert policies._candidate_score(returns) > 0


_RETURNS_MENTIONS_SHIPPING = (
    "<html><body><main>Return Policy. Items may be returned or exchanged within 30 "
    "days of delivery for a full refund to your original payment method. Final sale "
    "items are not returnable. Return shipping is the customer's responsibility."
    "</main></body></html>"
)
_DEDICATED_SHIPPING = (
    "<html><body><main>Shipping and Delivery. We ship within 2 business days. Free "
    "standard shipping on US orders over $100. Express and international shipping are "
    "calculated at checkout. Orders are dispatched with tracking; delivery times vary "
    "by carrier and customs or duties may apply.</main></body></html>"
)


def test_shipping_slot_uses_dedicated_page_over_returns_page():
    """A dedicated shipping page (custom slug, only found via a homepage link) must
    win the shipping slot over a returns page that is also offered to it."""
    base = "https://shop.example.com"
    homepage = (
        "<html><body>"
        '<a href="/policies/refund-policy">Refund policy</a>'
        '<a href="/pages/orders-delivery">Shipping and Delivery</a>'
        "</body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": _RETURNS_MENTIONS_SHIPPING,
            f"{base}/pages/orders-delivery": _DEDICATED_SHIPPING,
        }
    )
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings(), homepage_html=homepage)
    )
    assert "SHIPPING:" in text
    assert f"{base}/pages/orders-delivery" in url  # shipping from the dedicated page
    ship = text.split("SHIPPING:")[-1].lower()
    assert "at checkout" in ship or "dispatched" in ship  # real shipping content


# A tabbed "Shipping & Returns" page: shipping and returns live in SEPARATE
# containers (Alexis). _clean_text must keep both, not drop a tab.
_TABBED_COMBINED_HTML = (
    "<html><body>"
    "<div class='rte'>Return Policy. Items may be returned within 30 days of delivery "
    "for a full refund to the original payment method. Exchanges and store credit are "
    "available. All sale items are final sale.</div>"
    "<div class='rte'>Domestic Shipping Rates. Ground shipping is $25, 2nd Day is $60, "
    "Next Day is $80. Orders ship within 1 business day via UPS; delivery in 2-7 business "
    "days. Free shipping over $150. Orders are not available for P.O. Box addresses.</div>"
    "</body></html>"
)


def test_clean_text_keeps_both_tabs_of_a_combined_page():
    t = policies._clean_text(_TABBED_COMBINED_HTML)
    assert "return" in t.lower() and "refund" in t.lower()
    assert "ground shipping" in t.lower() and "$25" in t  # the shipping tab is NOT dropped


def test_category_text_splits_combined_blocks():
    combined = policies._clean_text(_TABBED_COMBINED_HTML)
    ship = policies._category_text(combined, "shipping")
    ret = policies._category_text(combined, "returns")
    assert "ground shipping" in ship.lower() and "$25" in ship
    assert "refund" in ret.lower()
    assert "$25" not in ret  # shipping rates don't leak into the returns section
    assert "refund" not in ship.lower()  # return text doesn't leak into shipping


def test_combined_shipping_page_yields_shipping_not_returns():
    """A store whose shipping candidate is a combined tabbed page must land real
    shipping content in the SHIPPING slot, not the returns tab (Alexis bug)."""
    base = "https://shop.example.com"
    refund = (
        "<html><body><main>Return Policy. Items may be returned within 30 days for a full "
        "refund to the original payment method. Exchanges and store credit are available. "
        "Sale items are final sale.</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": refund,
            f"{base}/pages/shipping-returns": _TABBED_COMBINED_HTML,  # a _COMBINED_PATHS slug
        }
    )
    text, _ = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    # the shipping rates must survive to the output (previously dropped)
    assert "ground shipping" in text.lower() and "$25" in text


# Regression: a storefront mega-nav wraps the policy. Before the content-container
# fix, _clean_text returned <body> text and the menu ate the whole char budget,
# so shipping_returns was stored as pure navigation (observed on doors.nyc).
_NAV = (
    "<nav>SHOP CLOTHING All Clothing Blouses Bodysuits Cardigans Coats Dresses "
    "Hoodies Jackets Denim Knitwear SHOES All Shoes Boots Flats Heels Sneakers "
    "BAGS All Bags Clutch bags Tote bags ACCESSORIES Belts Hats Scarves "
    "New Arrivals Best Sellers Shopping Cart My Account</nav>"
)
_MENU_NOISE = "<div class='site-nav'>" + ("Designer Brand Name " * 400) + "</div>"


def _nav_wrapped(policy_html: str) -> str:
    return f"<html><body>{_NAV}{_MENU_NOISE}{policy_html}{_NAV}</body></html>"


def test_policy_extracted_from_content_container_not_navigation():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": _nav_wrapped(
                "<div class='shopify-policy__body'>Items may be returned within 30 "
                "days for a full refund. Items must be unworn and in their original "
                "packaging. Final sale items are non-returnable.</div>"
            ),
            f"{base}/policies/shipping-policy": _nav_wrapped(
                "<div class='shopify-policy__body'>Orders ship within 2 business days. "
                "Free shipping on orders over $100. A tracking number is emailed once "
                "your order ships.</div>"
            ),
        }
    )
    text, _ = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None
    # The real policy content is present...
    assert "30 days" in text and "original packaging" in text
    assert "business days" in text and "tracking number" in text.lower()
    # ...and the navigation menu is not.
    for junk in ("All Clothing", "Shopping Cart", "Designer Brand Name", "Best Sellers"):
        assert junk not in text, f"navigation leaked into policy text: {junk!r}"


def test_navigation_only_page_is_not_accepted_as_policy():
    """A menu mentioning 'Shipping'/'Returns' must not be stored as a policy."""
    base = "https://shop.example.com"
    nav_only = (
        "<html><body><main>SHOP All Clothing New Arrivals Best Sellers Shipping "
        "Returns Shopping Cart My Account Gift Cards Store Locator About Us "
        + ("Brand Name " * 300)
        + "</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": nav_only,
            f"{base}/policies/shipping-policy": nav_only,
        }
    )
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is None and url is None


def test_finds_generically_named_policy_page_via_homepage_link():
    """A page whose slug says neither 'shipping' nor 'returns' must still be found.

    Regression: atemponewyork.com keeps both policies at /pages/online-store-policy.
    It matched no canonical path, and link discovery ignored it because the old
    keyword only matched shipping/returns words — so the store stored nothing.
    """
    base = "https://shop.example.com"
    homepage = (
        "<html><body><a href='/pages/online-store-policy'>Online Store Policy</a>"
        "</body></html>"
    )
    policy = (
        "<html><body><main>Online store policy SHIPPING All orders ship within 2 "
        "business days with a tracking number. RETURNS Items may be returned within "
        "30 days for a refund if unworn and in original packaging."
        "</main></body></html>"
    )
    fetcher = _FakeFetcher({f"{base}/pages/online-store-policy": policy})
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings(), homepage_html=homepage)
    )
    assert text is not None, "generically-named policy page was not discovered"
    assert "30 days" in text and "business days" in text
    assert "online-store-policy" in url


def test_generic_policy_path_is_a_canonical_candidate():
    """Even with no homepage, common generic policy slugs are probed."""
    base = "https://shop.example.com"
    policy = (
        "<html><body><main>Store policy: orders ship within 3 business days. "
        "Returns accepted within 30 days for a refund on unworn items."
        "</main></body></html>"
    )
    fetcher = _FakeFetcher({f"{base}/pages/store-policy": policy})
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None
    assert "store-policy" in url


def test_terse_final_sale_policy_is_captured():
    """A no-returns store may never write the word "return".

    Regression: nostandingnyc.com's whole policy is "All of our pieces are final
    sale...". It failed the returns keyword (no return/refund/exchange) and the
    two-signal rule, so the store captured nothing.
    """
    base = "https://shop.example.com"
    terse = (
        "<html><body><main>All of our pieces are final sale. If you experience any "
        "problems with your order please email us at shop@example.com so we can "
        "help solve the issue.</main></body></html>"
    )
    fetcher = _FakeFetcher({f"{base}/pages/returns-exchanges": terse})
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None, "terse final-sale policy was rejected"
    assert "final sale" in text.lower()


def test_long_navigation_with_one_signal_is_still_rejected():
    """The terse allowance must not let a nav blob through."""
    nav_blob = "Shop All Clothing New Arrivals Best Sellers " * 120 + " final sale"
    assert not policies._looks_like_policy(nav_blob)
    assert policies.stored_policy_is_weak(nav_blob)


def test_policy_published_inline_in_the_homepage_footer():
    """Some stores have no policy page — the policy lives in the footer.

    Regression: greatlabels.com keeps its whole return policy in a collapsible
    footer block. The homepage was never tried as a policy source, and the
    footer was hard-stripped (via <footer> / [role=contentinfo]) before
    extraction, so the store captured nothing.
    """
    base = "https://shop.example.com"
    homepage = (
        "<html><body>"
        "<main>Shop Our Inventory Handbags Shoes Clothing Jacket Sale price $990.00</main>"
        "<footer role='contentinfo' class='site-footer'>"
        "  <div class='footer__item-padding'>"
        "    <p class='footer__title'>Return Policy</p>"
        "    <div class='collapsible-content'><div class='footer__collapsible'>"
        "      <p>All sales are FINAL, however, if there is a discrepancy with the "
        "      quality of your purchase we offer returns within 14 days of purchase. "
        "      You can return your product for store credit. We do not provide the "
        "      shipping label.</p>"
        "    </div></div>"
        "  </div>"
        "</footer></body></html>"
    )
    fetcher = _FakeFetcher({})  # no policy pages exist at all
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings(), homepage_html=homepage)
    )
    assert text is not None, "footer-inline policy was not found"
    assert "14 days" in text and "store credit" in text
    # The storefront/product content must not be what got captured.
    assert "Sale price" not in text


def test_privacy_and_terms_pages_are_not_stored_as_policies():
    """Privacy/T&C pages clear the phrasing gate but are not shipping/returns.

    Regression: broadening link discovery to generic '...policy' links made
    nostandingnyc store its PRIVACY POLICY and dansonjewelers its TERMS AND
    CONDITIONS as shipping_returns.
    """
    base = "https://shop.example.com"
    homepage = (
        "<html><body>"
        "<a href='/pages/privacy-policy'>Privacy Policy</a>"
        "<a href='/pages/terms-and-conditions'>Terms and Conditions</a>"
        "</body></html>"
    )
    privacy = (
        "<html><body><main>PRIVACY POLICY Last updated February 28, 2023. This "
        "privacy notice explains what information we collect. You may request a "
        "receipt of your data within 30 days of your request."
        "</main></body></html>"
    )
    terms = (
        "<html><body><main>PLEASE READ OUR TERMS AND CONDITIONS CAREFULLY BEFORE "
        "USING OUR SITE. These terms govern your use. Refund of fees may be "
        "requested within 30 days subject to a restocking review."
        "</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/pages/privacy-policy": privacy,
            f"{base}/pages/terms-and-conditions": terms,
        }
    )
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings(), homepage_html=homepage)
    )
    assert text is None and url is None, f"legal page stored as policy: {text!r}"


def test_looks_like_policy_requires_substantive_phrasing():
    assert not policies._looks_like_policy("Shipping Returns Cart Account Menu")
    assert policies._looks_like_policy(
        "Returns accepted within 30 days for a refund on unworn items."
    )


def test_finds_shipping_only_when_returns_missing():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher({f"{base}/policies/shipping-policy": _SHIP_HTML})
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    assert text is not None
    assert "SHIPPING:" in text and "RETURNS:" not in text
    assert url == f"{base}/policies/shipping-policy"


def test_ui_boilerplate_is_not_a_terse_policy():
    """The terse allowance must not admit Shopify UI chrome.

    Regression: dansonjewelers captured "Refund policy Choosing a selection
    results in a full page refresh. Opens in a new window." — 98 chars of
    variant-selector boilerplate that leads with "Refund policy".
    """
    junk = (
        "Refund policy Choosing a selection results in a full page refresh. "
        "Opens in a new window."
    )
    assert not policies._looks_like_policy(junk)
    assert policies.stored_policy_is_weak(junk)


def test_verbose_real_policy_is_not_flagged_weak():
    """Low signal density alone must not condemn a wordy but genuine policy.

    Regression: pookieandsebastian's real policy (11 signals over ~8000 chars)
    scored 1.37 density and was reported as boilerplate.
    """
    verbose = (
        "QUESTIONS ABOUT YOUR ORDER & RETURNS ORDER FULFILLMENT Once the order is "
        "placed, processing begins. You can expect merchandise within 4 to 12 "
        "business days. Returns accepted within 30 days for a refund on unworn "
        "items in original packaging; final sale items are excluded. "
    ) + ("We appreciate your patience while we prepare your order. " * 90)
    assert not policies.stored_policy_is_weak(verbose)


def test_navigation_padded_capture_is_still_weak():
    """Low density WITH a navigation lead is what actually indicates padding."""
    padded = (
        "Skip to content Shop now pay later New Arrivals SALE HANDBAGS ACCESSORIES "
        "Login Consign With Us Visit Our Store "
    ) + ("Designer Brand Name " * 300) + " returns within 30 days for a refund"
    assert policies.stored_policy_is_weak(padded)


def test_best_scoring_policy_page_wins_over_first_match():
    """A tighter page should beat an earlier-but-noisier one."""
    base = "https://shop.example.com"
    noisy = (
        "<html><body><main>QUESTIONS ABOUT YOUR ORDER Once the order is placed "
        "processing begins within 4 to 12 business days. Returns accepted within 30 "
        "days for a refund. " + ("Assorted filler copy about our brand story. " * 60)
        + "</main></body></html>"
    )
    tight = (
        "<html><body><main>Return &amp; Exchange Policy Items may be returned within "
        "30 days for a refund if unworn and in original packaging. Final sale items "
        "are excluded and a restocking fee may apply.</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": noisy,   # tried first
            f"{base}/pages/return-policy": tight,      # tried later, but better
        }
    )
    text, url = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None
    assert "/pages/return-policy" in url, f"picked the noisier page: {url}"


# --- Regression tests for the full-page shipping fallback + T&C returns bound ---
# (pre-merge review of fix/policy-terms-fallback surfaced these paths as unpinned)

_DILUTED_SHIP_TAB = (
    "<html><body>"
    "<div class='rte'>Return Policy. Items may be returned within 30 days of delivery "
    "for a full refund or exchange to the original payment method. Store credit is "
    "available. All sale items are final sale and cannot be returned.</div>"
    "<div class='rte'>Domestic Shipping Rates. Ground shipping is $25, 2nd Day is $60, "
    "Next Day is $80. Orders ship within 1 business day; delivery in 2-7 business days. "
    "Free shipping over $150. No P.O. Box addresses. We ship to: "
    + ", ".join(f"Country{i}" for i in range(400)) + ".</div>"
    "</body></html>"
)


def test_full_page_shipping_fallback_recovers_diluted_shipping_tab():
    """When the real shipping section is so diluted (huge ship-to list) that the
    density cleaner drops it and the shipping slot resolves to returns text, the
    full-page fallback recovers the rate window (exercises the swap at the emit
    step)."""
    # precondition: the cleaner drops the diluted shipping tab, so without the
    # fallback the shipping slot would hold only returns text (no rate figures).
    assert "$25" not in policies._clean_text(_DILUTED_SHIP_TAB)
    base = "https://shop.example.com"
    refund = (
        "<html><body><main>Return Policy. Items may be returned within 30 days for a full "
        "refund to the original payment method. Exchanges and store credit are available. "
        "Sale items are final sale.</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": refund,
            f"{base}/policies/shipping-policy": _DILUTED_SHIP_TAB,
        }
    )
    text, _ = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None and "SHIPPING:" in text
    ship = text.split("SHIPPING:")[-1]
    assert "ground shipping is $25" in ship.lower()  # recovered only via the fallback


def test_shipping_from_full_page_picks_densest_window_over_footer_link():
    """A stray 'Shipping Rates' footer/nav link must not strand the window in the
    footer: the densest window (the real delivery lead) wins."""
    html = (
        "<html><body>"
        "<main><h1>Delivery Information</h1><p>Standard delivery time is 3-5 business "
        "days. Orders are dispatched within 24 hours Monday to Friday. A tracking "
        "number is provided once your parcel ships. We ship to P.O. boxes and offer "
        "express shipping for an additional fee. International customers may owe "
        "customs and duties.</p></main>"
        "<footer><ul><li><a href='/pages/shipping-rates'>Shipping Rates</a></li>"
        "<li>Returns</li><li>Contact</li></ul>(c) 2026 Store</footer>"
        "</body></html>"
    )
    win = policies._shipping_from_full_page(html)
    assert win is not None
    assert "business days" in win.lower() and "tracking number" in win.lower()


def test_clean_shipping_container_not_clobbered_by_announcement_bar():
    """A clean dedicated shipping page carrying a 'free shipping' announcement bar
    and nav must keep its clean container; the full-page fallback must not replace
    a real shipping container with banner + menu chrome."""
    base = "https://shop.example.com"
    shipping = (
        "<html><body>"
        "<div class='announcement-bar'>Free shipping on all US orders over $50!</div>"
        + _NAV +
        "<main><div class='shopify-policy__body'>Ground shipping is a $5 flat rate. "
        "Orders ship within 2 business days via standard shipping. Expedited shipping "
        "is available at checkout. A tracking number is emailed once your order ships."
        "</div></main></body></html>"
    )
    refund = (
        "<html><body><main>Items may be returned within 30 days for a full refund. "
        "Items must be unworn and in original packaging. Final sale items are "
        "non-returnable.</main></body></html>"
    )
    fetcher = _FakeFetcher(
        {
            f"{base}/policies/refund-policy": refund,
            f"{base}/policies/shipping-policy": shipping,
        }
    )
    text, _ = asyncio.run(policies.fetch_shipping_returns(base, fetcher, _settings()))
    assert text is not None and "SHIPPING:" in text
    ship = text.split("SHIPPING:")[-1]
    assert "flat rate" in ship.lower() and "business days" in ship.lower()
    for junk in ("All Clothing", "Shopping Cart", "My Account", "Best Sellers"):
        assert junk not in ship, f"nav leaked into shipping via full-page fallback: {junk!r}"


def test_extract_returns_from_legal_bounds_terse_section():
    """A terse returns section is bounded at the next legal heading, not run on
    into governing-law / jurisdiction boilerplate (the fixed-200-char skip bug)."""
    body = (
        "Return Policy. All sales are final and no refunds are issued; however "
        "defective items may be exchanged within 14 days of delivery for store credit. "
        "Governing Law. These terms are governed by the laws of the state and any "
        "dispute is subject to the exclusive jurisdiction of its courts. "
        "Limitation of Liability. In no event shall we be liable for damages."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "store credit" in section.lower()
    assert "jurisdiction" not in section.lower()
    assert "limitation of liability" not in section.lower()


def test_extract_returns_from_legal_keeps_body_mentioning_legal_terms():
    """A legit returns body that MENTIONS 'Terms of Use' / 'governing law'
    mid-sentence must not be truncated at the mention; only a real following
    section heading ends it."""
    body = (
        "Returns and Refunds. All returns are subject to these Terms of Use. You may "
        "return unworn merchandise within 30 days of delivery for a full refund to the "
        "original payment method; exchanges and store credit are also available. "
        "Governing Law. These terms are governed by the laws of the state."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "30 days" in section and "store credit" in section.lower()
    assert "governed by the laws" not in section.lower()  # stops at the real heading


def test_extract_returns_from_legal_keeps_inline_disclaimer_label():
    """An inline 'Disclaimer:' label introducing return terms is not a section
    heading and must not truncate the returns body."""
    body = (
        "Return Policy. Disclaimer: all sale items are final and cannot be returned. "
        "Full-price items may be returned within 30 days for a refund if unworn with "
        "original tags attached."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "final" in section.lower() and "30 days" in section


def test_shipping_from_full_page_does_not_jump_past_lead_into_footer():
    """A trailing 'delivery time' phrase must not anchor the window past the real
    shipping lead into footer chrome; the lead (methods, tracking) is preserved."""
    html = (
        "<html><body>"
        "<div class='announcement-bar'>FREE SHIPPING ON ORDERS OVER $75</div>"
        "<nav>Home Shop Collections About Contact Account Cart</nav>"
        "<main><p>Free Shipping. We offer free shipping on all U.S. orders over $75. "
        "Orders ship within 1-2 business days via ground shipping. Standard and express "
        "shipping options are available at checkout, and a tracking number is emailed "
        "once your order ships. Please allow additional delivery time during holidays.</p>"
        "</main><footer>Newsletter Sign up Instagram Facebook Pinterest</footer>"
        "</body></html>"
    )
    win = policies._shipping_from_full_page(html)
    assert win is not None
    # the real shipping lead survives (the bug anchored at 'delivery time', losing it)
    assert "business days" in win.lower()
    assert "tracking number" in win.lower()
    assert "ground shipping" in win.lower()


def test_extract_returns_from_legal_drops_boilerplate_heading_without_punctuation():
    """A real trailing legal section with NO preceding period (flattened text /
    bulleted returns list) must still be dropped — it carries no return vocab."""
    body = (
        "Returns and Exchanges Items may be returned within 30 days Items must be "
        "unworn with original tags Refunds are issued to the original payment method "
        "Governing Law These Terms shall be governed by the laws of the State of "
        "California and any dispute shall be resolved in the courts located in Los "
        "Angeles County. You hereby consent to personal jurisdiction and waive any "
        "objection to venue."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "30 days" in section and "original tags" in section
    assert "governed by the laws" not in section.lower()
    assert "jurisdiction" not in section.lower()


def test_extract_returns_from_legal_keeps_returns_after_sentence_initial_mention():
    """A returns body whose later sentence merely BEGINS with 'Governing law ...'
    as prose (still discussing return rights) must not be truncated."""
    body = (
        "Return Policy. We accept returns within 30 days of delivery for a full "
        "refund. Governing law of your purchase does not limit any statutory consumer "
        "rights you may have, and we will honor all mandatory return rights in your "
        "jurisdiction. Please retain your receipt and contact us to arrange your "
        "return shipment."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "30 days" in section
    assert "return shipment" in section.lower()  # the tail was not dropped


def test_extract_returns_from_legal_drops_liability_boilerplate_with_refund_word():
    """Legal boilerplate that itself contains 'refund' (as-is / liability clauses)
    must still be cut — the semantic lookahead alone was fooled by it."""
    body = (
        "Returns & Refunds. We gladly accept returns within 30 days of delivery for a "
        "full refund to your original payment method. Items must be unworn with tags. "
        "Disclaimer of Warranties. THE PRODUCTS ARE PROVIDED ON AN AS IS AND AS "
        "AVAILABLE BASIS. We make no warranties, express or implied. In no event shall "
        "our liability exceed the amount you paid, and your sole remedy is to return "
        "the product for a refund of the purchase price actually paid by you. Governing "
        "Law. These Terms shall be governed by the laws of the State of California."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "30 days" in section and "unworn" in section.lower()
    for junk in ("as is", "warranties", "in no event", "liability", "governed by the laws"):
        assert junk not in section.lower(), f"legal boilerplate leaked: {junk!r}"


def test_extract_returns_from_legal_keeps_returns_tail_after_inline_disclaimer():
    """An inline 'Disclaimer:' inside a genuine returns policy followed by return-free
    logistics prose must not truncate the refund-timeframe tail."""
    body = (
        "Returns Policy. We happily accept returns within 30 days of delivery. To be "
        "eligible your item must be unused and in the same condition that you received "
        "it, in its original packaging with all tags attached. Disclaimer: we cannot be "
        "held responsible for items lost or damaged in transit on their way back to our "
        "warehouse, so we strongly recommend that you use a trackable shipping method "
        "and consider purchasing insurance for any higher-value merchandise, because we "
        "are simply unable to guarantee that we will receive the package that you have "
        "sent to us for processing at our facility. Once your package arrives and has "
        "passed our inspection, please allow five to ten business days for your refund "
        "to be issued back to the original method of payment used at purchase."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "five to ten business days" in section.lower()  # tail retained


def test_shipping_from_full_page_prefers_body_rates_over_footer_usp_band():
    """A late footer benefits/USP band that is shipping-dense must not out-select the
    real (earlier) shipping section in the page body."""
    countries = ", ".join(f"Country{i}" for i in range(400))
    html = (
        "<html><body>"
        "<nav>Home Shop New In Sale Account Cart</nav>"
        "<div class='rte'>Return Policy. Items may be returned within 30 days, final "
        "sale excluded, unworn with original tags.</div>"
        "<div class='rte'>Domestic Shipping Rates. Ground shipping is a flat rate. "
        "Orders ship within 1 business day. We ship to: " + countries + ".</div>"
        "<div class='usp'>Free Shipping over $150. Express shipping and overnight "
        "shipping available at checkout. We ship worldwide; customer pays customs and "
        "duties. Ground shipping on all domestic orders.</div>"
        "<footer>About Careers Instagram Facebook Pinterest (c) 2026</footer>"
        "</body></html>"
    )
    win = policies._shipping_from_full_page(html)
    assert win is not None
    assert win.lower().startswith("domestic shipping rates")
    assert "flat rate" in win.lower() and "business day" in win.lower()
    assert "Instagram" not in win and "Careers" not in win


def test_extract_returns_from_legal_keeps_resale_sold_as_is_condition():
    """A resale/vintage store's 'sold as-is' item-condition note (returns-relevant)
    must not be treated as a legal 'as is' warranty disclaimer and drop the policy."""
    body = (
        "Terms & Conditions. Return Policy. All pre-owned items are sold as-is; because our "
        "pieces are secondhand we note any flaws in the listing. Full-price items purchased "
        "online may be returned within 10 days of delivery for store credit only, provided the "
        "garment is unworn with tags. Sale and vintage items are final sale. Email us with your "
        "order number to begin a return. Governing Law. These Terms are governed by the laws of "
        "the State of California."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "10 days" in section and "store credit" in section.lower()
    assert "governed by the laws" not in section.lower()


def test_extract_returns_from_legal_still_cuts_legal_as_is_available_boilerplate():
    """The legal 'provided on an as is and as available basis' disclaimer must still
    be cut even though 'as is' is now context-sensitive."""
    body = (
        "Return Policy. Items may be returned within 30 days for a refund. The site and all "
        "products are provided on an as is and as available basis, and we make no warranties of "
        "any kind, express or implied."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "30 days" in section
    assert "as available" not in section.lower()
    assert "no warranties" not in section.lower()


def test_extract_returns_from_legal_keeps_resale_provided_as_is_condition():
    """'items are provided as-is' (resale item-condition) must survive just like
    'sold as-is' — only the legal 'as-is and as available/basis/without warranty'
    forms cut."""
    body = (
        "Return Policy. All secondhand items are provided as is due to their pre-loved "
        "nature, but if your order arrives damaged we accept returns within 5 days for a "
        "full refund; new items may be returned within 30 days with tags. Governing Law. "
        "These Terms are governed by the laws of the State of New York."
    )
    section = policies._extract_returns_from_legal(body)
    assert section is not None
    assert "5 days" in section and "30 days" in section
    assert "governed by the laws" not in section.lower()


def test_shipping_from_full_page_recovers_rates_past_promo_bar():
    """A top 'free shipping' promo bar must not strand the window before the real
    rate section further down the page."""
    countries = ", ".join(f"Country{i}" for i in range(240))
    menu = "Shop New In Dresses Tops Bottoms Shoes Accessories Sale About Contact " * 20
    returns = (
        "Returns. We accept returns within 30 days of delivery. Items must be unworn with "
        "tags. Return shipping is the customer's responsibility. Refunds are issued to the "
        "original payment method once received. " * 6
    )
    html = (
        "<html><body>"
        "<div class='announce'>Free shipping on U.S. orders over $150</div>"
        "<nav>" + menu + "</nav>"
        "<div class='rte'>" + returns + "</div>"
        "<div class='rte'>Shipping Rates. Domestic shipping is a $8.95 flat rate, free over "
        "$150. Express shipping is $24.95. Orders ship within 1-2 business days via USPS and "
        "UPS with tracking. International shipping is calculated at checkout. We ship to: "
        + countries + ".</div>"
        "</body></html>"
    )
    win = policies._shipping_from_full_page(html)
    assert win is not None
    assert "8.95" in win and "flat rate" in win.lower()
    assert "business days" in win.lower()
