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
