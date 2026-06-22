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


def test_finds_shipping_only_when_returns_missing():
    base = "https://shop.example.com"
    fetcher = _FakeFetcher({f"{base}/policies/shipping-policy": _SHIP_HTML})
    text, url = asyncio.run(
        policies.fetch_shipping_returns(base, fetcher, _settings())
    )
    assert text is not None
    assert "SHIPPING:" in text and "RETURNS:" not in text
    assert url == f"{base}/policies/shipping-policy"
