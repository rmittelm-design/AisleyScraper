from aisley_scraper.cli import _dedupe_seeds_by_domain
from aisley_scraper.models import StoreSeed


def test_dedupe_seeds_by_domain_keeps_first_per_domain() -> None:
    seeds = [
        StoreSeed(store_url="https://example.com", source_id="a"),
        StoreSeed(store_url="https://example.com", source_id="b"),
        StoreSeed(store_url="https://shop.example.org", source_id="c"),
        StoreSeed(store_url="https://SHOP.EXAMPLE.ORG", source_id="d"),
        StoreSeed(store_url="https://another.com", source_id="e"),
    ]

    deduped = _dedupe_seeds_by_domain(seeds)

    assert [s.store_url for s in deduped] == [
        "https://example.com",
        "https://shop.example.org",
        "https://another.com",
    ]
    assert [s.source_id for s in deduped] == ["a", "c", "e"]
