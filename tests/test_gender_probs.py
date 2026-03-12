import asyncio

from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products, one_hot_gender_probs_csv
from aisley_scraper.models import ProductRecord


class _FakeFetcher:
    def __init__(self, payload_by_url: dict[str, bytes]) -> None:
        self._payload_by_url = payload_by_url
        self.calls: list[str] = []

    async def get_bytes(self, url: str) -> bytes:
        self.calls.append(url)
        return self._payload_by_url[url]


def test_one_hot_gender_probs_csv_mapping() -> None:
    assert one_hot_gender_probs_csv("male") == "1.0,0,0"
    assert one_hot_gender_probs_csv("female") == "0,1.0,0"
    assert one_hot_gender_probs_csv("unisex") == "0,0,1.0"
    assert one_hot_gender_probs_csv("unknown") is None
    assert one_hot_gender_probs_csv(None) is None


def test_enrich_gender_probs_keeps_explicit_label_one_hot() -> None:
    product = ProductRecord(
        product_id="p1",
        product_handle="shirt",
        item_name="Shirt",
        description=None,
        images=["https://cdn.example.com/p1.jpg"],
        gender_label="male",
    )
    fetcher = _FakeFetcher({"https://cdn.example.com/p1.jpg": b"img"})

    asyncio.run(
        enrich_gender_probabilities_for_products(products=[product], fetcher=fetcher, concurrency=2)
    )

    assert product.gender_probs_csv == "1.0,0,0"
    assert fetcher.calls == []


def test_enrich_gender_probs_averages_scored_validated_images(monkeypatch) -> None:
    product = ProductRecord(
        product_id="p2",
        product_handle="look",
        item_name="Look",
        description=None,
        images=["https://cdn.example.com/a.jpg", "https://cdn.example.com/b.jpg"],
        gender_label=None,
    )

    fake_scores = {
        b"A": (0.7, 0.2, 0.1),
        b"B": (0.1, 0.7, 0.2),
    }

    def _fake_score_image_bytes_with_clip(image_bytes: bytes):
        return fake_scores[image_bytes]

    monkeypatch.setattr(
        "aisley_scraper.gender_probs._score_image_bytes_with_clip",
        _fake_score_image_bytes_with_clip,
    )

    fetcher = _FakeFetcher(
        {
            "https://cdn.example.com/a.jpg": b"A",
            "https://cdn.example.com/b.jpg": b"B",
        }
    )

    asyncio.run(
        enrich_gender_probabilities_for_products(products=[product], fetcher=fetcher, concurrency=2)
    )

    assert product.gender_probs_csv == "0.4,0.45,0.15"


def test_enrich_gender_probs_uses_only_successful_scored_images(monkeypatch) -> None:
    product = ProductRecord(
        product_id="p3",
        product_handle="mixed",
        item_name="Mixed",
        description=None,
        images=["https://cdn.example.com/a.jpg", "https://cdn.example.com/b.jpg"],
        gender_label=None,
    )

    def _fake_score_image_bytes_with_clip(image_bytes: bytes):
        if image_bytes == b"A":
            return (0.0, 1.0, 0.0)
        return None

    monkeypatch.setattr(
        "aisley_scraper.gender_probs._score_image_bytes_with_clip",
        _fake_score_image_bytes_with_clip,
    )

    fetcher = _FakeFetcher(
        {
            "https://cdn.example.com/a.jpg": b"A",
            "https://cdn.example.com/b.jpg": b"B",
        }
    )

    asyncio.run(
        enrich_gender_probabilities_for_products(products=[product], fetcher=fetcher, concurrency=2)
    )

    assert product.gender_probs_csv == "0,1,0"
