from __future__ import annotations

import asyncio
import pytest

from aisley_scraper.gender_probs import GenderProbComputationError, enrich_gender_probabilities_for_products
from aisley_scraper.models import ProductRecord


class _DummyFetcher:
    pass


def test_enrich_gender_probs_leaves_none_when_all_clip_scores_fail(monkeypatch) -> None:
    async def _always_fail_score(_fetcher: object, _image_url: str):
        return None

    monkeypatch.setattr("aisley_scraper.gender_probs._score_image_url", _always_fail_score)

    products = [
        ProductRecord(
            product_id="p1",
            product_handle="p1",
            item_name="Item",
            description=None,
            images=["https://cdn.example.com/a.jpg"],
            gender_label=None,
            gender_probs_csv=None,
        )
    ]

    with pytest.raises(GenderProbComputationError):
        asyncio.run(
            enrich_gender_probabilities_for_products(
                products=products,
                fetcher=_DummyFetcher(),
                concurrency=1,
            )
        )

    assert products[0].gender_probs_csv is None
