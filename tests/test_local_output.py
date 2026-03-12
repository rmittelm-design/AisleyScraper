from pathlib import Path

from aisley_scraper.local_output import write_local_results
from aisley_scraper.models import ProductRecord, ScrapeResult, StoreProfile, StoreSeed


def test_write_local_results_creates_json(tmp_path: Path) -> None:
    output = tmp_path / "results.json"
    seed = StoreSeed(store_url="https://example.com")
    result = ScrapeResult(
        store=StoreProfile(
            store_name="Example",
            website="https://example.com",
            store_type="online",
            instagram_handle="example",
            address=None,
        ),
        products=[
            ProductRecord(
                product_id="1",
                product_handle="item",
                item_name="Item",
                description=None,
                images=["https://cdn.example.com/a.jpg"],
                updated_at="2026-01-01T12:00:00Z",
                position=3,
            )
        ],
    )

    success, fail = write_local_results(str(output), [(seed, result)])
    assert success == 1
    assert fail == 0
    assert output.exists()
    payload = output.read_text(encoding="utf-8")
    assert "https://cdn.example.com/a.jpg" in payload
    assert '"product_id": "1"' in payload
    assert '"updated_at": "2026-01-01T12:00:00Z"' in payload
    assert '"position": 3' in payload
