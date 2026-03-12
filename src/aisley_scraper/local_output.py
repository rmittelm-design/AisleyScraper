from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from aisley_scraper.models import ScrapeResult, StoreSeed


def write_local_results(
    output_path: str, results: list[tuple[StoreSeed, ScrapeResult | Exception]]
) -> tuple[int, int]:
    rows: list[dict[str, object]] = []
    success_count = 0
    fail_count = 0

    for seed, outcome in results:
        if isinstance(outcome, Exception):
            fail_count += 1
            rows.append(
                {
                    "store_url": seed.store_url,
                    "status": "failed",
                    "error": str(outcome),
                }
            )
            continue

        success_count += 1
        rows.append(
            {
                "store_url": seed.store_url,
                "status": "ok",
                "store": asdict(outcome.store),
                "products": [asdict(p) for p in outcome.products],
            }
        )

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")
    return success_count, fail_count
