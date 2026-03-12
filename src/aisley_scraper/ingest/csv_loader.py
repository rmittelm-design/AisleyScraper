from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse

from aisley_scraper.config import Settings
from aisley_scraper.models import StoreSeed


def _normalize_url(url: str) -> str:
    cleaned = url.strip()
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    if not parsed.netloc:
        raise ValueError(f"invalid URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def load_store_seeds(csv_path: str, settings: Settings) -> list[StoreSeed]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    seeds: list[StoreSeed] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        if settings.input_csv_has_header:
            dict_reader = csv.DictReader(handle)
            for row in dict_reader:
                if row is None:
                    continue
                store_url = _normalize_url(row.get(settings.input_csv_url_column, ""))
                seeds.append(
                    StoreSeed(
                        store_url=store_url,
                        source_id=row.get(settings.input_csv_source_id_column) or None,
                        notes=row.get(settings.input_csv_notes_column) or None,
                    )
                )
        else:
            list_reader = csv.reader(handle)
            for row_values in list_reader:
                if not row_values:
                    continue
                seeds.append(StoreSeed(store_url=_normalize_url(row_values[0])))

    return seeds
