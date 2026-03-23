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


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def load_store_seeds(csv_path: str, settings: Settings) -> list[StoreSeed]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"TSV file not found: {csv_path}")

    seeds: list[StoreSeed] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        if settings.input_csv_has_header:
            reader = csv.reader(handle, delimiter="\t")
            header = next(reader, None)
            if header is None:
                return seeds

            header_map = {value.strip().lower(): idx for idx, value in enumerate(header)}
            url_idx = header_map.get("url", 0)
            store_name_idx = header_map.get("store name", 1)
            store_address_idx = header_map.get("store address", 2)

            for row_values in reader:
                if not row_values:
                    continue
                if len(row_values) <= url_idx:
                    continue

                store_url = _normalize_url(row_values[url_idx])
                seeds.append(
                    StoreSeed(
                        store_url=store_url,
                        store_name=_clean_optional(
                            row_values[store_name_idx]
                            if store_name_idx < len(row_values)
                            else None
                        ),
                        address=_clean_optional(
                            row_values[store_address_idx]
                            if store_address_idx < len(row_values)
                            else None
                        ),
                    )
                )
        else:
            list_reader = csv.reader(handle, delimiter="\t")
            for row_values in list_reader:
                if not row_values:
                    continue

                store_url = _normalize_url(row_values[0])
                store_name = _clean_optional(row_values[1]) if len(row_values) > 1 else None
                store_address = _clean_optional(row_values[2]) if len(row_values) > 2 else None
                seeds.append(
                    StoreSeed(
                        store_url=store_url,
                        store_name=store_name,
                        address=store_address,
                    )
                )

    return seeds
