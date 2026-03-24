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
    if not parsed.netloc or any(char.isspace() for char in parsed.netloc):
        raise ValueError(f"invalid URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _extract_address(row_values: list[str], store_address_idx: int) -> str | None:
    if store_address_idx < len(row_values):
        address = _clean_optional(row_values[store_address_idx])
        if address is not None:
            return address

    # Recover malformed rows where an extra empty tab shifts the address one cell right.
    shifted_idx = store_address_idx + 1
    if shifted_idx < len(row_values):
        return _clean_optional(row_values[shifted_idx])

    return None


def _looks_like_header(row_values: list[str]) -> bool:
    normalized = {value.strip().lower() for value in row_values if value.strip()}
    return bool(
        normalized
        & {
            "url",
            "store url",
            "store name",
            "name",
            "store address",
            "address",
            "source_id",
            "notes",
        }
    )


def _parse_headerless_row(row_values: list[str]) -> StoreSeed | None:
    if not row_values:
        return None

    first = _clean_optional(row_values[0])
    second = _clean_optional(row_values[1]) if len(row_values) > 1 else None
    third = _clean_optional(row_values[2]) if len(row_values) > 2 else None
    fourth = _clean_optional(row_values[3]) if len(row_values) > 3 else None
    address = third or fourth

    if first is not None:
        try:
            return StoreSeed(
                store_url=_normalize_url(first),
                store_name=second,
                address=address,
            )
        except ValueError:
            pass

    if second is not None:
        try:
            return StoreSeed(
                store_url=_normalize_url(second),
                store_name=first,
                address=address,
            )
        except ValueError:
            pass

    raise ValueError(f"invalid TSV row: {row_values}")


def load_store_seeds(csv_path: str, settings: Settings) -> list[StoreSeed]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"TSV file not found: {csv_path}")

    seeds: list[StoreSeed] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        if settings.input_csv_has_header:
            reader = csv.reader(handle, delimiter="\t")
            first_row = next(reader, None)
            if first_row is None:
                return seeds

            if not _looks_like_header(first_row):
                parsed = _parse_headerless_row(first_row)
                if parsed is not None:
                    seeds.append(parsed)
                for row_values in reader:
                    if not row_values:
                        continue
                    parsed = _parse_headerless_row(row_values)
                    if parsed is not None:
                        seeds.append(parsed)
                return seeds

            header = first_row

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
                        address=_extract_address(row_values, store_address_idx),
                    )
                )
        else:
            list_reader = csv.reader(handle, delimiter="\t")
            for row_values in list_reader:
                if not row_values:
                    continue
                parsed = _parse_headerless_row(row_values)
                if parsed is not None:
                    seeds.append(parsed)

    return seeds
