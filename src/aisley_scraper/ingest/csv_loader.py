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


def _extract_addresses(row_values: list[str], start_idx: int) -> list[str]:
    """Collect every non-empty branch address from start_idx onward, in order."""
    addresses: list[str] = []
    seen: set[str] = set()
    for value in row_values[start_idx:]:
        address = _clean_optional(value)
        if address is None:
            continue
        key = address.lower()
        if key in seen:
            continue
        seen.add(key)
        addresses.append(address)
    return addresses


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

    # Skip blank/padding rows (e.g. trailing tab-only lines in exported TSVs).
    if not any(_clean_optional(value) for value in row_values):
        return None

    first = _clean_optional(row_values[0])
    second = _clean_optional(row_values[1]) if len(row_values) > 1 else None
    addresses = _extract_addresses(row_values, 2)

    if first is not None:
        try:
            return StoreSeed(
                store_url=_normalize_url(first),
                store_name=second,
                addresses=addresses,
            )
        except ValueError:
            pass

    if second is not None:
        try:
            return StoreSeed(
                store_url=_normalize_url(second),
                store_name=first,
                addresses=addresses,
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
                if not _clean_optional(row_values[url_idx]):
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
                        addresses=_extract_addresses(row_values, store_address_idx),
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


def _domain_key(store_url: str) -> str:
    """Scheme- and www-insensitive domain key (e.g. http://www.x.com -> x.com)."""
    netloc = urlparse(store_url).netloc.strip().lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def dedupe_seeds_by_domain(seeds: list[StoreSeed]) -> list[StoreSeed]:
    """Keep one seed per domain (ignoring scheme and a leading ``www.``).

    When several rows share a domain, keep the one with the MOST branch
    addresses (ties keep the first seen). Output preserves first-seen domain
    order. The winning row is taken as-is (its url, name, and addresses) — rows
    for the same domain are not merged.
    """
    best: dict[str, StoreSeed] = {}
    order: list[str] = []
    for seed in seeds:
        key = _domain_key(seed.store_url)
        if not key:
            continue
        existing = best.get(key)
        if existing is None:
            order.append(key)
            best[key] = seed
        elif len(seed.addresses) > len(existing.addresses):
            best[key] = seed
    return [best[key] for key in order]


def load_store_seeds_from_dir(dir_path: str, settings: Settings) -> list[StoreSeed]:
    """Parse every ``*.tsv`` file in ``dir_path`` and dedupe by domain.

    This is the primary seed source: all store TSV files live in one folder
    (``INPUT_TSV_DIR``). Across every file, store URLs are deduped by domain
    (scheme- and ``www.``-insensitive); when a domain appears more than once the
    row with the most branch addresses wins.
    """
    directory = Path(dir_path)
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"TSV directory not found: {dir_path}")

    all_seeds: list[StoreSeed] = []
    for tsv_path in sorted(directory.glob("*.tsv")):
        if not tsv_path.is_file():
            continue
        all_seeds.extend(load_store_seeds(str(tsv_path), settings))

    return dedupe_seeds_by_domain(all_seeds)
