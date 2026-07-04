from __future__ import annotations

import re
from threading import Lock
import time

from geopy.exc import GeopyError
from geopy.geocoders import ArcGIS, Nominatim, Photon


_GEOCODE_LOCK = Lock()
_LAST_GEOCODE_AT = 0.0
_MIN_GEOCODE_INTERVAL_SEC = 1.1

# Score awarded when a provider's result echoes the queried ZIP — a strong
# signal the address was parsed correctly, enough to trust the result outright.
_ZIP_MATCH_SCORE = 2

_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
_LEADING_NUMBER_RE = re.compile(r"^\s*(\d+)\b")


def _apply_geocode_rate_limit() -> None:
    global _LAST_GEOCODE_AT

    elapsed = time.monotonic() - _LAST_GEOCODE_AT
    if elapsed < _MIN_GEOCODE_INTERVAL_SEC:
        time.sleep(_MIN_GEOCODE_INTERVAL_SEC - elapsed)
    _LAST_GEOCODE_AT = time.monotonic()


def _expected_zip(address: str) -> str | None:
    """The 5-digit ZIP embedded in a query address, if any."""
    match = _ZIP_RE.search(address)
    return match.group(1) if match else None


def _leading_house_number(address: str) -> str | None:
    """The leading street number of a query address, if it starts with one."""
    match = _LEADING_NUMBER_RE.match(address)
    return match.group(1) if match else None


def _result_score(*, expected_zip: str | None, house_number: str | None, result_text: str) -> int:
    """How well a geocoder result matches the queried address.

    ZIP agreement dominates (a mismatched ZIP is how a low street number gets
    mis-read as a numbered cross-street, e.g. "42 5th Avenue" -> "5th Ave & West
    42nd St" in a different ZIP). The house-number check requires a standalone
    number so an ordinal like "42nd" never counts as the street number "42".
    """
    text = result_text or ""
    score = 0
    if expected_zip and re.search(rf"\b{re.escape(expected_zip)}\b", text):
        score += _ZIP_MATCH_SCORE
    if house_number and re.search(
        rf"(?<!\d){re.escape(house_number)}(?!\d|(?:st|nd|rd|th)\b)", text
    ):
        score += 1
    return score


def geocode_address(
    address: str,
    *,
    user_agent: str,
    timeout_sec: float = 5.0,
    country_codes: list[str] | None = None,
) -> tuple[float, float] | None:
    cleaned = address.strip()
    if not cleaned:
        return None

    kwargs: dict[str, object] = {}
    normalized_codes = [code.strip().lower() for code in (country_codes or []) if code.strip()]
    if normalized_codes:
        kwargs["country_codes"] = normalized_codes

    providers: list[tuple[object, dict[str, object]]] = [
        (Nominatim(user_agent=user_agent), kwargs),
        (Photon(user_agent=user_agent), {}),
        (ArcGIS(), {}),
    ]

    expected_zip = _expected_zip(cleaned)
    house_number = _leading_house_number(cleaned)

    # When the query carries no ZIP we have no way to tell a good parse from a
    # plausible-but-wrong one, so keep the original "first hit wins" behavior
    # (and its single-lookup speed). With a ZIP we validate each result and
    # prefer the provider whose answer actually matches the queried address.
    best_coords: tuple[float, float] | None = None
    best_score = -1

    with _GEOCODE_LOCK:
        for geocoder, provider_kwargs in providers:
            _apply_geocode_rate_limit()
            try:
                location = geocoder.geocode(
                    cleaned,
                    exactly_one=True,
                    timeout=timeout_sec,
                    **provider_kwargs,
                )
            except GeopyError:
                continue

            if location is None or location.latitude is None or location.longitude is None:
                continue

            coords = (float(location.latitude), float(location.longitude))

            if expected_zip is None:
                return coords

            score = _result_score(
                expected_zip=expected_zip,
                house_number=house_number,
                result_text=getattr(location, "address", "") or "",
            )
            # A ZIP-matching result is trusted immediately — no extra lookups
            # when the first provider is already right.
            if score >= _ZIP_MATCH_SCORE:
                return coords
            if score > best_score:
                best_score = score
                best_coords = coords

    return best_coords
