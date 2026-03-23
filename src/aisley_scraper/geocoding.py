from __future__ import annotations

from geopy.exc import GeopyError
from geopy.geocoders import Nominatim


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

    geocoder = Nominatim(user_agent=user_agent)

    kwargs: dict[str, object] = {}
    normalized_codes = [code.strip().lower() for code in (country_codes or []) if code.strip()]
    if normalized_codes:
        kwargs["country_codes"] = normalized_codes

    try:
        location = geocoder.geocode(
            cleaned,
            exactly_one=True,
            timeout=timeout_sec,
            **kwargs,
        )
    except GeopyError:
        return None

    if location is None or location.latitude is None or location.longitude is None:
        return None

    return float(location.latitude), float(location.longitude)
