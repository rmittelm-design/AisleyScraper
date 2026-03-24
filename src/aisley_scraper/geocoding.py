from __future__ import annotations

from threading import Lock
import time

from geopy.exc import GeopyError
from geopy.geocoders import ArcGIS, Nominatim, Photon


_GEOCODE_LOCK = Lock()
_LAST_GEOCODE_AT = 0.0
_MIN_GEOCODE_INTERVAL_SEC = 1.1


def _apply_geocode_rate_limit() -> None:
    global _LAST_GEOCODE_AT

    elapsed = time.monotonic() - _LAST_GEOCODE_AT
    if elapsed < _MIN_GEOCODE_INTERVAL_SEC:
        time.sleep(_MIN_GEOCODE_INTERVAL_SEC - elapsed)
    _LAST_GEOCODE_AT = time.monotonic()


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

            return float(location.latitude), float(location.longitude)

    return None
