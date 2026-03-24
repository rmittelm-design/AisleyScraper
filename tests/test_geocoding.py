from aisley_scraper import geocoding
from geopy.exc import GeopyError


class _FakeLocation:
    def __init__(self, latitude: float, longitude: float) -> None:
        self.latitude = latitude
        self.longitude = longitude


def test_geocode_address_returns_lat_long(monkeypatch) -> None:
    class _FakeNominatim:
        def __init__(self, *, user_agent: str) -> None:
            assert user_agent == "aisley-test-agent"

        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            assert query == "1600 Amphitheatre Parkway, Mountain View, CA"
            assert exactly_one is True
            assert timeout == 9.0
            assert kwargs["country_codes"] == ["us"]
            return _FakeLocation(37.422, -122.084)

    monkeypatch.setattr(geocoding, "Nominatim", _FakeNominatim)

    coords = geocoding.geocode_address(
        "1600 Amphitheatre Parkway, Mountain View, CA",
        user_agent="aisley-test-agent",
        timeout_sec=9.0,
        country_codes=["US"],
    )

    assert coords == (37.422, -122.084)


def test_geocode_address_returns_none_when_not_found(monkeypatch) -> None:
    class _FakeNominatim:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            return None

    class _FakePhoton:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            return None

    class _FakeArcGIS:
        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            return None

    monkeypatch.setattr(geocoding, "Nominatim", _FakeNominatim)
    monkeypatch.setattr(geocoding, "Photon", _FakePhoton)
    monkeypatch.setattr(geocoding, "ArcGIS", lambda: _FakeArcGIS())

    coords = geocoding.geocode_address("Unknown Place", user_agent="aisley-test-agent")
    assert coords is None


def test_geocode_address_falls_back_when_primary_provider_errors(monkeypatch) -> None:
    class _FailingNominatim:
        def __init__(self, *, user_agent: str) -> None:
            assert user_agent == "aisley-test-agent"

        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            raise GeopyError("blocked")

    class _EmptyPhoton:
        def __init__(self, *, user_agent: str) -> None:
            assert user_agent == "aisley-test-agent"

        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            return None

    class _WorkingArcGIS:
        def geocode(self, query: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (query, exactly_one, timeout, kwargs)
            return _FakeLocation(37.422, -122.084)

    monkeypatch.setattr(geocoding, "Nominatim", _FailingNominatim)
    monkeypatch.setattr(geocoding, "Photon", _EmptyPhoton)
    monkeypatch.setattr(geocoding, "ArcGIS", lambda: _WorkingArcGIS())

    coords = geocoding.geocode_address(
        "1600 Amphitheatre Parkway, Mountain View, CA",
        user_agent="aisley-test-agent",
    )

    assert coords == (37.422, -122.084)
