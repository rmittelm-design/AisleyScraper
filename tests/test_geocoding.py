from aisley_scraper import geocoding


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

    monkeypatch.setattr(geocoding, "Nominatim", _FakeNominatim)

    coords = geocoding.geocode_address("Unknown Place", user_agent="aisley-test-agent")
    assert coords is None
