from aisley_scraper import geocoding
from geopy.exc import GeopyError


class _FakeLocation:
    def __init__(self, latitude: float, longitude: float, address: str = "") -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.address = address


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


def test_geocode_address_rejects_zip_mismatch_prefers_matching_provider(monkeypatch) -> None:
    """The real "42 5th Avenue" bug: Photon mis-parses it as "5th Ave & West
    42nd St" in ZIP 10036; ArcGIS resolves it correctly in ZIP 10011. The ZIP
    mismatch must make the correct ArcGIS result win despite Photon answering
    first among the two."""
    query = "42 5th Avenue, New York, New York 10011"

    class _EmptyNominatim:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return None  # 403 / no result in practice

    class _WrongPhoton:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(
                40.753743,
                -73.981900,
                address="5th Avenue, West 42nd Street, 10036, New York, New York, United States",
            )

    class _RightArcGIS:
        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(
                40.733894,
                -73.995391,
                address="42 5th Ave, New York, New York, 10011",
            )

    monkeypatch.setattr(geocoding, "Nominatim", _EmptyNominatim)
    monkeypatch.setattr(geocoding, "Photon", _WrongPhoton)
    monkeypatch.setattr(geocoding, "ArcGIS", lambda: _RightArcGIS())

    coords = geocoding.geocode_address(query, user_agent="aisley-test-agent")
    assert coords == (40.733894, -73.995391)


def test_geocode_address_returns_first_zip_match_without_extra_lookups(monkeypatch) -> None:
    """A first provider whose result matches the queried ZIP is trusted at once;
    later providers must not even be consulted."""
    later_called = {"photon": False, "arcgis": False}

    class _MatchingNominatim:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(40.733894, -73.995391, address="42 5th Ave, New York, 10011")

    class _Photon:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            later_called["photon"] = True
            return _FakeLocation(0.0, 0.0)

    class _ArcGIS:
        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            later_called["arcgis"] = True
            return _FakeLocation(0.0, 0.0)

    monkeypatch.setattr(geocoding, "Nominatim", _MatchingNominatim)
    monkeypatch.setattr(geocoding, "Photon", _Photon)
    monkeypatch.setattr(geocoding, "ArcGIS", lambda: _ArcGIS())

    coords = geocoding.geocode_address(
        "42 5th Avenue, New York, New York 10011", user_agent="aisley-test-agent"
    )
    assert coords == (40.733894, -73.995391)
    assert later_called == {"photon": False, "arcgis": False}


def test_geocode_address_falls_back_to_best_when_no_zip_matches(monkeypatch) -> None:
    """When no provider echoes the ZIP, a result that at least echoes the street
    number (and isn't an ordinal false-positive) is preferred over one that
    matches nothing."""

    class _EmptyNominatim:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(1.0, 1.0, address="somewhere near West 42nd Street")

    class _HouseNumPhoton:
        def __init__(self, *, user_agent: str) -> None:
            _ = user_agent

        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(2.0, 2.0, address="42 5th Ave, New York")

    class _EmptyArcGIS:
        def geocode(self, q: str, exactly_one: bool, timeout: float, **kwargs):
            _ = (q, exactly_one, timeout, kwargs)
            return _FakeLocation(3.0, 3.0, address="unrelated place")

    monkeypatch.setattr(geocoding, "Nominatim", _EmptyNominatim)
    monkeypatch.setattr(geocoding, "Photon", _HouseNumPhoton)
    monkeypatch.setattr(geocoding, "ArcGIS", lambda: _EmptyArcGIS())

    coords = geocoding.geocode_address(
        "42 5th Avenue, New York, New York 10011", user_agent="aisley-test-agent"
    )
    # Nominatim's "42nd Street" must NOT count as street number 42; Photon's real
    # "42 5th Ave" does, so it wins.
    assert coords == (2.0, 2.0)
