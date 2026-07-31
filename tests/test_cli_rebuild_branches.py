from aisley_scraper import cli
from aisley_scraper.config import Settings
from aisley_scraper.models import StoreProfile, StoreSeed


def _settings() -> Settings:
    return Settings(
        LOG_LEVEL="INFO",
        USER_AGENT="aisley-test-agent",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        PERSISTENCE_TARGET="supabase",
    )


def _install_fakes(monkeypatch, seeds, profiles, captured):
    settings = _settings()
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_setup_logging", lambda *_a, **_k: None)
    monkeypatch.setattr(cli, "geocode_address", lambda *_a, **_k: (1.0, 2.0))
    monkeypatch.setattr(cli, "load_store_seeds_from_dir", lambda *_a, **_k: seeds)

    class _FakeRepo:
        def __init__(self, *_a, **_k) -> None:
            pass

        def ensure_schema(self) -> None:
            pass

        def list_all_store_profiles(self):
            return profiles

        def sync_store_branches(self, website, branches):
            captured.append((website, [b.address for b in branches],
                             [(b.lat, b.long) for b in branches]))
            return list(range(100, 100 + len(branches)))

        def upsert_store(self, store):
            captured.append(("upsert", store.website, store.address))
            return 1

    monkeypatch.setattr(cli, "Repository", _FakeRepo)


def test_rebuild_branches_fans_out_all_addresses_for_existing_store(monkeypatch) -> None:
    seeds = [
        StoreSeed(store_url="https://corridornyc.com/", store_name="Corridor",
                  addresses=["A St", "B St", "C St"]),
        StoreSeed(store_url="https://notindb.com/", store_name="NotInDB",
                  addresses=["X St", "Y St"]),
        StoreSeed(store_url="https://noaddr.com/", store_name="NoAddr", addresses=[]),
    ]
    # Existing store row under a different scheme/www variant of the same domain.
    profiles = [
        StoreProfile(store_name="Corridor", website="http://www.corridornyc.com",
                     store_type="online", address="A St"),
    ]
    captured: list = []
    _install_fakes(monkeypatch, seeds, profiles, captured)

    rc = cli.run_rebuild_branches()
    assert rc == 0

    # Only corridornyc has BOTH TSV addresses and an existing row -> reconciled.
    # notindb.com has no existing row; noaddr.com has no TSV addresses -> skipped.
    assert len(captured) == 1
    website, addresses, coords = captured[0]
    assert website == "http://www.corridornyc.com"  # matched across scheme/www by domain
    assert addresses == ["A St", "B St", "C St"]     # ALL branches, not just the first
    assert coords == [(1.0, 2.0), (1.0, 2.0), (1.0, 2.0)]  # geocoded


def test_rebuild_branches_include_missing_creates_missing_and_addressless(monkeypatch) -> None:
    seeds = [
        StoreSeed(store_url="https://corridornyc.com/", store_name="Corridor",
                  addresses=["A St", "B St"]),
        StoreSeed(store_url="https://notindb.com/", store_name="NotInDB",
                  addresses=["X St", "Y St"]),
        StoreSeed(store_url="https://onlineonly.com/", store_name="Online", addresses=[]),
    ]
    profiles = [
        StoreProfile(store_name="Corridor", website="https://corridornyc.com",
                     store_type="online", address="A St"),
    ]
    captured: list = []
    _install_fakes(monkeypatch, seeds, profiles, captured)

    rc = cli.run_rebuild_branches(include_missing=True)
    assert rc == 0

    synced_sites = [c[0] for c in captured if c[0] != "upsert"]
    upserts = [c for c in captured if c[0] == "upsert"]
    # Existing (corridornyc) AND missing-with-addresses (notindb) are both synced.
    assert any("corridornyc" in w for w in synced_sites)
    assert any("notindb" in w for w in synced_sites)
    # Address-less missing store gets a single upserted row.
    assert any("onlineonly" in c[1] for c in upserts)


def test_rebuild_branches_default_skips_missing_stores(monkeypatch) -> None:
    seeds = [
        StoreSeed(store_url="https://notindb.com/", store_name="NotInDB",
                  addresses=["X St"]),
        StoreSeed(store_url="https://onlineonly.com/", store_name="Online", addresses=[]),
    ]
    profiles: list = []  # nothing in production
    captured: list = []
    _install_fakes(monkeypatch, seeds, profiles, captured)

    rc = cli.run_rebuild_branches()  # default (include_missing=False)
    assert rc == 0
    assert captured == []  # nothing created/reconciled without --include-missing


def test_rebuild_branches_skip_geocode_leaves_coords_null(monkeypatch) -> None:
    seeds = [StoreSeed(store_url="https://corridornyc.com/", store_name="Corridor",
                       addresses=["A St", "B St"])]
    profiles = [StoreProfile(store_name="Corridor", website="https://corridornyc.com",
                             store_type="online", address="A St")]
    captured: list = []
    _install_fakes(monkeypatch, seeds, profiles, captured)

    rc = cli.run_rebuild_branches(skip_geocode=True)
    assert rc == 0
    _w, _addrs, coords = captured[0]
    assert coords == [(None, None), (None, None)]
