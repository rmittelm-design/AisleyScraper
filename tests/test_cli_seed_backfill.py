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
        INPUT_CSV_PATH="./data/stores.tsv",
        PERSISTENCE_TARGET="supabase",
    )


def test_build_db_first_seeds_backfills_existing_missing_address_and_keeps_domain_dedupe(monkeypatch) -> None:
    settings = _settings()

    csv_seeds = [
        StoreSeed(
            store_url="https://existing.com",
            store_name="Existing TSV",
            address="123 Main St, New York, NY",
        ),
        StoreSeed(store_url="https://newstore.com", store_name="New TSV", address=None),
    ]

    existing_profile = StoreProfile(
        store_name="Existing DB",
        website="https://existing.com",
        store_type="online",
        address=None,
    )

    class _FakeRepo:
        upserts: list[StoreProfile] = []

        def list_all_store_profiles(self) -> list[StoreProfile]:
            return [existing_profile]

        def list_all_store_websites(self) -> list[str]:
            return ["https://existing.com"]

        def upsert_store(self, store: StoreProfile) -> int:
            self.upserts.append(store)
            return 1

    monkeypatch.setattr(cli, "load_store_seeds", lambda _path, _settings: csv_seeds)
    monkeypatch.setattr(cli, "geocode_address", lambda *_args, **_kwargs: (40.7128, -74.0060))

    repo = _FakeRepo()
    seeds = cli._build_db_first_seeds(settings, repo)

    # Existing domain is sourced from DB seed only; TSV duplicate is excluded.
    assert [seed.store_url for seed in seeds] == [
        "https://existing.com",
        "https://newstore.com",
    ]

    # Existing row got address + coordinates backfilled.
    assert len(repo.upserts) == 1
    assert repo.upserts[0].address == "123 Main St, New York, NY"
    assert repo.upserts[0].lat == 40.7128
    assert repo.upserts[0].long == -74.006
