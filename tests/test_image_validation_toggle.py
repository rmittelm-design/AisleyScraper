from aisley_scraper import image_validation
import aisley_scraper.config as config


class _SettingsStub:
    def __init__(self, enabled: bool) -> None:
        self.image_validation_use_gcloud_vision = enabled


def test_gcv_toggle_uses_settings_when_env_not_exported(monkeypatch) -> None:
    monkeypatch.delenv("IMAGE_VALIDATION_USE_GCLOUD_VISION", raising=False)
    monkeypatch.setattr(config, "get_settings", lambda: _SettingsStub(False))

    assert image_validation._gcv_nsfw_check_enabled() is False


def test_gcv_toggle_env_overrides_settings(monkeypatch) -> None:
    monkeypatch.setenv("IMAGE_VALIDATION_USE_GCLOUD_VISION", "true")
    monkeypatch.setattr(config, "get_settings", lambda: _SettingsStub(False))

    assert image_validation._gcv_nsfw_check_enabled() is True
