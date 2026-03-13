from __future__ import annotations

from functools import lru_cache
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    user_agent: str = Field(default="", alias="USER_AGENT")

    supabase_url: str = Field(alias="SUPABASE_URL")
    supabase_service_role_key: str = Field(alias="SUPABASE_SERVICE_ROLE_KEY")
    supabase_schema: str = Field(default="public", alias="SUPABASE_SCHEMA")
    supabase_storage_bucket: str = Field(alias="SUPABASE_STORAGE_BUCKET")
    supabase_storage_path: str = Field(alias="SUPABASE_STORAGE_PATH")

    persistence_target: str = Field(default="supabase", alias="PERSISTENCE_TARGET")
    local_output_path: str = Field(default="./out/scrape_results.json", alias="LOCAL_OUTPUT_PATH")

    input_csv_path: str = Field(alias="INPUT_CSV_PATH")
    input_csv_has_header: bool = Field(default=True, alias="INPUT_CSV_HAS_HEADER")
    input_csv_url_column: str = Field(default="store_url", alias="INPUT_CSV_URL_COLUMN")
    input_csv_source_id_column: str = Field(default="source_id", alias="INPUT_CSV_SOURCE_ID_COLUMN")
    input_csv_notes_column: str = Field(default="notes", alias="INPUT_CSV_NOTES_COLUMN")

    crawl_max_stores_per_run: int = Field(default=1000, alias="CRAWL_MAX_STORES_PER_RUN")
    crawl_global_concurrency: int = Field(default=15, alias="CRAWL_GLOBAL_CONCURRENCY")
    crawl_per_domain_concurrency: int = Field(default=2, alias="CRAWL_PER_DOMAIN_CONCURRENCY")
    crawl_global_qps: int = Field(default=8, alias="CRAWL_GLOBAL_QPS")
    crawl_request_timeout_sec: int = Field(default=25, alias="CRAWL_REQUEST_TIMEOUT_SEC")
    crawl_connect_timeout_sec: int = Field(default=10, alias="CRAWL_CONNECT_TIMEOUT_SEC")
    crawl_max_retries: int = Field(default=4, alias="CRAWL_MAX_RETRIES")
    crawl_backoff_base_ms: int = Field(default=500, alias="CRAWL_BACKOFF_BASE_MS")
    crawl_backoff_max_ms: int = Field(default=8000, alias="CRAWL_BACKOFF_MAX_MS")
    crawl_jitter_ms: int = Field(default=250, alias="CRAWL_JITTER_MS")
    crawl_respect_robots: bool = Field(default=True, alias="CRAWL_RESPECT_ROBOTS")

    shopify_products_page_limit: int = Field(default=250, alias="SHOPIFY_PRODUCTS_PAGE_LIMIT")
    shopify_products_max_pages: int = Field(default=100, alias="SHOPIFY_PRODUCTS_MAX_PAGES")

    image_validation_enabled: bool = Field(default=True, alias="IMAGE_VALIDATION_ENABLED")
    image_validation_use_gcloud_vision: bool = Field(
        default=True,
        alias="IMAGE_VALIDATION_USE_GCLOUD_VISION",
    )
    image_validation_concurrency: int = Field(default=4, alias="IMAGE_VALIDATION_CONCURRENCY")
    image_validation_max_retries: int = Field(default=2, alias="IMAGE_VALIDATION_MAX_RETRIES")

    crawl_run_state_path: str = Field(default=".aisley_active_run_id", alias="CRAWL_RUN_STATE_PATH")

    classify_require_ecom_signal: bool = Field(default=True, alias="CLASSIFY_REQUIRE_ECOM_SIGNAL")

    @field_validator(
        "crawl_global_concurrency",
        "crawl_per_domain_concurrency",
        "crawl_global_qps",
        "image_validation_concurrency",
    )
    @classmethod
    def positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be > 0")
        return value

    @field_validator("image_validation_max_retries")
    @classmethod
    def non_negative_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("persistence_target")
    @classmethod
    def valid_persistence_target(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"supabase", "local"}:
            raise ValueError("PERSISTENCE_TARGET must be one of: supabase, local")
        return normalized

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
