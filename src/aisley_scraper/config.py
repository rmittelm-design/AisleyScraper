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
    crawl_store_batch_size: int = Field(default=3, alias="CRAWL_STORE_BATCH_SIZE")
    crawl_per_domain_concurrency: int = Field(default=2, alias="CRAWL_PER_DOMAIN_CONCURRENCY")
    crawl_global_qps: int = Field(default=8, alias="CRAWL_GLOBAL_QPS")
    crawl_request_timeout_sec: int = Field(default=25, alias="CRAWL_REQUEST_TIMEOUT_SEC")
    crawl_connect_timeout_sec: int = Field(default=10, alias="CRAWL_CONNECT_TIMEOUT_SEC")
    crawl_http2_enabled: bool = Field(default=True, alias="CRAWL_HTTP2_ENABLED")
    crawl_http_max_connections: int = Field(default=100, alias="CRAWL_HTTP_MAX_CONNECTIONS")
    crawl_http_max_keepalive_connections: int = Field(
        default=20,
        alias="CRAWL_HTTP_MAX_KEEPALIVE_CONNECTIONS",
    )
    crawl_max_retries: int = Field(default=4, alias="CRAWL_MAX_RETRIES")
    crawl_backoff_base_ms: int = Field(default=500, alias="CRAWL_BACKOFF_BASE_MS")
    crawl_backoff_max_ms: int = Field(default=8000, alias="CRAWL_BACKOFF_MAX_MS")
    crawl_jitter_ms: int = Field(default=250, alias="CRAWL_JITTER_MS")
    crawl_long_jitter_min_ms: int = Field(default=900, alias="CRAWL_LONG_JITTER_MIN_MS")
    crawl_long_jitter_max_ms: int = Field(default=1600, alias="CRAWL_LONG_JITTER_MAX_MS")
    crawl_long_jitter_every_min_requests: int = Field(
        default=2,
        alias="CRAWL_LONG_JITTER_EVERY_MIN_REQUESTS",
    )
    crawl_long_jitter_every_max_requests: int = Field(
        default=3,
        alias="CRAWL_LONG_JITTER_EVERY_MAX_REQUESTS",
    )
    crawl_respect_robots: bool = Field(default=True, alias="CRAWL_RESPECT_ROBOTS")
    crawl_stall_log_interval_sec: int = Field(default=60, alias="CRAWL_STALL_LOG_INTERVAL_SEC")
    store_page_streaming_enabled: bool = Field(default=False, alias="STORE_PAGE_STREAMING_ENABLED")

    shopify_products_page_limit: int = Field(default=250, alias="SHOPIFY_PRODUCTS_PAGE_LIMIT")
    shopify_products_max_pages: int = Field(default=100, alias="SHOPIFY_PRODUCTS_MAX_PAGES")
    shopify_products_max_items_per_store: int = Field(
        default=0,
        alias="SHOPIFY_PRODUCTS_MAX_ITEMS_PER_STORE",
    )

    image_validation_enabled: bool = Field(default=True, alias="IMAGE_VALIDATION_ENABLED")
    image_validation_concurrency: int = Field(default=4, alias="IMAGE_VALIDATION_CONCURRENCY")
    image_validation_attempt_timeout_sec: float = Field(
        default=6.0,
        alias="IMAGE_VALIDATION_ATTEMPT_TIMEOUT_SEC",
    )
    image_validation_chunk_timeout_sec: float = Field(
        default=100.0,
        alias="IMAGE_VALIDATION_CHUNK_TIMEOUT_SEC",
    )
    image_validation_queue_max_retries: int = Field(
        default=3,
        alias="IMAGE_VALIDATION_QUEUE_MAX_RETRIES",
    )
    phase2_upload_concurrency: int = Field(default=8, alias="PHASE2_UPLOAD_CONCURRENCY")
    image_validation_max_retries: int = Field(default=2, alias="IMAGE_VALIDATION_MAX_RETRIES")
    fetcher_byte_cache_max_mb: int = Field(default=256, alias="FETCHER_BYTE_CACHE_MAX_MB")
    fetcher_disk_cache_enabled: bool = Field(default=True, alias="FETCHER_DISK_CACHE_ENABLED")
    fetcher_disk_cache_dir: str = Field(default=".aisley_image_cache", alias="FETCHER_DISK_CACHE_DIR")
    fetcher_disk_cache_max_mb: int = Field(default=2048, alias="FETCHER_DISK_CACHE_MAX_MB")
    image_min_width: int = Field(default=650, alias="IMAGE_MIN_WIDTH")
    image_min_height: int = Field(default=800, alias="IMAGE_MIN_HEIGHT")
    postprocess_product_chunk_size: int = Field(
        default=200,
        alias="POSTPROCESS_PRODUCT_CHUNK_SIZE",
    )
    phase2_max_unique_image_urls_per_chunk: int = Field(
        default=120,
        alias="PHASE2_MAX_UNIQUE_IMAGE_URLS_PER_CHUNK",
    )
    phase2_max_images_per_product: int = Field(
        default=5,
        alias="PHASE2_MAX_IMAGES_PER_PRODUCT",
    )
    phase2_first_image_product_validation_only: bool = Field(
        default=False,
        alias="PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY",
    )
    phase2_first_image_product_prob_threshold: float = Field(
        default=0.5,
        alias="PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD",
    )
    hf_token: str = Field(default="", alias="HF_TOKEN")

    crawl_run_state_path: str = Field(default=".aisley_active_run_id", alias="CRAWL_RUN_STATE_PATH")

    classify_require_ecom_signal: bool = Field(default=True, alias="CLASSIFY_REQUIRE_ECOM_SIGNAL")

    @field_validator(
        "crawl_global_concurrency",
        "crawl_store_batch_size",
        "crawl_per_domain_concurrency",
        "crawl_global_qps",
        "crawl_http_max_connections",
        "crawl_http_max_keepalive_connections",
        "image_validation_concurrency",
        "phase2_upload_concurrency",
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

    @field_validator("image_validation_queue_max_retries")
    @classmethod
    def non_negative_queue_retries(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator(
        "image_validation_attempt_timeout_sec",
        "image_validation_chunk_timeout_sec",
    )
    @classmethod
    def positive_float(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be > 0")
        return float(value)

    @field_validator("fetcher_byte_cache_max_mb")
    @classmethod
    def non_negative_cache_mb(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("fetcher_disk_cache_max_mb")
    @classmethod
    def non_negative_disk_cache_mb(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("postprocess_product_chunk_size")
    @classmethod
    def positive_chunk_size(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be > 0")
        return value

    @field_validator("phase2_max_unique_image_urls_per_chunk")
    @classmethod
    def positive_phase2_unique_url_cap(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be > 0")
        return value

    @field_validator("phase2_max_images_per_product")
    @classmethod
    def positive_phase2_images_per_product(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be > 0")
        return value

    @field_validator("phase2_first_image_product_prob_threshold")
    @classmethod
    def valid_phase2_first_image_product_prob_threshold(cls, value: float) -> float:
        v = float(value)
        if not (0.0 <= v <= 1.0):
            raise ValueError("must be between 0 and 1")
        return v

    @field_validator("crawl_stall_log_interval_sec")
    @classmethod
    def non_negative_stall_interval(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("shopify_products_max_items_per_store")
    @classmethod
    def non_negative_item_cap(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator(
        "crawl_jitter_ms",
        "crawl_long_jitter_min_ms",
        "crawl_long_jitter_max_ms",
        "crawl_long_jitter_every_min_requests",
        "crawl_long_jitter_every_max_requests",
    )
    @classmethod
    def non_negative_jitter_values(cls, value: int) -> int:
        if value < 0:
            raise ValueError("must be >= 0")
        return value

    @field_validator("crawl_long_jitter_max_ms")
    @classmethod
    def validate_long_jitter_max_ms(cls, value: int, info) -> int:
        min_ms = info.data.get("crawl_long_jitter_min_ms", 0)
        if value < min_ms:
            raise ValueError("must be >= CRAWL_LONG_JITTER_MIN_MS")
        return value

    @field_validator("crawl_long_jitter_every_max_requests")
    @classmethod
    def validate_long_jitter_every_max_requests(cls, value: int, info) -> int:
        min_requests = info.data.get("crawl_long_jitter_every_min_requests", 0)
        if value < min_requests:
            raise ValueError("must be >= CRAWL_LONG_JITTER_EVERY_MIN_REQUESTS")
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
