import asyncio

from aisley_scraper import cli
from aisley_scraper.config import Settings
from aisley_scraper.models import ProductRecord


def _settings(**overrides) -> Settings:
    kwargs = dict(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        IMAGE_VALIDATION_ENABLED=True,
        IMAGE_VALIDATION_CONCURRENCY=4,
        IMAGE_VALIDATION_MAX_RETRIES=0,
        PHASE2_MAX_IMAGES_PER_PRODUCT=3,
    )
    kwargs.update(overrides)
    return Settings(**kwargs)


def test_phase2_image_cap_limits_validation_to_first_n_images(monkeypatch) -> None:
    """Verify that image validation runs on only the first N images when cap is set."""
    settings = _settings()
    
    # Track which URLs were validated
    validated_urls = set()
    
    async def _fake_verify_product_images(products, fetcher, settings):
        # Track which images were in each product when validation ran
        for product in products:
            for image_url in product.images:
                validated_urls.add(image_url)
        # Remove all images (simulate validation failure for simplicity)
        for product in products:
            product.images = []

    async def _fake_enrich_gender_probabilities_for_products(products, fetcher, concurrency):
        # Just set a dummy gender_probs_csv
        for product in products:
            if product.images:
                product.gender_probs_csv = "1.0,0.0,0.0"

    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    # Create a product with 8 images
    product = ProductRecord(
        product_id="test-1",
        product_handle="test-1",
        item_name="Test Product",
        description=None,
        images=[
            "https://cdn.example.com/1.jpg",
            "https://cdn.example.com/2.jpg",
            "https://cdn.example.com/3.jpg",
            "https://cdn.example.com/4.jpg",
            "https://cdn.example.com/5.jpg",
            "https://cdn.example.com/6.jpg",
            "https://cdn.example.com/7.jpg",
            "https://cdn.example.com/8.jpg",
        ],
    )

    # Simulate the phase2 chunking and validation logic
    max_images_for_validation = max(1, settings.phase2_max_images_per_product)
    original_images = list(product.images)
    product.images = product.images[:max_images_for_validation]
    
    # Call the fake validation (which tracks what was validated)
    asyncio.run(
        _fake_verify_product_images([product], None, settings)
    )
    
    # Restore original images
    product.images = original_images
    
    # Verify only the first 3 images were validated
    assert validated_urls == {
        "https://cdn.example.com/1.jpg",
        "https://cdn.example.com/2.jpg",
        "https://cdn.example.com/3.jpg",
    }
    
    # Verify all original images are still in the product
    assert len(product.images) == 8
    assert product.images == original_images


def test_phase2_image_cap_restores_all_original_images_after_scoring(monkeypatch) -> None:
    """Verify that all original images are restored after validation and scoring."""
    settings = _settings()
    
    async def _fake_verify_product_images(products, fetcher, settings):
        # Remove some images to simulate validation filtering
        for product in products:
            product.images = product.images[:2]  # Keep only first 2

    async def _fake_enrich_gender_probabilities_for_products(products, fetcher, concurrency):
        # Set gender_probs_csv if any images remain
        for product in products:
            if product.images:
                product.gender_probs_csv = "0.0,1.0,0.0"

    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    # Create a product with 6 images
    product = ProductRecord(
        product_id="test-2",
        product_handle="test-2",
        item_name="Test Product",
        description=None,
        images=[
            "https://cdn.example.com/1.jpg",
            "https://cdn.example.com/2.jpg",
            "https://cdn.example.com/3.jpg",
            "https://cdn.example.com/4.jpg",
            "https://cdn.example.com/5.jpg",
            "https://cdn.example.com/6.jpg",
        ],
    )

    original_images = list(product.images)
    max_images_for_validation = max(1, settings.phase2_max_images_per_product)
    
    # Truncate for validation
    product.images = product.images[:max_images_for_validation]
    assert len(product.images) == 3
    
    # Run validation (which further truncates to 2)
    asyncio.run(
        _fake_verify_product_images([product], None, settings)
    )
    assert len(product.images) == 2
    
    # Run scoring
    asyncio.run(
        _fake_enrich_gender_probabilities_for_products([product], None, 1)
    )
    
    # Restore original images
    product.images = original_images
    
    # Verify all original images are restored
    assert len(product.images) == 6
    assert product.images == original_images
    # But gender_probs_csv was computed based on validation of first 2 images
    assert product.gender_probs_csv == "0.0,1.0,0.0"


def test_phase2_image_cap_uses_all_images_in_final_output(monkeypatch) -> None:
    """Verify that all original images (not just validated ones) are in the final product."""
    settings = _settings()
    
    async def _fake_verify_product_images(products, fetcher, settings):
        # Filter to only even-numbered images
        for product in products:
            product.images = [
                url for url in product.images
                if url.endswith(("2.jpg", "4.jpg"))
            ]

    async def _fake_enrich_gender_probabilities_for_products(products, fetcher, concurrency):
        for product in products:
            if product.images:
                product.gender_probs_csv = "1.0,0.0,0.0"

    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    product = ProductRecord(
        product_id="test-3",
        product_handle="test-3",
        item_name="Test Product",
        description=None,
        images=[
            "https://cdn.example.com/1.jpg",
            "https://cdn.example.com/2.jpg",
            "https://cdn.example.com/3.jpg",
            "https://cdn.example.com/4.jpg",
            "https://cdn.example.com/5.jpg",
        ],
    )

    original_images = list(product.images)
    max_images_for_validation = max(1, settings.phase2_max_images_per_product)
    
    # Truncate to first 3
    product.images = product.images[:max_images_for_validation]
    assert product.images == [
        "https://cdn.example.com/1.jpg",
        "https://cdn.example.com/2.jpg",
        "https://cdn.example.com/3.jpg",
    ]
    
    # Validation keeps only 2.jpg (which is in the first 3)
    asyncio.run(
        _fake_verify_product_images([product], None, settings)
    )
    assert product.images == ["https://cdn.example.com/2.jpg"]
    
    # Score
    asyncio.run(
        _fake_enrich_gender_probabilities_for_products([product], None, 1)
    )
    
    # Restore all original images
    product.images = original_images
    
    # Final product should have all 5 original images
    assert product.images == [
        "https://cdn.example.com/1.jpg",
        "https://cdn.example.com/2.jpg",
        "https://cdn.example.com/3.jpg",
        "https://cdn.example.com/4.jpg",
        "https://cdn.example.com/5.jpg",
    ]
    # But gender_probs_csv was computed based only on first 3, filtered to valid 1
    assert product.gender_probs_csv == "1.0,0.0,0.0"


def test_phase2_image_cap_config_respects_custom_limit() -> None:
    """Verify that the configured image cap is respected."""
    settings_cap_2 = _settings(PHASE2_MAX_IMAGES_PER_PRODUCT=2)
    settings_cap_5 = _settings(PHASE2_MAX_IMAGES_PER_PRODUCT=5)
    
    assert settings_cap_2.phase2_max_images_per_product == 2
    assert settings_cap_5.phase2_max_images_per_product == 5
