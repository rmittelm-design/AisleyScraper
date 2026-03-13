from __future__ import annotations

import asyncio
import io
import logging
import threading
from dataclasses import dataclass

from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.models import ProductRecord

logger = logging.getLogger(__name__)


class GenderProbComputationError(RuntimeError):
    pass


_EXPLICIT_GENDER_TO_CSV: dict[str, str] = {
    "male": "1.0,0.0,0.0",
    "female": "0.0,1.0,0.0",
    "unisex": "0.0,0.0,1.0",
}

_GENDER_CLASSES: tuple[str, str, str] = ("male", "female", "unisex")


_CLIP_PROMPTS: dict[str, list[str]] = {
    "male": [
        "fashion ecommerce product photo of men's clothing",
        "catalog image of a male outfit with apparel and shoes",
        "studio product image of menswear clothing",
        "online store photo of men's accessories like watches belts and sunglasses",
        "ecommerce photo of men's footwear sneakers boots or dress shoes",
        "catalog product image of male jewelry and watches",
    ],
    "female": [
        "fashion ecommerce product photo of women's clothing",
        "catalog image of a female outfit with apparel and shoes",
        "studio product image of womenswear clothing",
        "online store photo of women's accessories like handbags jewelry and watches",
        "ecommerce photo of women's footwear heels boots or sneakers",
        "catalog product image of female jewellery rings bracelets or necklaces",
    ],
    "unisex": [
        "fashion ecommerce product photo of unisex clothing",
        "catalog image of gender neutral outfit apparel shoes and accessories",
        "studio product image of unisex streetwear",
        "online store photo of unisex accessories watches and jewellery",
        "ecommerce image of unisex footwear sneakers boots or sandals",
        "catalog product image suitable for all genders",
    ],
}


@dataclass(slots=True)
class _ClipGenderModel:
    model: object
    preprocess: object
    tokenizer: object
    torch: object
    text_features: object
    device: str


_model_lock = threading.Lock()
_model_cache: _ClipGenderModel | None = None
_model_unavailable_reason: str | None = None


def one_hot_gender_probs_csv(gender_label: str | None) -> str | None:
    if gender_label is None:
        return None
    normalized = gender_label.strip().lower()
    return _EXPLICIT_GENDER_TO_CSV.get(normalized)


def _format_probability(value: float) -> str:
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    if not text:
        return "0"
    return text


def _probs_to_csv(male_prob: float, female_prob: float, unisex_prob: float) -> str:
    return ",".join(
        _format_probability(v)
        for v in (male_prob, female_prob, unisex_prob)
    )


def _normalize_probs(probs: tuple[float, float, float]) -> tuple[float, float, float]:
    clipped = tuple(max(0.0, min(1.0, float(v))) for v in probs)
    total = sum(clipped)
    if total <= 0:
        return (0.0, 0.0, 0.0)
    return (clipped[0] / total, clipped[1] / total, clipped[2] / total)


def _init_clip_model() -> _ClipGenderModel:
    import open_clip  # type: ignore[import-untyped]
    import torch  # type: ignore[import-untyped]

    model_name = "ViT-B-32"
    pretrained = "laion2b_s34b_b79k"
    device = "cuda" if torch.cuda.is_available() else "cpu"

    model, _, preprocess = open_clip.create_model_and_transforms(model_name, pretrained=pretrained)
    tokenizer = open_clip.get_tokenizer(model_name)

    model = model.to(device)
    model.eval()

    class_prompts = [_CLIP_PROMPTS[label] for label in _GENDER_CLASSES]
    flattened_prompts = [prompt for group in class_prompts for prompt in group]

    with torch.inference_mode():
        text_tokens = tokenizer(flattened_prompts).to(device)
        all_text_features = model.encode_text(text_tokens)
        all_text_features = all_text_features / all_text_features.norm(dim=-1, keepdim=True)

        prompt_count = len(class_prompts[0])
        per_class_vectors = []
        for idx in range(len(_GENDER_CLASSES)):
            start = idx * prompt_count
            end = start + prompt_count
            class_vector = all_text_features[start:end].mean(dim=0)
            class_vector = class_vector / class_vector.norm(dim=-1, keepdim=True)
            per_class_vectors.append(class_vector)
        text_features = torch.stack(per_class_vectors, dim=0)

    return _ClipGenderModel(
        model=model,
        preprocess=preprocess,
        tokenizer=tokenizer,
        torch=torch,
        text_features=text_features,
        device=device,
    )


def _get_clip_model() -> _ClipGenderModel | None:
    global _model_cache
    global _model_unavailable_reason

    if _model_cache is not None:
        return _model_cache
    if _model_unavailable_reason is not None:
        return None

    with _model_lock:
        if _model_cache is not None:
            return _model_cache
        if _model_unavailable_reason is not None:
            return None

        try:
            _model_cache = _init_clip_model()
            return _model_cache
        except Exception as exc:
            _model_unavailable_reason = str(exc)
            logger.warning("CLIP gender model unavailable: %s", exc)
            return None


def _score_image_bytes_with_clip(image_bytes: bytes) -> tuple[float, float, float] | None:
    clip_model = _get_clip_model()
    if clip_model is None:
        return None

    try:
        from PIL import Image

        pil_image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        image_tensor = clip_model.preprocess(pil_image).unsqueeze(0).to(clip_model.device)

        with clip_model.torch.inference_mode():
            image_features = clip_model.model.encode_image(image_tensor)
            image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            logits = 100.0 * image_features @ clip_model.text_features.T
            probs_tensor = clip_model.torch.softmax(logits, dim=-1)[0]

        probs = (
            float(probs_tensor[0].item()),
            float(probs_tensor[1].item()),
            float(probs_tensor[2].item()),
        )
        return _normalize_probs(probs)
    except Exception as exc:
        logger.info("CLIP scoring failed for one image: %s", exc)
        return None


async def _score_image_url(fetcher: Fetcher, image_url: str) -> tuple[float, float, float] | None:
    attempts = 5
    for attempt in range(1, attempts + 1):
        try:
            content = await fetcher.get_bytes(image_url)
        except Exception as exc:
            logger.info(
                "Failed to fetch image for CLIP scoring %s (attempt %s/%s): %s",
                image_url,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                await asyncio.sleep(0.3 * attempt)
                continue
            return None

        scored = await asyncio.to_thread(_score_image_bytes_with_clip, content)
        if scored is not None:
            return scored
        if attempt < attempts:
            await asyncio.sleep(0.2 * attempt)

    logger.info("CLIP scoring produced no result for %s after %s attempts", image_url, attempts)
    return None


async def enrich_gender_probabilities_for_products(
    *,
    products: list[ProductRecord],
    fetcher: Fetcher,
    concurrency: int = 8,
) -> None:
    # Explicit labels always map to deterministic one-hot outputs.
    products_needing_clip: list[ProductRecord] = []
    for product in products:
        explicit_csv = one_hot_gender_probs_csv(product.gender_label)
        if explicit_csv is not None:
            product.gender_probs_csv = explicit_csv
            continue
        product.gender_probs_csv = None
        if product.images:
            products_needing_clip.append(product)

    if not products_needing_clip:
        return

    unique_urls = {
        image_url.strip()
        for product in products_needing_clip
        for image_url in product.images
        if image_url and image_url.strip()
    }
    if not unique_urls:
        return

    semaphore = asyncio.Semaphore(max(1, concurrency))
    probs_by_url: dict[str, tuple[float, float, float] | None] = {}

    async def _run(image_url: str) -> None:
        async with semaphore:
            probs_by_url[image_url] = await _score_image_url(fetcher, image_url)

    await asyncio.gather(*(_run(url) for url in unique_urls))

    unresolved_products: list[str] = []

    for product in products_needing_clip:
        image_probs = [
            probs_by_url.get(image_url.strip())
            for image_url in product.images
            if image_url and image_url.strip()
        ]
        valid_probs = [probs for probs in image_probs if probs is not None]
        if not valid_probs:
            product.gender_probs_csv = None
            unresolved_products.append(product.product_id)
            continue

        count = float(len(valid_probs))
        avg = (
            sum(probs[0] for probs in valid_probs) / count,
            sum(probs[1] for probs in valid_probs) / count,
            sum(probs[2] for probs in valid_probs) / count,
        )
        normalized_avg = _normalize_probs(avg)
        product.gender_probs_csv = _probs_to_csv(*normalized_avg)

    if unresolved_products:
        raise GenderProbComputationError(
            "Failed to compute gender_probs_csv for products with images: "
            f"count={len(unresolved_products)} sample={unresolved_products[:10]}"
        )
