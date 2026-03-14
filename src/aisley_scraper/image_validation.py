from __future__ import annotations

import io
import logging
import os
import threading
import time
import warnings
from dataclasses import dataclass
from typing import Any, Optional

from aisley_scraper.hf_auth import ensure_hf_token_from_settings


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value < minimum:
        return default
    return value

MAX_IMAGE_BYTES = 10 * 1024 * 1024
REQUIRED_MIN_WIDTH = _env_int("IMAGE_MIN_WIDTH", 800)
REQUIRED_MIN_HEIGHT = _env_int("IMAGE_MIN_HEIGHT", 800)
REQUIRED_MAX_WIDTH = 12000
REQUIRED_MAX_HEIGHT = 12000
MIN_ASPECT_RATIO = 0.5
MAX_ASPECT_RATIO = 2.0

# Threshold tuned to reject only very blurry images from imports/uploads.
MIN_BLUR_SCORE = 20.0
# Secondary sharpness floor (Tenengrad, normalized) used when Laplacian alone
# underestimates sharpness on low-texture product photos.
MIN_TENENGRAD_SCORE = 1.8
MIN_PATCH_P90_BLUR_SCORE = 35.0

# Adaptive blur thresholding for low-texture images.
LOW_EDGE_DENSITY_THRESHOLD = 0.08
MEDIUM_EDGE_DENSITY_THRESHOLD = 0.14
HIGH_EDGE_DENSITY_THRESHOLD = 0.20
LOW_EDGE_BLUR_MULTIPLIER = 0.55
MEDIUM_EDGE_BLUR_MULTIPLIER = 0.75
HIGH_EDGE_BLUR_MULTIPLIER = 0.60
MIN_BRIGHTNESS_MEAN = 55.0
# White-background product photos tend to have a high mean brightness; allow more headroom.
MAX_BRIGHTNESS_MEAN = 240.0
MIN_CONTRAST_STD = 18.0

# Keep product-photo validation active. Detector robustness is improved via
# richer prompts, so we can keep a stricter threshold.
MIN_PRODUCT_PROB = 0.50

CLIP_PRODUCT_POSITIVE_PROMPTS = [
    "an ecommerce product photo of clothing",
    "a catalog photo of a garment",
    "a studio photo of apparel on a plain background",
    "a fashion product image with a model",
    "a clothing item laid flat for online store listing",
    "an ecommerce product photo of shoes",
    "a catalog photo of footwear",
    "a studio product image of sneakers or heels",
    "an ecommerce product photo of a handbag or accessory",
    "a catalog image of sunglasses, belt, or hat",
    "a product photo of jewelry on a clean background",
    "a catalog image of necklace, ring, bracelet, or earrings",
]

CLIP_PRODUCT_NEGATIVE_PROMPTS = [
    "a screenshot",
    "a meme",
    "a selfie",
    "a group photo",
    "a landscape photo",
    "a store logo",
    "a text-heavy poster",
    "an abstract graphic",
]

# Only reject as too bright/dark when the image is both extreme in mean brightness
# and has substantial clipping near white/black.
OVEREXPOSED_PIXEL_THRESHOLD = 250
UNDEREXPOSED_PIXEL_THRESHOLD = 5
MAX_OVEREXPOSED_FRACTION = 0.65
MAX_UNDEREXPOSED_FRACTION = 0.65

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImageValidationFailure(Exception):
    code: str
    message: str
    details: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "error_code": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def _safe_basename(name: str) -> str:
    # Avoid weird paths from user agents.
    base = os.path.basename(name or "upload")
    return base or "upload"


def _replace_ext(filename: str, new_ext: str) -> str:
    root, _ = os.path.splitext(filename)
    return f"{root}{new_ext}"


def _looks_like_heic(data: bytes) -> bool:
    # HEIF/HEIC containers typically contain brand strings like ftypheic/ftypheif
    # within the first 16 bytes.
    head = data[:32]
    return (b"ftypheic" in head) or (b"ftypheif" in head) or (b"ftypmif1" in head)


def _open_pil_image(data: bytes):
    try:
        from PIL import Image, ImageOps
    except Exception as exc:  # pragma: no cover
        raise ImageValidationFailure(
            code="server_missing_dependency",
            message="Server is missing Pillow; image validation is unavailable.",
            details={"dependency": "Pillow", "error": str(exc)},
        )

    try:
        # Register HEIF opener if available.
        try:
            from pillow_heif import register_heif_opener

            register_heif_opener()
        except Exception:
            pass

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r"Invalid profile .*")
            warnings.filterwarnings("ignore", message=r".*iCCP.*")
            warnings.filterwarnings(
                "ignore",
                message=r"Palette images with Transparency expressed in bytes should be converted to RGBA images",
            )
            img = Image.open(io.BytesIO(data))
            opened_format = (getattr(img, "format", None) or "").upper().strip()
            img = ImageOps.exif_transpose(img)
            if img.mode == "P" and "transparency" in getattr(img, "info", {}):
                img = img.convert("RGBA")
            img = img.convert("RGB")

        try:
            img.info.pop("icc_profile", None)
        except Exception:
            pass

        # Pillow can drop `.format` on the returned/transposed image; keep a copy for detection.
        try:
            setattr(img, "_detected_format", opened_format)
        except Exception:
            pass
        img.load()
        return img
    except ImageValidationFailure:
        raise
    except Exception as exc:
        raise ImageValidationFailure(
            code="invalid_image",
            message="File could not be decoded as an image.",
            details={"error": str(exc)},
        )


def _detect_format(data: bytes, pil_img) -> str:
    fmt = (getattr(pil_img, "format", None) or "").upper().strip()
    if not fmt:
        fmt = (getattr(pil_img, "_detected_format", None) or "").upper().strip()
    if fmt:
        return fmt

    # Fallback: magic-byte sniffing.
    head = data[:32]
    if head.startswith(b"\xFF\xD8\xFF"):
        return "JPEG"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return "GIF"
    if _looks_like_heic(data):
        return "HEIC"
    return "UNKNOWN"


def _encode_jpeg(pil_img, quality: int = 92) -> bytes:
    try:
        from PIL import Image
    except Exception as exc:  # pragma: no cover
        raise ImageValidationFailure(
            code="server_missing_dependency",
            message="Server is missing Pillow; image conversion is unavailable.",
            details={"dependency": "Pillow", "error": str(exc)},
        )

    rgb = pil_img.convert("RGB")
    buf = io.BytesIO()
    rgb.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()


def assess_image_quality(pil_img) -> dict[str, Any]:
    try:
        import cv2
        import numpy as np
    except Exception as exc:  # pragma: no cover
        raise ImageValidationFailure(
            code="quality_checks_unavailable",
            message="Server is missing image quality dependencies; upload cannot be validated.",
            details={"dependencies": ["opencv-python-headless", "numpy"], "error": str(exc)},
        )

    img = np.array(pil_img.convert("RGB"))
    gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)

    h, w = gray.shape[:2]
    laplacian = cv2.Laplacian(gray, cv2.CV_64F)
    blur_score_global = float(laplacian.var())

    grad_x = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
    grad_y = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)
    gradient_mag = cv2.magnitude(grad_x, grad_y)

    edge_percentile = float(np.percentile(gradient_mag, 75.0))
    edge_threshold = max(edge_percentile, 1.0)
    edge_mask = gradient_mag >= edge_threshold
    edge_pixels = int(np.count_nonzero(edge_mask))
    total_pixels = int(gray.size) if gray.size else 1
    edge_density = float(edge_pixels) / float(total_pixels)

    if edge_pixels >= 128:
        blur_score_edge = float(np.var(laplacian[edge_mask]))
    else:
        blur_score_edge = blur_score_global

    # Patch-level sharpness catches images where the product region is sharp
    # but global metrics are diluted by smooth backgrounds.
    patch_scores: list[float] = []
    patch_rows = 4
    patch_cols = 4
    patch_h = max(1, h // patch_rows)
    patch_w = max(1, w // patch_cols)
    for r in range(patch_rows):
        y0 = r * patch_h
        y1 = h if r == patch_rows - 1 else min(h, (r + 1) * patch_h)
        if y1 <= y0:
            continue
        for c in range(patch_cols):
            x0 = c * patch_w
            x1 = w if c == patch_cols - 1 else min(w, (c + 1) * patch_w)
            if x1 <= x0:
                continue
            patch = laplacian[y0:y1, x0:x1]
            if patch.size == 0:
                continue
            patch_scores.append(float(np.var(patch)))

    if patch_scores:
        patch_p90_blur_score = float(np.percentile(np.array(patch_scores, dtype=np.float64), 90.0))
    else:
        patch_p90_blur_score = blur_score_global

    # Edge-aware score: preserve compatibility with historic thresholds while
    # recovering detail from images where texture is concentrated in limited regions.
    blur_score = max(blur_score_global, blur_score_edge)

    tenengrad = float(np.mean(gradient_mag * gradient_mag))
    tenengrad_score = (tenengrad / (255.0 * 255.0)) * 100.0

    adaptive_min_blur_score = float(MIN_BLUR_SCORE)
    if edge_density >= HIGH_EDGE_DENSITY_THRESHOLD:
        adaptive_min_blur_score = float(MIN_BLUR_SCORE * HIGH_EDGE_BLUR_MULTIPLIER)
    elif edge_density < LOW_EDGE_DENSITY_THRESHOLD:
        adaptive_min_blur_score = float(MIN_BLUR_SCORE * LOW_EDGE_BLUR_MULTIPLIER)
    elif edge_density < MEDIUM_EDGE_DENSITY_THRESHOLD:
        adaptive_min_blur_score = float(MIN_BLUR_SCORE * MEDIUM_EDGE_BLUR_MULTIPLIER)

    is_blurry = bool(
        (blur_score < adaptive_min_blur_score)
        and (tenengrad_score < MIN_TENENGRAD_SCORE)
        and (patch_p90_blur_score < MIN_PATCH_P90_BLUR_SCORE)
    )

    brightness_mean = float(np.mean(gray))
    contrast_std = float(np.std(gray))

    total = float(gray.size) if gray.size else 1.0
    overexposed_fraction = float(np.count_nonzero(gray >= OVEREXPOSED_PIXEL_THRESHOLD)) / total
    underexposed_fraction = float(np.count_nonzero(gray <= UNDEREXPOSED_PIXEL_THRESHOLD)) / total
    return {
        "width": int(w),
        "height": int(h),
        "blur_score": blur_score,
        "blur_score_global": blur_score_global,
        "blur_score_edge": blur_score_edge,
        "edge_density": edge_density,
        "tenengrad_score": tenengrad_score,
        "patch_p90_blur_score": patch_p90_blur_score,
        "adaptive_min_blur_score": adaptive_min_blur_score,
        "is_blurry": is_blurry,
        "brightness_mean": brightness_mean,
        "contrast_std": contrast_std,
        "overexposed_fraction": overexposed_fraction,
        "underexposed_fraction": underexposed_fraction,
    }


_CLIP_LOCK = threading.Lock()
_CLIP_MODEL = None
_CLIP_PREPROCESS = None
_CLIP_TOKENIZER = None
_CLIP_TEXT_PROMPTS: Optional[list[str]] = None
_CLIP_TEXT_FEATURES = None

_CLIP_GENDER_PROMPTS: Optional[list[str]] = None
_CLIP_GENDER_TEXT_FEATURES = None


def _get_clip():
    global _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_TOKENIZER
    with _CLIP_LOCK:
        if _CLIP_MODEL is not None:
            return _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_TOKENIZER
        ensure_hf_token_from_settings()
        try:
            import open_clip
            import torch
        except Exception as exc:  # pragma: no cover
            raise ImageValidationFailure(
                code="product_check_unavailable",
                message="Server is missing CLIP dependencies; product verification is unavailable.",
                details={"dependencies": ["torch", "open-clip-torch"], "error": str(exc)},
            )

        # This will download weights the first time if not already cached.
        model, _, preprocess = open_clip.create_model_and_transforms(
            "ViT-B-32",
            pretrained="laion2b_s34b_b79k",
        )
        tokenizer = open_clip.get_tokenizer("ViT-B-32")

        model.eval()
        _CLIP_MODEL = model
        _CLIP_PREPROCESS = preprocess
        _CLIP_TOKENIZER = tokenizer
        return _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_TOKENIZER


def warmup_clip(*, strict: bool = True) -> None:
    """Load CLIP weights and precompute prompt embeddings.

    This avoids startup latency on the first upload and reduces per-request compute
    (text prompt encoding is cached).
    """

    global _CLIP_TEXT_PROMPTS, _CLIP_TEXT_FEATURES
    global _CLIP_GENDER_PROMPTS, _CLIP_GENDER_TEXT_FEATURES

    try:
        model, _, tokenizer = _get_clip()
        import torch
    except Exception as exc:
        if strict:
            raise
        logger.warning("CLIP warmup failed: %s", exc)
        return

    prompts = CLIP_PRODUCT_POSITIVE_PROMPTS + CLIP_PRODUCT_NEGATIVE_PROMPTS

    # Encode text prompts once.
    with _CLIP_LOCK:
        if _CLIP_TEXT_FEATURES is not None and _CLIP_TEXT_PROMPTS == prompts:
            # Still proceed to gender prompt warmup + dummy forward pass below.
            pass
        text_input = tokenizer(prompts)
        with torch.no_grad():
            text_features = model.encode_text(text_input)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        _CLIP_TEXT_PROMPTS = prompts
        _CLIP_TEXT_FEATURES = text_features

    gender_prompts = [
        "a product photo of men's clothing",
        "a product photo of women's clothing",
        "a product photo of unisex clothing",
    ]

    with _CLIP_LOCK:
        if _CLIP_GENDER_TEXT_FEATURES is not None and _CLIP_GENDER_PROMPTS == gender_prompts:
            pass
        gender_text_input = tokenizer(gender_prompts)
        with torch.no_grad():
            gender_text_features = model.encode_text(gender_text_input)
            gender_text_features = gender_text_features / gender_text_features.norm(dim=-1, keepdim=True)
        _CLIP_GENDER_PROMPTS = gender_prompts
        _CLIP_GENDER_TEXT_FEATURES = gender_text_features

    # Dummy forward pass to reduce first-request latency (torch/OpenCLIP can do lazy init).
    try:
        from PIL import Image

        dummy = Image.new("RGB", (224, 224), color=(255, 255, 255))
        image_input = _CLIP_PREPROCESS(dummy).unsqueeze(0)
        with torch.no_grad():
            _ = model.encode_image(image_input)
    except Exception as exc:
        if strict:
            raise
        logger.warning("CLIP dummy forward warmup failed: %s", exc)


def warmup_quality_checks(*, strict: bool = False) -> None:
    """Warm up numpy/opencv imports and code paths to reduce first-request latency."""

    try:
        from PIL import Image

        dummy = Image.new("RGB", (256, 256), color=(127, 127, 127))
        _ = assess_image_quality(dummy)
    except Exception as exc:
        if strict:
            raise
        logger.warning("Quality warmup failed: %s", exc)


def _format_probs_csv(values: list[float]) -> str:
    # Stable + compact string for storage.
    return ",".join(f"{v:.6f}" for v in values)


def _parse_probs_csv(value: Optional[str]) -> Optional[list[float]]:
    if not value:
        return None
    parts = [p.strip() for p in str(value).split(",")]
    if len(parts) != 3:
        return None
    try:
        return [float(parts[0]), float(parts[1]), float(parts[2])]
    except Exception:
        return None


def gender_probs_clip(pil_img) -> dict[str, Any]:
    """Return CLIP-based gender probabilities for an image.

    Output probabilities are ordered: [male, female, unisex].
    """
    global _CLIP_GENDER_PROMPTS, _CLIP_GENDER_TEXT_FEATURES

    model, preprocess, tokenizer = _get_clip()
    try:
        import torch
    except Exception as exc:  # pragma: no cover
        raise ImageValidationFailure(
            code="gender_check_unavailable",
            message="Server is missing torch; gender verification is unavailable.",
            details={"dependency": "torch", "error": str(exc)},
        )

    prompts = [
        "a product photo of men's clothing",
        "a product photo of women's clothing",
        "a product photo of unisex clothing",
    ]
    labels = ["male", "female", "unisex"]

    image_input = preprocess(pil_img.convert("RGB")).unsqueeze(0)

    with _CLIP_LOCK:
        cached_prompts = _CLIP_GENDER_PROMPTS
        cached_features = _CLIP_GENDER_TEXT_FEATURES
    if cached_features is None or cached_prompts != prompts:
        warmup_clip(strict=False)
        with _CLIP_LOCK:
            cached_prompts = _CLIP_GENDER_PROMPTS
            cached_features = _CLIP_GENDER_TEXT_FEATURES
    if cached_features is None or cached_prompts != prompts:
        text_input = tokenizer(prompts)
        with torch.no_grad():
            text_features = model.encode_text(text_input)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
    else:
        text_features = cached_features

    with torch.no_grad():
        image_features = model.encode_image(image_input)
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        logits = (image_features @ text_features.T).squeeze(0)
        probs = torch.softmax(logits, dim=-1)

    values = [float(probs[i].item()) for i in range(len(labels))]
    winner_idx = int(torch.argmax(probs).item())
    winner = labels[winner_idx]
    return {
        "labels": labels,
        "probs": {labels[i]: values[i] for i in range(len(labels))},
        "probs_csv": _format_probs_csv(values),
        "winner": winner,
    }


def product_probability_clip(pil_img) -> dict[str, Any]:
    global _CLIP_TEXT_PROMPTS, _CLIP_TEXT_FEATURES

    model, preprocess, tokenizer = _get_clip()
    try:
        import torch
    except Exception as exc:  # pragma: no cover
        raise ImageValidationFailure(
            code="product_check_unavailable",
            message="Server is missing torch; product verification is unavailable.",
            details={"dependency": "torch", "error": str(exc)},
        )

    product_prompts = CLIP_PRODUCT_POSITIVE_PROMPTS
    non_product_prompts = CLIP_PRODUCT_NEGATIVE_PROMPTS
    prompts = product_prompts + non_product_prompts
    positive_count = len(product_prompts)

    image_input = preprocess(pil_img.convert("RGB")).unsqueeze(0)

    # Use cached text embeddings if available; otherwise compute once and cache.
    with _CLIP_LOCK:
        cached_prompts = _CLIP_TEXT_PROMPTS
        cached_features = _CLIP_TEXT_FEATURES
    if cached_features is None or cached_prompts != prompts:
        warmup_clip(strict=False)
        with _CLIP_LOCK:
            cached_prompts = _CLIP_TEXT_PROMPTS
            cached_features = _CLIP_TEXT_FEATURES
    if cached_features is None or cached_prompts != prompts:
        text_input = tokenizer(prompts)
        with torch.no_grad():
            text_features = model.encode_text(text_input)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
    else:
        text_features = cached_features

    with torch.no_grad():
        image_features = model.encode_image(image_input)
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        logits = (image_features @ text_features.T).squeeze(0)
        probs = torch.softmax(logits, dim=-1)

        # Aggregate all product-like prompt probabilities for a more stable score.
        product_prob = float(probs[:positive_count].sum().item())
        best_positive_idx = int(torch.argmax(probs[:positive_count]).item())
        best_negative_idx_local = int(torch.argmax(probs[positive_count:]).item())
        best_negative_idx = positive_count + best_negative_idx_local
        product_margin = float(probs[best_positive_idx].item() - probs[best_negative_idx].item())
        best_idx = int(torch.argmax(probs).item())
        return {
            "product_prob": product_prob,
            "product_margin": product_margin,
            "best_positive_prompt": prompts[best_positive_idx],
            "best_negative_prompt": prompts[best_negative_idx],
            "best_prompt": prompts[best_idx],
            "probs": {prompts[i]: float(probs[i].item()) for i in range(len(prompts))},
        }


def validate_and_normalize_upload(
    *,
    content: bytes,
    filename: str,
) -> dict[str, Any]:
    """Validate an uploaded image and optionally normalize (e.g., HEIC -> JPG).

    Returns a dict with:
      - ok: True
      - normalized_bytes
      - normalized_filename
      - normalized_content_type
      - quality
      - product
    - nsfw (always None)
    """

    if not content:
        raise ImageValidationFailure(code="empty_file", message="Upload is empty.")

    if len(content) > MAX_IMAGE_BYTES:
        raise ImageValidationFailure(
            code="file_too_large",
            message="Image must be smaller than 10MB.",
            details={"max_bytes": MAX_IMAGE_BYTES, "actual_bytes": len(content)},
        )

    timings: dict[str, float] = {}

    safe_name = _safe_basename(filename)
    t0 = time.perf_counter()
    pil_img = _open_pil_image(content)
    timings["decode_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    detected = _detect_format(content, pil_img)
    timings["format_detect_s"] = time.perf_counter() - t0

    if detected == "GIF":
        raise ImageValidationFailure(code="gif_not_supported", message="GIF images are not supported.")

    normalized_bytes = content
    normalized_name = safe_name
    normalized_content_type = None
    normalized_format = detected

    if detected in {"HEIC", "HEIF"} or _looks_like_heic(content):
        t0 = time.perf_counter()
        normalized_bytes = _encode_jpeg(pil_img)
        timings["heic_convert_s"] = time.perf_counter() - t0
        if len(normalized_bytes) > MAX_IMAGE_BYTES:
            raise ImageValidationFailure(
                code="file_too_large",
                message="Image must be smaller than 10MB.",
                details={"max_bytes": MAX_IMAGE_BYTES, "actual_bytes": len(normalized_bytes)},
            )
        normalized_name = _replace_ext(safe_name, ".jpg")
        normalized_content_type = "image/jpeg"
        normalized_format = "JPEG"
    elif detected in {"JPEG", "JPG"}:
        normalized_content_type = "image/jpeg"
    elif detected == "PNG":
        normalized_content_type = "image/png"
    else:
        raise ImageValidationFailure(
            code="unsupported_file_type",
            message="Unsupported image type. Please upload a JPG/JPEG or PNG (HEIC will be converted to JPG).",
            details={"detected_format": detected},
        )

    # Re-open if we converted.
    if normalized_bytes is not content:
        t0 = time.perf_counter()
        pil_img = _open_pil_image(normalized_bytes)
        timings["redecode_s"] = time.perf_counter() - t0

    w, h = int(getattr(pil_img, "width", 0)), int(getattr(pil_img, "height", 0))
    if w < REQUIRED_MIN_WIDTH or h < REQUIRED_MIN_HEIGHT:
        raise ImageValidationFailure(
            code="resolution_too_low",
            message="Image resolution is too low. Minimum is 800x800 pixels.",
            details={
                "width": w,
                "height": h,
                "min_width": REQUIRED_MIN_WIDTH,
                "min_height": REQUIRED_MIN_HEIGHT,
            },
        )
    if w > REQUIRED_MAX_WIDTH or h > REQUIRED_MAX_HEIGHT:
        raise ImageValidationFailure(
            code="resolution_too_high",
            message="Image resolution is too high.",
            details={
                "width": w,
                "height": h,
                "max_width": REQUIRED_MAX_WIDTH,
                "max_height": REQUIRED_MAX_HEIGHT,
            },
        )

    aspect_ratio = (float(w) / float(h)) if h else 0.0
    if not (MIN_ASPECT_RATIO <= aspect_ratio <= MAX_ASPECT_RATIO):
        raise ImageValidationFailure(
            code="invalid_aspect_ratio",
            message="Image aspect ratio is not supported.",
            details={"width": w, "height": h, "aspect_ratio": aspect_ratio, "min": MIN_ASPECT_RATIO, "max": MAX_ASPECT_RATIO},
        )

    t0 = time.perf_counter()
    quality = assess_image_quality(pil_img)
    timings["quality_s"] = time.perf_counter() - t0
    if bool(quality.get("is_blurry", False)):
        raise ImageValidationFailure(
            code="image_too_blurry",
            message="Image is too blurry. Please upload a sharper photo.",
            details={
                "blur_score": quality["blur_score"],
                "min_blur_score": quality.get("adaptive_min_blur_score", MIN_BLUR_SCORE),
                "base_min_blur_score": MIN_BLUR_SCORE,
                "tenengrad_score": quality.get("tenengrad_score"),
                "min_tenengrad_score": MIN_TENENGRAD_SCORE,
                "patch_p90_blur_score": quality.get("patch_p90_blur_score"),
                "min_patch_p90_blur_score": MIN_PATCH_P90_BLUR_SCORE,
                "edge_density": quality.get("edge_density"),
                "blur_score_global": quality.get("blur_score_global"),
                "blur_score_edge": quality.get("blur_score_edge"),
            },
        )
    if float(quality["brightness_mean"]) < MIN_BRIGHTNESS_MEAN:
        # Reject only if a substantial portion is near-black (clipped).
        if float(quality.get("underexposed_fraction", 0.0)) >= MAX_UNDEREXPOSED_FRACTION:
            raise ImageValidationFailure(
                code="image_too_dark",
                message="Image is too dark. Please upload a brighter photo.",
                details={
                    "brightness_mean": quality["brightness_mean"],
                    "min": MIN_BRIGHTNESS_MEAN,
                    "underexposed_fraction": quality.get("underexposed_fraction"),
                },
            )
    if float(quality["brightness_mean"]) > MAX_BRIGHTNESS_MEAN:
        # Reject only if a substantial portion is near-white (clipped).
        if float(quality.get("overexposed_fraction", 0.0)) >= MAX_OVEREXPOSED_FRACTION:
            raise ImageValidationFailure(
                code="image_too_bright",
                message="Image is too bright. Please reduce exposure and try again.",
                details={
                    "brightness_mean": quality["brightness_mean"],
                    "max": MAX_BRIGHTNESS_MEAN,
                    "overexposed_fraction": quality.get("overexposed_fraction"),
                },
            )
    if float(quality["contrast_std"]) < MIN_CONTRAST_STD:
        raise ImageValidationFailure(
            code="low_contrast",
            message="Image has low contrast. Please upload a clearer photo.",
            details={"contrast_std": quality["contrast_std"], "min": MIN_CONTRAST_STD},
        )

    t0 = time.perf_counter()
    product = product_probability_clip(pil_img)
    timings["clip_product_s"] = time.perf_counter() - t0
    product_prob = float(product.get("product_prob", 0.0))
    if product_prob < MIN_PRODUCT_PROB:
        raise ImageValidationFailure(
            code="not_a_product_photo",
            message="Image does not look like a product photo. Please upload a product photo on a plain background.",
            details={
                "product_prob": product_prob,
                "min_product_prob": MIN_PRODUCT_PROB,
                "best_prompt": product.get("best_prompt"),
                "probs": product.get("probs"),
            },
        )

    nsfw = None

    return {
        "ok": True,
        "normalized_bytes": normalized_bytes,
        "normalized_filename": normalized_name,
        "normalized_content_type": normalized_content_type,
        "normalized_format": normalized_format,
        "quality": quality,
        "product": product,
        "nsfw": nsfw,
        "timings": timings,
    }
