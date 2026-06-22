from __future__ import annotations

import io
import logging
import os
import queue as _queue
import threading
import time
import warnings
from dataclasses import dataclass
from typing import Any, Optional

from aisley_scraper.hf_auth import ensure_hf_token_from_settings

# Let any op the MPS (Apple-Silicon GPU) backend doesn't implement fall back to
# CPU instead of raising. Must be set before torch initializes the MPS backend.
os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")


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

# Decode guard only — scraped images are not saved by these validators; this
# just avoids decoding a pathologically large file into memory.
MAX_IMAGE_BYTES = 50 * 1024 * 1024
# Keep in sync with config IMAGE_MIN_WIDTH / IMAGE_MIN_HEIGHT (defaults below).
REQUIRED_MIN_WIDTH = 600
REQUIRED_MIN_HEIGHT = 800
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

# Broad fashion category taxonomy vendored from the AisleyRebrand backend
# (app/services/labels.py :: COARSE_CATEGORIES, 41 classes) plus pajamas and
# lingerie. These are the positive ("is fashion") classes; the configured
# FashionSigLIP model encodes them. List order is not significant here — the
# classifier only takes the single best-matching category (max-vs-max).
BROAD_FASHION_CATEGORIES: list[str] = [
    "coats", "jackets", "trench", "blazer-as-outerwear", "puffers", "tees",
    "shirts", "blouses", "sweaters", "cardigans", "hoodies", "sweatshirts",
    "jeans", "trousers", "pants", "shorts", "skirts", "dresses", "jumpsuits",
    "rompers", "boots", "sneakers", "heels", "flats", "sandals", "totes",
    "handbags", "backpacks", "crossbody", "jewelry", "belts", "scarves", "hats",
    "sunglasses", "leggings", "sports bras", "sets", "sweats", "gowns",
    "cocktail", "suiting", "pajamas", "lingerie", "sleepwear", "underwear",
    "tops", "bottoms",
]

# Natural-language phrasing for prompts (some tokens need expanding,
# e.g. "trench" -> "a trench coat"). Falls back to the raw token.
_CATEGORY_PROMPT_PHRASE: dict[str, str] = {
    "coats": "a coat", "jackets": "a jacket", "trench": "a trench coat",
    "blazer-as-outerwear": "a blazer", "puffers": "a puffer jacket",
    "tees": "a t-shirt", "shirts": "a shirt", "blouses": "a blouse",
    "sweaters": "a sweater", "cardigans": "a cardigan", "hoodies": "a hoodie",
    "sweatshirts": "a sweatshirt", "jeans": "jeans", "trousers": "trousers",
    "pants": "pants", "shorts": "shorts", "skirts": "a skirt", "dresses": "a dress",
    "jumpsuits": "a jumpsuit", "rompers": "a romper", "boots": "boots",
    "sneakers": "sneakers", "heels": "high heels", "flats": "flat shoes",
    "sandals": "sandals", "totes": "a tote bag", "handbags": "a handbag",
    "backpacks": "a backpack", "crossbody": "a crossbody bag", "jewelry": "jewelry",
    "belts": "a belt", "scarves": "a scarf", "hats": "a hat", "sunglasses": "sunglasses",
    "leggings": "leggings", "sports bras": "a sports bra", "sets": "a matching clothing set",
    "sweats": "sweatpants", "gowns": "a gown", "cocktail": "a cocktail dress",
    "suiting": "a suit", "pajamas": "pajamas", "lingerie": "lingerie",
    "sleepwear": "sleepwear", "underwear": "underwear", "tops": "a top",
    "bottoms": "bottoms",
}

# Generic apparel anchors (catch fashion not captured by a specific category,
# and an on-model anchor so model shots aren't lost to portrait negatives).
_GENERIC_FASHION_PROMPTS = [
    "an ecommerce product photo of clothing",
    "a fashion product photo of an apparel item worn by a model",
    "a product photo of footwear",
]

CLIP_PRODUCT_POSITIVE_PROMPTS = _GENERIC_FASHION_PROMPTS + [
    f"a product photo of {_CATEGORY_PROMPT_PHRASE.get(cat, cat)}"
    for cat in BROAD_FASHION_CATEGORIES
]

CLIP_PRODUCT_NEGATIVE_PROMPTS = [
    "a screenshot of a phone, website, or software interface",
    "a meme",
    "a selfie or close-up portrait of a person's face",
    "a group photo",
    "a landscape photo",
    "a store logo",
    "a text-heavy poster",
    "an abstract graphic",
    "a product photo of a bottle",
    "a catalog photo of children's clothing",
    "a product photo of a doll",
    "a product photo of a teddy bear",
    "a product photo of canned food",
    "a product photo of perfume",
    "a product photo of a cardboard box",
    "a product photo of electronics",
    "a product photo of an item inside a box",
    "a product photo of children or baby clothes or shoes",
    "a product photo of a smartphone laptop tablet or computer",
    "a product photo of headphones speaker camera or gaming console",
    "a product photo of kitchenware cookware or utensils",
    "a product photo of dishes cups mugs or glassware",
    "a product photo of furniture chair table sofa or bed",
    "a product photo of home decor candle vase frame or lamp",
    "a product photo of cleaning supplies detergent or paper towels",
    "a product photo of groceries snacks cereal or beverages",
    "a product photo of pet food toys or accessories",
    "a product photo of books stationery or office supplies",
    "a product photo of tools hardware or construction materials",
    "a product photo of car parts automotive accessories or tires",
    "a product photo of sports equipment gym gear or bicycles",
    "a product photo of toys board games or puzzles",
    "a product photo of medical supplies vitamins or supplements",
    # Reinforce Layer-1 keyword categories that previously had no image backstop.
    "a product photo of skincare lotion balm serum tanner or cosmetics",
    "a product photo of shampoo conditioner soap or hair products",
    "a product photo of a tumbler water bottle mug or drinkware",
    "a product photo of a scrunchie hair tie or hair clip",
    "a product photo of a diffuser candle or home fragrance",
    "a product photo of a mirror tray sponge or pouch",
    # Reinforce the cosmetics / body-care / shoe-care categories that slipped
    # through (nail polish, nipple covers, insoles, etc.).
    "a product photo of nail polish nail lacquer or a manicure",
    "a product photo of makeup lipstick mascara eyeliner or eyeshadow",
    "a product photo of nipple covers breast petals or pasties",
    "a product photo of shoe insoles shoelaces or shoe-care products",
    "a product photo of a cosmetic bag toiletry bag or makeup pouch",
    "a product photo of a doll teddy bear plush or toy",
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


def _clip_logit_scale(model: object) -> float:
    logit_scale = getattr(model, "logit_scale", None)
    if logit_scale is None:
        return 100.0

    try:
        scale_value = logit_scale.exp() if hasattr(logit_scale, "exp") else logit_scale
        if hasattr(scale_value, "item"):
            scale_value = scale_value.item()
        scale = float(scale_value)
    except Exception:
        return 100.0

    if scale <= 0:
        return 100.0
    return scale


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
    del img  # full-res RGB array not needed past the grayscale conversion

    h, w = gray.shape[:2]
    # Use float32 (not float64) for these full-res derivative arrays: it halves
    # the per-image working set (the dominant memory cost at high resolution),
    # and the variance / threshold values are unchanged within float32 precision.
    laplacian = cv2.Laplacian(gray, cv2.CV_32F)
    blur_score_global = float(laplacian.var())

    grad_x = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    grad_y = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    gradient_mag = cv2.magnitude(grad_x, grad_y)
    del grad_x, grad_y  # only needed to compute gradient_mag

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
# Serializes actual model inference (encode_image/encode_text). The GPU/MPS
# processes one batch at a time anyway, and serializing avoids MPS thread-safety
# issues while letting download/decode/blur still run concurrently in workers.
_CLIP_INFER_LOCK = threading.Lock()
_CLIP_MODEL = None
_CLIP_PREPROCESS = None
_CLIP_TOKENIZER = None
_CLIP_DEVICE = "cpu"
_INFER_SINCE_EMPTY = 0


def _maybe_empty_device_cache(every: int = 24) -> None:
    """Periodically release cached device memory. The MPS/CUDA allocator keeps
    freed blocks in its own pool rather than returning them to the OS, so without
    this the resident set climbs across thousands of inferences and forces swap on
    small-RAM machines. Batched every ``every`` calls to keep the sync cost low."""
    global _INFER_SINCE_EMPTY
    _INFER_SINCE_EMPTY += 1
    if _INFER_SINCE_EMPTY < every:
        return
    _INFER_SINCE_EMPTY = 0
    try:
        import torch  # torch is imported lazily throughout this module
        if _CLIP_DEVICE == "mps":
            torch.mps.empty_cache()
        elif _CLIP_DEVICE == "cuda":
            torch.cuda.empty_cache()
    except Exception:  # noqa: BLE001 - cache clearing is best-effort
        pass
_CLIP_TEXT_PROMPTS: Optional[list[str]] = None
_CLIP_TEXT_FEATURES = None


# --- Dynamic micro-batching for CLIP image scoring ---------------------------
# Serialized single-image inference underutilizes the GPU and is the Phase-2
# throughput bottleneck. A single batcher thread pulls preprocessed image tensors
# from the concurrent validation workers, runs encode_image + the prompt softmax
# on a BATCH, and hands each image back its prompt-probability list (on the host).
# Every device op runs on this one thread, which also sidesteps MPS thread-safety;
# workers only do CPU preprocess + read back a Python list.
_ENCODE_QUEUE: "_queue.Queue" = _queue.Queue()
_BATCHER_THREAD = None
_BATCHER_LOCK = threading.Lock()
_ENCODE_BATCH_SIZE = 16
_ENCODE_BATCH_WAIT_S = 0.03


class _ScoreReq:
    __slots__ = ("tensor", "event", "probs", "error")

    def __init__(self, tensor) -> None:
        self.tensor = tensor
        self.event = threading.Event()
        self.probs: Optional[list[float]] = None
        self.error: Optional[BaseException] = None


def _ensure_batcher() -> None:
    global _BATCHER_THREAD
    if _BATCHER_THREAD is not None:
        return
    with _BATCHER_LOCK:
        if _BATCHER_THREAD is not None:
            return
        t = threading.Thread(target=_batcher_loop, name="clip-batcher", daemon=True)
        t.start()
        _BATCHER_THREAD = t


def _batcher_loop() -> None:
    import time as _time
    while True:
        reqs = [_ENCODE_QUEUE.get()]
        deadline = _time.monotonic() + _ENCODE_BATCH_WAIT_S
        while len(reqs) < _ENCODE_BATCH_SIZE:
            remaining = deadline - _time.monotonic()
            if remaining <= 0:
                break
            try:
                reqs.append(_ENCODE_QUEUE.get(timeout=remaining))
            except _queue.Empty:
                break
        try:
            import torch  # resolve current torch (real in prod, mock under tests)
            model = _get_clip()[0]
            text_features = _CLIP_TEXT_FEATURES
            batch = torch.cat([r.tensor for r in reqs], dim=0)
            with torch.no_grad():
                feats = model.encode_image(batch)
                feats = feats / feats.norm(dim=-1, keepdim=True)
                logit_scale = _clip_logit_scale(model)
                logits = logit_scale * (feats @ text_features.T)
                probs = torch.softmax(logits, dim=-1)
            probs_cpu = probs.detach().to("cpu").tolist()
            for r, row in zip(reqs, probs_cpu):
                r.probs = row
                r.event.set()
            del feats, logits, probs, batch
            _maybe_empty_device_cache()
        except Exception as exc:  # noqa: BLE001 - propagate to every waiter
            for r in reqs:
                r.error = exc
                r.event.set()


def _score_image_batched(image_input) -> list[float]:
    """Submit one preprocessed image tensor to the batcher and block for its
    prompt-probability list."""
    _ensure_batcher()
    req = _ScoreReq(image_input)
    _ENCODE_QUEUE.put(req)
    req.event.wait()
    if req.error is not None:
        raise req.error
    return req.probs or []


def _get_clip():
    global _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_TOKENIZER, _CLIP_DEVICE
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

        # Resolve the configured encoder (defaults to Marqo-FashionSigLIP via
        # open_clip hf-hub). This will download weights the first time if not
        # already cached.
        from aisley_scraper.config import get_settings

        settings = get_settings()
        model_name = (settings.clip_model_name or "").strip() or "hf-hub:Marqo/marqo-fashionSigLIP"
        pretrained = (settings.clip_pretrained or "").strip()
        if pretrained:
            model, _, preprocess = open_clip.create_model_and_transforms(
                model_name, pretrained=pretrained
            )
        else:
            # hf-hub checkpoints bundle their weights; no separate pretrained tag.
            model, _, preprocess = open_clip.create_model_and_transforms(model_name)
        tokenizer = open_clip.get_tokenizer(model_name)

        # Run inference on the fastest available device: Apple-Silicon GPU (MPS,
        # which uses unified memory so it adds no extra RAM), else CUDA, else CPU.
        # Fall back to CPU if moving the model fails.
        device = "cpu"
        try:
            if torch.backends.mps.is_available():
                device = "mps"
            elif torch.cuda.is_available():
                device = "cuda"
        except Exception:
            device = "cpu"
        if device != "cpu":
            try:
                model = model.to(device)
            except Exception as exc:
                logger.warning("CLIP: could not use device=%s (%s); falling back to CPU", device, exc)
                device = "cpu"
                model = model.to("cpu")

        model.eval()
        _CLIP_MODEL = model
        _CLIP_PREPROCESS = preprocess
        _CLIP_TOKENIZER = tokenizer
        _CLIP_DEVICE = device
        logger.info("CLIP model loaded on device=%s", device)
        return _CLIP_MODEL, _CLIP_PREPROCESS, _CLIP_TOKENIZER


def warmup_clip(*, strict: bool = True) -> None:
    """Load CLIP weights and precompute prompt embeddings.

    This avoids startup latency on the first upload and reduces per-request compute
    (text prompt encoding is cached).
    """

    global _CLIP_TEXT_PROMPTS, _CLIP_TEXT_FEATURES

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
            # Still proceed to the dummy forward pass below.
            pass
        text_input = tokenizer(prompts).to(_CLIP_DEVICE)
        with torch.no_grad():
            text_features = model.encode_text(text_input)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        _CLIP_TEXT_PROMPTS = prompts
        _CLIP_TEXT_FEATURES = text_features

    # Dummy forward pass to reduce first-request latency (torch/OpenCLIP can do lazy init).
    try:
        from PIL import Image

        dummy = Image.new("RGB", (224, 224), color=(255, 255, 255))
        image_input = _CLIP_PREPROCESS(dummy).unsqueeze(0).to(_CLIP_DEVICE)
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

    image_input = preprocess(pil_img.convert("RGB")).unsqueeze(0).to(_CLIP_DEVICE)

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
        text_input = tokenizer(prompts).to(_CLIP_DEVICE)
        with torch.no_grad(), _CLIP_INFER_LOCK:
            text_features = model.encode_text(text_input)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
    else:
        text_features = cached_features

    # Make the cached globals reflect the prompts the batcher will score against
    # (the batcher thread reads _CLIP_TEXT_FEATURES).
    _CLIP_TEXT_PROMPTS = prompts
    _CLIP_TEXT_FEATURES = text_features

    # Batched inference: a single batcher thread runs encode_image + the prompt
    # softmax across many images at once (large throughput win) and returns this
    # image's prompt-probability list on the host — no per-image device sync, and
    # all MPS/CUDA work stays on that one thread.
    probs_list = _score_image_batched(image_input)

    best_positive_idx = max(range(positive_count), key=lambda i: probs_list[i])
    best_negative_idx = positive_count + max(
        range(len(probs_list) - positive_count),
        key=lambda i: probs_list[positive_count + i],
    )
    best_idx = max(range(len(probs_list)), key=lambda i: probs_list[i])

    # Binary score: ratio of best positive prompt probability to best negative.
    # This is more robust than averaging class feature embeddings, because it is
    # not fooled by dark/moody images matching an unrelated negative prompt
    # (e.g. "a meme") when CLIP's dominant match is clearly a positive prompt.
    max_positive_prob = float(probs_list[best_positive_idx])
    max_negative_prob = float(probs_list[best_negative_idx])
    # Equivalent to sigmoid(logit_scale * (cos_pos - cos_neg)): the shared softmax
    # denominator cancels, so this is a calibrated-by-logit_scale margin between
    # the best positive and best negative prompt. softmax outputs are strictly
    # > 0, so denom is always > 0; fall back to 0.0 (fail-safe drop) defensively.
    denom = max_positive_prob + max_negative_prob
    product_prob = (max_positive_prob / denom) if denom > 0 else 0.0
    non_product_prob = 1.0 - product_prob
    return {
        "product_prob": product_prob,
        "non_product_prob": non_product_prob,
        "best_positive_prompt": prompts[best_positive_idx],
        "best_negative_prompt": prompts[best_negative_idx],
        "best_prompt": prompts[best_idx],
        "class_probs": {
            "product": product_prob,
            "non_product": non_product_prob,
        },
        "probs": {prompts[i]: float(probs_list[i]) for i in range(len(prompts))},
    }


def validate_and_normalize_upload(
    *,
    content: bytes,
    filename: str,
    min_width: int = REQUIRED_MIN_WIDTH,
    min_height: int = REQUIRED_MIN_HEIGHT,
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
            message=f"Image must be smaller than {MAX_IMAGE_BYTES // (1024 * 1024)}MB.",
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
                message=f"Image must be smaller than {MAX_IMAGE_BYTES // (1024 * 1024)}MB.",
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
    if w < min_width or h < min_height:
        raise ImageValidationFailure(
            code="resolution_too_low",
            message=f"Image resolution is too low. Minimum is {min_width}x{min_height} pixels.",
            details={
                "width": w,
                "height": h,
                "min_width": min_width,
                "min_height": min_height,
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


def validate_product_photo_only(
    *,
    content: bytes,
    filename: str,
    min_product_prob: float = MIN_PRODUCT_PROB,
    min_width: int = REQUIRED_MIN_WIDTH,
    min_height: int = REQUIRED_MIN_HEIGHT,
    check_quality: bool = True,
) -> dict[str, Any]:
    """Validate only whether an image appears to be a product photo.

    This is intentionally lighter than full image validation and is meant for
    phase-2 first-image gating. When ``check_quality`` is set it also applies a
    lightweight size + blur gate (the contrast/brightness gates are skipped, as
    those false-reject valid white-background flat-lays).
    """
    if not content:
        raise ImageValidationFailure(code="empty_file", message="Upload is empty.")

    if len(content) > MAX_IMAGE_BYTES:
        raise ImageValidationFailure(
            code="file_too_large",
            message=f"Image must be smaller than {MAX_IMAGE_BYTES // (1024 * 1024)}MB.",
            details={"max_bytes": MAX_IMAGE_BYTES, "actual_bytes": len(content)},
        )

    safe_name = _safe_basename(filename)
    _ = safe_name
    pil_img = _open_pil_image(content)

    if check_quality:
        width, height = int(getattr(pil_img, "width", 0)), int(getattr(pil_img, "height", 0))
        if width < min_width or height < min_height:
            raise ImageValidationFailure(
                code="resolution_too_low",
                message=f"Image resolution is too low. Minimum is {min_width}x{min_height} pixels.",
                details={"width": width, "height": height, "min_width": min_width, "min_height": min_height},
            )
        quality = assess_image_quality(pil_img)
        if bool(quality.get("is_blurry", False)):
            raise ImageValidationFailure(
                code="image_too_blurry",
                message="Image is too blurry. Please upload a sharper photo.",
                details={
                    "blur_score": quality.get("blur_score"),
                    "min_blur_score": quality.get("adaptive_min_blur_score", MIN_BLUR_SCORE),
                },
            )

    product = product_probability_clip(pil_img)
    product_prob = float(product.get("product_prob", 0.0))
    threshold = float(min_product_prob)
    if product_prob < threshold:
        raise ImageValidationFailure(
            code="not_a_product_photo",
            message="Image does not look like a product photo.",
            details={
                "product_prob": product_prob,
                "min_product_prob": threshold,
                "best_prompt": product.get("best_prompt"),
                "probs": product.get("probs"),
            },
        )

    return {
        "ok": True,
        "product": product,
        "min_product_prob": threshold,
    }
