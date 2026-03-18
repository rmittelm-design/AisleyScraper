"""Inspect image statistics and CLIP per-prompt details for all test URLs."""
import httpx
from io import BytesIO
from PIL import Image
import numpy as np

from aisley_scraper.image_validation import product_probability_clip

ALL_URLS = [
    # Expected product shots
    ("PRODUCT", "https://cdn.shopify.com/s/files/1/0558/0383/8646/files/Products-98_2.jpg?v=1623696297"),
    ("PRODUCT", "https://cdn.shopify.com/s/files/1/0263/6018/4892/files/white-fox-rue-de-romance-long-sleeve-jumpsuit-black-30.9.2502.jpg?v=1761199189"),
    ("PRODUCT", "https://cdn.shopify.com/s/files/1/0263/6018/4892/files/white-fox-jar-of-hearts-pants-black-lost-in-paris-bustier-30.9.2502.jpg?v=1761279333"),
    # Expected non-product shots
    ("NON-PRODUCT", "https://images.unsplash.com/photo-1506794778202-cad84cf45f1d?w=800"),   # portrait
    ("NON-PRODUCT", "https://images.unsplash.com/photo-1476514525535-07fb3b4ae5f1?w=800"),   # landscape
]

for label, url in ALL_URLS:
    name = url.split("/")[-1].split("?")[0][:60]
    print(f"\n{'='*70}")
    print(f"[{label}] {name}")

    r = httpx.get(url, timeout=15, follow_redirects=True)
    r.raise_for_status()
    img = Image.open(BytesIO(r.content))
    arr = np.array(img.convert("RGB"))

    mean_brightness = arr.mean()
    near_white = ((arr > 240).all(axis=2)).mean()
    print(f"  Size: {img.size}  Mean brightness: {mean_brightness:.1f}  Near-white: {near_white:.3f}")

    result = product_probability_clip(img)
    print(f"  product_prob (binary avg): {result['product_prob']:.4f}")
    print(f"  best_prompt (20-way softmax): {result['best_prompt']}")

    # Compute max-positive vs max-negative from per-prompt probs
    from aisley_scraper.image_validation import CLIP_PRODUCT_POSITIVE_PROMPTS, CLIP_PRODUCT_NEGATIVE_PROMPTS
    pos_probs = [result["probs"][p] for p in CLIP_PRODUCT_POSITIVE_PROMPTS]
    neg_probs = [result["probs"][p] for p in CLIP_PRODUCT_NEGATIVE_PROMPTS]
    max_pos = max(pos_probs)
    max_neg = max(neg_probs)
    max_based = max_pos / (max_pos + max_neg) if (max_pos + max_neg) > 0 else 0.5
    print(f"  max_pos={max_pos:.4f} ({CLIP_PRODUCT_POSITIVE_PROMPTS[pos_probs.index(max_pos)]})")
    print(f"  max_neg={max_neg:.4f} ({CLIP_PRODUCT_NEGATIVE_PROMPTS[neg_probs.index(max_neg)]})")
    print(f"  product_prob (max-based): {max_based:.4f}")
