"""Quick live test of the binary CLIP classifier against known product/non-product images."""
import sys
import httpx
from io import BytesIO
from PIL import Image

from aisley_scraper.image_validation import product_probability_clip

PRODUCT_URLS = [
    # Fashion model shots from a Shopify brand (White Fox Boutique)
    "https://cdn.shopify.com/s/files/1/0263/6018/4892/files/white-fox-rue-de-romance-long-sleeve-jumpsuit-black-30.9.2502.jpg?v=1761199189",
    "https://cdn.shopify.com/s/files/1/0263/6018/4892/files/white-fox-jar-of-hearts-pants-black-lost-in-paris-bustier-30.9.2502.jpg?v=1761279333",
]

NON_PRODUCT_URLS = [
    # Lifestyle / editorial / banner images
    # Products-98_2.jpg from store 8646 is intentionally excluded from PRODUCT_URLS:
    # CLIP correctly classifies it as "a meme/selfie" (score ~0.07) — it is NOT a clean catalog shot.
    "https://cdn.shopify.com/s/files/1/0558/0383/8646/files/Products-98_2.jpg?v=1623696297",  # meme/selfie-like informal shot
    "https://images.unsplash.com/photo-1506794778202-cad84cf45f1d?w=800",  # portrait
    "https://images.unsplash.com/photo-1476514525535-07fb3b4ae5f1?w=800",  # landscape
]

THRESHOLD = 0.65


def score_url(url: str) -> dict | None:
    try:
        r = httpx.get(url, timeout=15, follow_redirects=True)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content))
        return product_probability_clip(img)
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def main() -> int:
    print(f"Threshold: {THRESHOLD}\n")

    print("=== PRODUCT IMAGES (expect score > threshold) ===")
    product_scores = []
    for url in PRODUCT_URLS:
        result = score_url(url)
        if result is not None:
            score = result["product_prob"]
            product_scores.append(score)
            verdict = "PASS" if score >= THRESHOLD else "FAIL"
            print(f"  [{verdict}] score={score:.4f}  {url.split('/')[-1][:55]}")
            print(f"         best_prompt={result['best_prompt']}")
            print(f"         best_pos={result['best_positive_prompt']}")
            print(f"         best_neg={result['best_negative_prompt']}")

    print()
    print("=== NON-PRODUCT IMAGES (expect score < threshold) ===")
    non_product_scores = []
    for url in NON_PRODUCT_URLS:
        result = score_url(url)
        if result is not None:
            score = result["product_prob"]
            non_product_scores.append(score)
            verdict = "PASS" if score < THRESHOLD else "FAIL"
            print(f"  [{verdict}] score={score:.4f}  {url.split('?')[0].split('/')[-1][:55]}")
            print(f"         best_prompt={result['best_prompt']}")
            print(f"         best_pos={result['best_positive_prompt']}")
            print(f"         best_neg={result['best_negative_prompt']}")

    if product_scores:
        print(f"\nProduct image scores: min={min(product_scores):.4f}  max={max(product_scores):.4f}  mean={sum(product_scores)/len(product_scores):.4f}")
    if non_product_scores:
        print(f"Non-product scores:   min={min(non_product_scores):.4f}  max={max(non_product_scores):.4f}  mean={sum(non_product_scores)/len(non_product_scores):.4f}")

    misclassified = sum(1 for s in product_scores if s < THRESHOLD) + sum(1 for s in non_product_scores if s >= THRESHOLD)
    total = len(product_scores) + len(non_product_scores)
    print(f"\nResult: {total - misclassified}/{total} correctly classified at threshold={THRESHOLD}")
    return 0 if misclassified == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
