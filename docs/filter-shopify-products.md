# Filter Shopify Products by First-Image Product Validation

## Purpose

The `filter-shopify-products` command is a standalone cleanup command.
It is independent from scraping and Phase 2 processing.

The command scans existing rows in `shopify_products`, validates the first image per product using the same product-photo classifier used by first-image Phase 2 mode, and removes low-confidence products.

## What Gets Filtered

A `shopify_products` row is considered for validation only when:

- The row has at least one source image (`images <> []`)
- The row `item_uuid` exists in `item_embeddings`

Rows that do not match both conditions are ignored by this command.

## Validation Rule

For each scanned row:

1. The first non-empty image URL from `images` is fetched.
2. `validate_product_photo_only` is executed with threshold from:
   - `PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD`
3. If the result is below threshold (`not_a_product_photo`), the row is filtered out.

Transient issues (timeout, fetch failure, temporary errors) are preserved and not deleted.

## Delete Behavior

When a row is filtered out (not dry run):

1. Delete row from `shopify_products` by `(store_id, product_id)`
2. Delete matching rows from `item_embeddings` by `item_uuid`

`item_embeddings` deletion is deduplicated per batch by UUID to avoid repeated delete calls.

## CLI Usage

Run deletion:

```bash
aisley-scraper filter-shopify-products
```

Preview only (no delete):

```bash
aisley-scraper filter-shopify-products --dry-run
```

Limit scanned rows:

```bash
aisley-scraper filter-shopify-products --limit 1000
```

Tune scan batch size:

```bash
aisley-scraper filter-shopify-products --batch-size 200
```

## Notes

- This command does not modify Phase 2 crawl flow.
- It uses keyset scan pagination (`id > after_id`) for stable scanning while deleting.
- Threshold is shared with first-image mode so behavior is consistent across workflows.
