"""Tests for _merge_missing_rate_lines — the shipping-rate-table harvest fix.

The extractor keeps only the single best-scoring shipping page, so a concrete
rate table on a lower-scoring candidate (marinelayer's Standard $5 / Expedited
$20 on /pages/shipping-info) was dropped. This grafts those cost rows back on,
while ignoring free-shipping thresholds ("free over $150") which are not costs.
"""
from aisley_scraper.extract.policies import _merge_missing_rate_lines

RATE_PAGE = (
    "Shipping Info Standard USPS (estimated delivery in 3-7 business days) $5 "
    "Free shipping on $75+ orders Expedited UPS (2-3 business days) $20 "
    "Express UPS (1-2 business days) $35 International DHL $35"
)


def test_appends_missing_rate_table():
    best = "Orders ship in 1-2 days. Free shipping on orders $75+."
    out = _merge_missing_rate_lines(best, [best, RATE_PAGE])
    assert out.startswith(best)
    assert "Shipping rates:" in out
    assert "$5" in out and "$20" in out            # the real domestic rates
    assert "Standard USPS" in out and "Expedited UPS" in out


def test_skips_free_threshold_not_a_rate():
    best = "Free shipping on orders over $50."
    page = "We offer free express shipping on all orders over $150 worldwide."
    out = _merge_missing_rate_lines(best, [best, page])
    assert out == best                              # $150 is a threshold, not a cost


def test_skips_amount_already_in_best_text():
    best = "Express UPS shipping is $35."
    page = "Express UPS (1-2 days) $35"
    out = _merge_missing_rate_lines(best, [best, page])
    assert out == best                              # $35 already represented


def test_skips_zero_and_no_rows_returns_unchanged():
    best = "Standard shipping is free."
    page = "Standard shipping $0.00 for all orders."
    out = _merge_missing_rate_lines(best, [best, page])
    assert out == best


def test_dollar_plus_orders_threshold_skipped():
    best = "Ships in 2 days."
    page = "Standard ground shipping $9 or free on $99+ orders via UPS."
    out = _merge_missing_rate_lines(best, [page])
    # $9 is a real rate (kept); $99+ is a threshold (skipped)
    assert "$9" in out and "$99" not in out
