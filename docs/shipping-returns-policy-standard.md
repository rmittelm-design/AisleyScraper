# Keeping shipping/returns policies at standard

Every store in `shopify_stores` should carry a `shipping_returns` value that is a **real, complete** shipping-and-returns policy — not navigation boilerplate, not a half-policy that dropped the costs. This doc defines what "at standard" means and the repeatable loop for getting there and staying there.

Policies live in two columns:

- `shopify_stores.shipping_returns` — the policy text in labelled section(s): normally a `RETURNS:` section and a `SHIPPING:` section, but when both policies come from the same combined page with identical text it is emitted as one `SHIPPING & RETURNS:` section (and only one section appears when just one category is found).
- `shopify_stores.shipping_returns_url` — the source page URL(s), ` | `-joined when there are two (a single URL when the sections share one page).

The automated capture path is described in the README (Shopify `/policies/*`, common `/pages/*`, combined pages, then matching homepage links). This doc covers the **quality bar** on top of that path and the three-tier maintenance loop.

---

## What "at standard" means

**1. Both concerns present, correctly labelled.** The value must cover both returns and shipping — normally as a `RETURNS:` block and a `SHIPPING:` block (or a single `SHIPPING & RETURNS:` block when both come from one combined page). A store that only sells online still has shipping terms; a store with "all sales final" still states that under returns.

**2. It is an actual policy, not chrome.** It must not be menu/navigation text, a cookie banner, or a privacy/terms page. The `stored_policy_is_weak()` gate in [`extract/policies.py`](../src/aisley_scraper/extract/policies.py) is the machine definition of "boilerplate": NULL/empty, a legal (privacy/T&C) page, text that does not read like a policy, or long text with low policy-signal density that leads with a navigation menu. Anything `stored_policy_is_weak()` flags is below standard by definition.

**3. It carries the concrete facets, not just prose.** The single most common defect is a policy that reads fine but silently dropped the numbers — the rate table, the restocking fee, the return window. Use this checklist. A policy is complete when it states every facet the store actually publishes (omit only facets the store genuinely doesn't have):

| `RETURNS:` facets | `SHIPPING:` facets |
| --- | --- |
| Return window (days) and when the clock starts (delivery / purchase / ship date) | Domestic rate table: each method → its cost |
| How to start a return (portal URL, email, or included form); label-validity window | Free-shipping threshold (e.g. free over $150) |
| Return-shipping cost or **restocking fee**; whether exchanges are free | Processing/handling time and order cutoff (e.g. before 3 pm ships same day) |
| Refund method (original payment / store credit / gift card) and processing time | Delivery estimate per service level |
| Exchange terms (one exchange only? online-only? in-store?) | Carriers (FedEx / UPS / USPS / DHL) |
| In-store return option, if any | Expedited options and any oversized/surcharge rules |
| **Final-sale / non-returnable exclusions** (sale, monogrammed, bodysuits, vintage, etc.) | International: rates, delivery windows, customs/duties responsibility, export fees, PayPal-only, address exclusions (PO box / APO-FPO) |

**4. Length is a smell test, not the target.** Established stores (`scraped = true`) average **~2,400 characters**. Completeness is the goal, not length — but a policy far below **~1,200 characters** is almost always missing facets from the checklist above and should be re-audited before you accept it. (Genuinely terse policies exist — "all sales final; ships in 2–3 days" — and are fine. Judge by the checklist, not the byte count.)

---

## The three-tier maintenance loop

Run the cheapest tier that fixes the problem; escalate only for what it can't reach.

### Tier 1 — Automated recapture (`recapture-policies`)

Re-fetches each store's policy and rewrites `shipping_returns` / `shipping_returns_url`. No product re-scrape. This repairs NULL rows and rows whose stored text is boilerplate.

```bash
# See what would change, write nothing:
aisley-scraper recapture-policies --dry-run

# Repair only NULL / boilerplate rows; leave already-good policies untouched:
aisley-scraper recapture-policies --only-broken

# One store, scheme/www-insensitive; every other store untouched:
aisley-scraper recapture-policies --domain marinelayer.com

# NULL out rows that are boilerplate AND could not be re-fetched (default: leave unchanged):
aisley-scraper recapture-policies --only-broken --clear-unfixable

# Cap the batch:
aisley-scraper recapture-policies --only-broken --limit 50
```

The extractor also harvests **rate tables** across the scored shipping-candidate pages (`_merge_missing_rate_lines` in `extract/policies.py` — those that passed the policy-phrasing and legal-page gates, capped at `_MAX_CANDIDATES_TO_SCORE`), so a shipping cost published on a candidate page other than the one chosen as "best" is merged back in rather than dropped. If a store shows prose but no cost, this is the code that should have caught it — check that path first.

**When Tier 1 is enough:** the store publishes its policy on a plain, fetchable page and the only problem is that the DB row is NULL, boilerplate, or a stale earlier capture.

### Tier 2 — LLM audit + verify completeness sweep

Tier 1 gets a *real* policy into the row; it does **not** guarantee the row has every facet. A structural/chrome check cannot see that a policy is missing the restocking fee or the final-sale list — for that you must compare the stored text against the store's **live pages**. This is the gold-standard completeness pass, and it is a two-stage workflow:

1. **Audit** — for each store, an agent fetches the store's live shipping/returns pages and lists the facets present live but missing from (or contradicted by) the stored `shipping_returns`.
2. **Verify (adversarial, independent)** — a *second* agent re-fetches the same pages and confirms each proposed addition is actually on the page, discarding anything it can't ground. Only verified additions are written.

The verify stage is **not optional**. In the last full sweep it removed hallucinated facts from ~40% of the enriched policies (fabricated "return in boutique" options, invented DDP/customs claims), and it caught Shopify serving another store's mis-cached content entirely. Never write audit output straight to the DB — always run it through independent verification first.

Author this as a `pipeline(stores, audit, verify)` workflow (see the workflow-authoring reference). Feed it the stores whose policies are short or whose facet checklist looks thin, not the whole table.

**When Tier 2 is needed:** the row has a plausible policy but you want to *guarantee* facet parity with the established stores — e.g. after adding a batch of new stores, or when a store is below the ~1,200-char smell line.

### Tier 3 — Manual capture for bot-blocked stores

Some stores can't be fetched by either tier: aggressive rate-limiting (HTTP 429) or JS-rendered help centers (e.g. Gorgias, Zendesk `*/help` pages) that return no policy text to a plain fetch. For these:

1. Get the live policy from the store's own URLs or from screenshots of the rendered pages.
2. Transcribe into the same `RETURNS:` / `SHIPPING:` format, covering the facet checklist.
3. Record the real source URLs in `shipping_returns_url`.
4. Where the page is reachable at all, still run the Tier-2 verify stance — only write facets you can see on the source.

Overland is the worked example: its help center was unreachable to the extractor, so its full rate tables (domestic + international), rug surcharges, fur-ban and PayPal-only terms were transcribed from the live Shipping & Tax Rates and Returns & Exchanges pages (617 → 3,675 chars).

**When Tier 3 is needed:** Tiers 1–2 both come back empty or degraded for a specific domain and you have the live pages another way.

---

## Safety rails

- **Never overwrite a good DB value with a degraded re-fetch.** A re-fetch that gets rate-limited comes back thin — a harness/audit "FAIL" means "couldn't re-verify from the network right now," **not** "the stored value is wrong." Read the current DB value first; only overwrite when the new text is genuinely richer. When in doubt, `--dry-run` and diff.
- **Fill-NULL-only by default.** When bulk-populating, scope writes to `where shipping_returns is null or shipping_returns = ''` unless you are *deliberately* enriching an existing value you've compared against.
- **Dry-run before you write.** Every ad-hoc apply script should default to dry-run and require an explicit `--execute`.
- **Destructive-op policy still applies.** No DB writes, `--execute`, or `--clear-unfixable` runs without explicit per-run sign-off.

---

## Failure modes seen (and the guards)

These are real ways policies silently fell below standard. Each has a cheap guard — apply them.

- **Silent 0-row writes.** `UPDATE shopify_stores SET shipping_returns=… WHERE website ILIKE %needle%` hits **0 rows with no error** when the store has no matching row (0 rows in the table) or a scraped-flag/URL mismatch. The apply is reported "done" but nothing persists. Several manual policies (colbo, marni, edithmachinist, aquelarre, jmclaughlin) were lost this way. **Guard:** assert `cur.rowcount > 0` on every targeted policy write; roll back and surface if 0. Then re-read the row to confirm.
- **Manual/screenshot captures are unreliable.** They omit facets and *fabricate by conflation* — colbo's stored value claimed free returns (live has a **$10 restocking fee**) and invented "free international over $500" by merging the homepage promo banner with the shipping page (a figure on neither source). **Guard:** never accept a manual capture as final; verify every fact against the store's live `/policies/*` pages, and attribute each figure to the exact page it appears on (banner ≠ policy).
- **Wrong URL hides the real store.** colbo was known as the apex `colbo.nyc` (404/bot-blocked); the real Shopify store is `shop.colbo.nyc`. Every automated fetch hit the dead URL and kept the thin manual version. **Guard:** when a domain 404s/bot-blocks, try `shop.<domain>` and other subdomains (and confirm `…/products.json` returns real JSON) before declaring it unreachable.
- **Audit scope blind spot.** A completeness sweep that only scores stores which *have* a row + policy + reachable URL silently skips the ones most likely wrong — 0-row stores and wrong-URL stores. **Guard:** the sweep's target set must be built from a completeness critic ("which stores are missing entirely, keyed by a dead URL, or below the length gate?"), not just "stores that already have a policy."

---

## Acceptance checks

After any policy work, confirm the batch is at standard:

```bash
# Coverage: sites still missing a policy (should be only genuinely-no-policy stores)
python - <<'PY'
from aisley_scraper.config import Settings
from aisley_scraper.db.repository import Repository
conn = Repository(Settings())._connect(); conn.autocommit = True
cur = conn.cursor(); cur.execute("set statement_timeout=30000")
cur.execute("select count(distinct website) from shopify_stores where shipping_returns is null or shipping_returns=''")
print("sites with NULL/empty policy:", cur.fetchone()[0])
# Length parity vs established stores
cur.execute("select round(avg(length(shipping_returns))) from shopify_stores where scraped=true and shipping_returns is not null")
print("established-store avg length:", cur.fetchone()[0])
cur.execute("select round(avg(length(shipping_returns))) from shopify_stores where scraped=false and shipping_returns is not null")
print("new-store avg length:", cur.fetchone()[0])
# Below-smell-line rows to re-audit
cur.execute("select website, length(shipping_returns) from shopify_stores where shipping_returns is not null and length(shipping_returns) < 1200 order by 2 limit 20")
print("shortest policies (re-audit candidates):")
for w, n in cur.fetchall(): print(f"  {n:5}  {w}")
conn.close()
PY
```

A batch is at standard when: every reachable store has a non-NULL policy (NULL only for stores that genuinely publish none), the new-store average length is within range of the established-store average, and the shortest policies have been checked against the facet checklist (short-because-terse, not short-because-incomplete).
