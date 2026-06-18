from aisley_scraper.models import ProductRecord
from aisley_scraper.normalize.products import enforce_attribute_policy, normalize_product, should_exclude_product


def test_policy_clears_attributes_without_images() -> None:
    p = ProductRecord(
        product_id="1",
        product_handle="h",
        item_name="Item",
        description=None,
        images=[],
        sizes=["M"],
        colors=["Red"],
        brand="BrandX",
        raw={"vendor": "BrandX", "options": [{"name": "Size", "values": ["M"]}]},
    )

    out = enforce_attribute_policy(p)
    assert out.sizes == []
    assert out.colors == []
    assert out.brand is None


def test_policy_keeps_attributes_with_images_and_explicit_source() -> None:
    p = ProductRecord(
        product_id="2",
        product_handle="h2",
        item_name="Item 2",
        description=None,
        images=["https://example.com/x.jpg"],
        sizes=["L"],
        colors=["Blue"],
        brand="BrandY",
        raw={"vendor": "BrandY", "options": [{"name": "Color", "values": ["Blue"]}]},
    )

    out = enforce_attribute_policy(p)
    assert out.sizes == ["L"]
    assert out.colors == ["Blue"]
    assert out.brand == "BrandY"


def test_should_exclude_cosmetics_product_type() -> None:
    p = ProductRecord(
        product_id="3",
        product_handle="h3",
        item_name="Lip Gloss",
        description=None,
        images=[f"https://example.com/gloss{i}.jpg" for i in range(4)],
        product_type="cosmetics",
    )

    assert should_exclude_product(p) is True
    assert normalize_product(p) is None


def test_normalize_product_keeps_non_cosmetics() -> None:
    p = ProductRecord(
        product_id="4",
        product_handle="h4",
        item_name="Dress",
        description=None,
        images=[f"https://example.com/dress{i}.jpg" for i in range(4)],
        product_type="dresses",
        brand="BrandZ",
        raw={"vendor": "BrandZ"},
    )

    out = normalize_product(p)
    assert out is not None
    assert out.product_id == "4"


def test_should_exclude_items_with_fewer_than_three_images() -> None:
    for n in (0, 1, 2):
        p = _p("Plain Dress")
        p.images = [f"https://example.com/{i}.jpg" for i in range(n)]
        assert should_exclude_product(p) is True, f"{n} images should be excluded"
        assert normalize_product(p) is None
    # Exactly 3 images is kept.
    p3 = _p("Plain Dress")
    p3.images = [f"https://example.com/{i}.jpg" for i in range(3)]
    assert should_exclude_product(p3) is False
    assert normalize_product(p3) is not None


_FOUR_IMAGES = [f"https://example.com/{i}.jpg" for i in range(4)]


def _p(name="", *, url=None, handle=None, ptype=None) -> ProductRecord:
    return ProductRecord(
        product_id="x",
        product_handle=handle,
        item_name=name,
        description=None,
        images=list(_FOUR_IMAGES),
        product_url=url,
        product_type=ptype,
    )


# Kids/children — aggressive substring (name/url/handle).
def test_should_exclude_kids_items() -> None:
    assert should_exclude_product(_p("Boys Graphic Tee")) is True
    assert should_exclude_product(_p("Toddler Romper")) is True
    assert should_exclude_product(_p("Dress", url="https://x.com/collections/kids/abc")) is True
    # Known intentional aggressive casualties:
    assert should_exclude_product(_p("Boyfriend Jeans")) is True
    assert should_exclude_product(_p("Babydoll Dress")) is True


# Non-apparel categories — whole-word / phrase match (name/url/handle/type).
def test_should_exclude_non_apparel_categories() -> None:
    drops = [
        _p("Oak Dining Chair", ptype="Furniture"),
        _p("Storage Boxes"),
        _p("Gift Box"),  # singular box also matched
        _p("Dog Petwear Coat", ptype="Pet"),
        _p("Cat Bed", url="https://x.com/collections/pet"),
        _p("$50 Gift Card"),
        _p("Greeting Cards"),
        _p("1000pc Puzzle", ptype="Puzzles"),
        _p("Bar Goodies Kit"),
        _p("Soy Candle", ptype="Candles"),
        _p("Brass Lighter"),
        _p("Leather Catchall"),
        _p("Coffee Table Book"),
        _p("Hardcover Book", ptype="Books"),
        _p("Ceramic Vase", ptype="Home Decor"),
        _p("Wall Décor Print"),
        _p("Picture Frame 5x7"),
        _p("Ceramic Serveware Platter"),
        _p("Lavender Soap Bar", ptype="Soaps"),
        _p("Reed Diffuser"),
        _p("Linen Tea Towels"),
        _p("Bath Towel", ptype="Towels"),
        _p("Sundries Pouch", ptype="Sundries"),
        # Newly added categories (singular + plural).
        _p("Foil Stationery Set"),
        _p("Cardstock Stationary"),
        _p("Scented Home Diffuser", ptype="Home"),
        _p("Drink Can Cooler"),
        _p("Pint Glass"),
        _p("Highball Glasses"),
        _p("Insulated Tumbler"),
        _p("Ceramic Mug", ptype="Mugs"),
        _p("Holiday Gift Set"),
        _p("Espresso Cup"),
        _p("Canvas Pouch", ptype="Pouches"),
        _p("Gradual Self Tanner"),
        _p("Round Wall Mirror"),
        _p("Eau de Perfume"),
        _p("Pearl Hair Pin"),
        _p("Enamel Hairpins"),
        _p("Rose Perfume Rollerball"),
        _p("Silk Scrunchies"),
        _p("Velvet Scrunchie"),
        _p("Nourishing Face Oil", ptype="Oils"),
        _p("Konjac Sponge"),
        _p("Gentle Cleanser", ptype="Cleansers"),
        _p("Natural Deodorant"),
        _p("Lip Balm"),
        _p("Hydrating Conditioner"),
        _p("Volumizing Shampoo"),
        _p("Vitamin C Serum"),
    ]
    for product in drops:
        assert should_exclude_product(product) is True, product.item_name


# Apparel whose names contain the category substrings must be KEPT
# (word-boundary matching, not substring).
def test_category_filter_preserves_apparel_collisions() -> None:
    keeps = [
        _p("Spring Floral Dress", ptype="Dresses"),    # 'ring' in 'spring'
        _p("Cropped Cardigan", ptype="Knitwear"),       # 'card' in 'cardigan'
        _p("Petite Linen Trousers"),                     # 'pet' in 'petite'
        _p("Boxy Oversized Tee"),                        # 'box' in 'boxy' (boundary-safe)
        _p("Boxer-stripe Shirt"),                        # 'box' in 'boxer' (boundary-safe)
        _p("Toweling Beach Robe"),                       # 'towel' in 'toweling'
        _p("Herringbone Blazer", ptype="Blazers"),       # 'ring' in 'herringbone'
        _p("Drawstring Joggers"),                        # 'ring'/'string'
        _p("Cupro Slip Dress"),                          # 'cup' in 'cupro'
        _p("Homecoming Gown", ptype="Dresses"),          # 'home' in 'homecoming'
        _p("Canvas Tote Jacket"),                        # 'can' in 'canvas'
        _p("Hourglass Belt"),                            # 'glass' in 'hourglass'
        _p("Cancan Ruffle Dress"),                       # 'can' in 'cancan'
        _p("Scrunch Bikini Bottom"),                     # 'scrunch' detail, not 'scrunchie'
        _p("Ruched Scrunch Leggings"),                   # 'scrunch' detail
        _p("Oilskin Rain Jacket"),                       # 'oil' in 'oilskin'
        _p("Pinstripe Tailored Blazer"),                 # bare 'pin' not matched
        _p("Pintuck Poplin Blouse"),                     # bare 'pin' not matched
        _p("Mirrored Sequin Mini Dress"),               # 'mirror' in 'mirrored'
        # Jewelry & watches are intentionally KEPT (not filtered).
        _p("Gold Signet Ring", ptype="Jewelry"),
        _p("Pearl Necklace"),
        _p("Hoop Earrings"),
        _p("Tennis Bracelet"),
        _p("Leather Watch", ptype="Watches"),
        _p("Diamond Pendant Necklace"),
    ]
    for product in keeps:
        assert should_exclude_product(product) is False, product.item_name
