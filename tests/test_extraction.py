"""Offline extraction contracts, including patterns observed across both categories."""

import copy
import json
import logging
from dataclasses import replace
from html import escape

import pytest

from safco_catalog_scraper.extraction import ExtractionError, extract_product, finalize_product
from safco_catalog_scraper.storage import Storage, TaskSpec

PRODUCT_URL = "https://www.safcodental.com/product/unfamiliar-family"
CAPTURED_AT = "2026-10-19T18:00:00+00:00"


def _price(amount):
    return f'<span data-price-type="finalPrice" data-price-amount="{amount}"></span>'


def _html(rows, *, total=None, paginate=False,
          path=("Dental Supplies", "Dental Exam Gloves")):
    """Use Safco's two JSON layers and UI config without copying its full HTML."""
    breadcrumbs = '<nav class="breadcrumbs"><li class="home"><a>Home</a></li>'
    for index, name in enumerate(path):
        breadcrumbs += f'<li class="category{index}"><a>{escape(name)}</a></li>'
    breadcrumbs += '<li class="product">Unfamiliar family</li></nav>'
    config = {"total": len(rows) if total is None else total,
              "paginate": paginate, "page_limit": 20}
    return ('<h1>Unfamiliar family</h1>' + breadcrumbs
            + '<meta property="product:price:currency" content="USD">'
            + '<script>window.masterData = ' + json.dumps(json.dumps(rows)) + ';</script>'
            + '<script>window.productConfig = JSON.parse('
            + json.dumps(json.dumps(config)) + ');</script>')


def _tiers(row, *offers, as_list=False):
    entries = [{"price_qty": str(quantity), "price": price, "website_price": price,
                "all_groups": "1", "percentage_value": None}
               for quantity, price in offers]
    row["tier_price"] = entries if as_list else {str(i + 1): entry
                                               for i, entry in enumerate(entries)}
    row["price"]["tier_price"] = '<ul class="prices-tier">' + "".join(
        f'<li>{quantity} <span data-price-amount="{price}"></span></li>'
        for quantity, price in offers) + '</ul>'


def _codes(result):
    return {issue["code"] for issue in result.issues}


@pytest.fixture
def row():
    return {
        "sku": "000012", "name": "Unfamiliar gloves small 200/box",
        "description": "Small, 200/box",
        "manufacturer_name": "Source manufacturer", "manufacturer_part_number": "0012-A",
        "stock_availability": "in_stock", "product_price": "16.490000",
        "price": {"price": _price("16.49"), "tier_price": False}, "tier_price": [],
        "promo_description": "Buy several, get more free",
    }


@pytest.mark.parametrize("path,item_name,description,pack", [
    (("Dental Supplies", "Dental Exam Gloves", "Nitrile gloves"),
     "Unfamiliar gloves small 200/box", "Small, 200/box", "200/box"),
    (("Dental Supplies", "Sutures & surgical products", "Sutures"),
     'Unfamiliar suture 6-0, 18&quot;, 12/box', '6-0, 18&quot;, 12/box', "12/box"),
])
def test_unfamiliar_families_in_either_category_follow_the_same_contract(
        row, path, item_name, description, pack):
    row.update(name=item_name, description=description)
    source = _html({"000012": row}, path=path)
    result = extract_product(source, PRODUCT_URL, CAPTURED_AT)
    item = result.items[0]

    assert result.status == "complete"
    assert not result.needs_recovery
    assert result.issues == ()
    assert result.expected_item_count == result.decoded_item_count == 1
    assert item["product_name"] == "Unfamiliar family"
    assert item["item_name"] == item_name.replace("&quot;", '"')
    assert item["item_description"] == description.replace("&quot;", '"')
    assert item["selling_unit"] == "box"
    assert item["pack_size"] == pack
    assert item["product_url"] == PRODUCT_URL
    assert item["captured_at"] == CAPTURED_AT
    assert item["category_paths"] == [list(path)]
    assert item["sku"] == "000012"
    assert item["manufacturer_part_number"] == "0012-A"
    assert item["manufacturer"] == "Source manufacturer"
    assert item["currency"] == "USD"
    assert item["base_price"] == "16.49"
    assert item["quantity_tiers"] == []
    assert not any("promo" in key for key in item)
    assert extract_product(source.encode(), PRODUCT_URL, CAPTURED_AT) == result


def test_paginated_ui_does_not_limit_embedded_item_extraction(row):
    rows = {}
    for index in range(36):
        rows[str(index)] = dict(row, sku=f"00{index:04}", name=f"Unfamiliar item {index} 12/box",
                                description="Source size, 12/box")
    result = extract_product(_html(rows, paginate=True), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.expected_item_count == result.decoded_item_count == len(result.items) == 36
    assert result.items[-1]["sku"] == "000035"


@pytest.mark.parametrize("wording,unit", [
    ("200/box", "box"), ("50 pairs/box", "box"), ("case of 24", "case"),
    ("box of 10", "box"), ("100/bx", "box"), ("100/bag", "bag"),
    ("3 pairs/pkg", "pack"), ("1/pk", "pack"), ("48/case", "case"),
    ("5/pack", "pack"), ("24/novel-container", "novel-container"),
    (".5cc jar", "jar"), ("0.5cc jar", "jar"), ("3.4 oz bottle", "bottle"),
    ("5-pack", "pack"), ("25-pack", "pack"), ("kit 5ml", "kit"),
    ("kit - 9ml", "kit"), ("0.5cc syringe", "syringe"),
    ("2.5cc novel-container", "novel-container"),
])
def test_explicit_pack_wording_is_retained_without_a_unit_allowlist(row, wording, unit):
    row["name"] = "Unfamiliar item " + wording
    row["description"] = "Source size and pack: " + wording
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    item = result.items[0]
    assert result.status == "complete"
    assert item["item_name"] == row["name"]
    assert item["item_description"] == row["description"]
    assert item["selling_unit"] == unit
    assert item["pack_size"] == wording
    assert "notes" not in item


@pytest.mark.parametrize("name,description,unit,pack", [
    ("IV set 15 drops/ml, 50/cs", 'Single access port, 86"L, 15 drops/ml, 50/cs',
     "cs", "50/cs"),
    ("Surgical blade #12B,100/box", "#12B,100/box", "box", "100/box"),
    ("Gloves size 5.5, 50 pairs/box", "Size 5.5, box of 50 pairs (100 gloves)",
     "box", "50 pairs/box"),
    ("Gloves small 100/bag", '<div data-content-type="html">Small, 100/bag</div>',
     "bag", "100/bag"),
])
def test_observed_pack_context_does_not_change_the_selling_pack(row, name, description, unit, pack):
    row.update(name=name, description=description)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    item = result.items[0]
    assert result.status == "complete"
    assert item["selling_unit"] == unit
    assert item["pack_size"] == pack
    assert item["item_description"] == description


def test_unknown_pack_wording_remains_an_issue_before_final_notes(row):
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert result.needs_recovery
    assert result.items[0]["selling_unit"] is None
    assert result.items[0]["pack_size"] is None
    assert _codes(result) == {"unresolved_selling_unit", "unresolved_pack_size"}
    assert "notes" not in result.items[0]
    with pytest.raises(ValueError, match="recovery/deferral"):
        finalize_product(result)


def test_package_contents_can_come_from_the_saved_short_description(row):
    row.update(name="Guidor Easy-Graft Classic small", description="small pkg (0.15ml, green)",
               short_description="Contains: 3 systems x 0.15ml.")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    item = result.items[0]
    assert result.status == "complete"
    assert item["selling_unit"] == "pack"
    assert item["pack_size"] == "3 systems x 0.15ml."
    assert any(fact["source_field"] == "masterData.short_description"
               for fact in result.field_evidence[item["sku"]]["pack_size"])


@pytest.mark.parametrize("description", ["15 x 20mm", "15x20mm", "18 inches"])
def test_dimensions_alone_do_not_establish_a_selling_unit_or_pack(row, description):
    row.update(name="Membrane " + description, description=description)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial" and result.needs_recovery
    assert result.items[0]["selling_unit"] is None
    assert result.items[0]["pack_size"] is None


def test_a_material_name_before_capacity_is_not_assumed_to_be_a_selling_unit(row):
    row.update(name="VeraFuse demineralized bone matrix putty 2.5cc", description="2.5cc")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert result.items[0]["selling_unit"] is None


def test_a_word_ending_in_x_is_not_a_pack_multiplier(row):
    row.update(name="Matrix kit 5ml", description="kit - 5ml")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["selling_unit"] == "kit"


@pytest.mark.parametrize("name,description,unit,fields", [
    ("Item 100/box", "Small, 200/box", "box", {"pack_size"}),
    ("Item 100/box", "Small, 100/bag", None, {"selling_unit", "pack_size"}),
    ("Item 50 pairs/box", "Small, 50 gloves/box", "box", {"pack_size"}),
])
def test_conflicting_pack_sources_keep_supported_fields_and_both_source_values(
        row, name, description, unit, fields):
    row.update(name=name, description=description)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.items[0]["selling_unit"] == unit
    assert result.items[0]["pack_size"] is None
    assert {issue["field"] for issue in result.issues} == fields
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.issues == result.issues
    assert not final.needs_recovery
    for field in fields:
        assert [note for note in final.items[0]["notes"] if note["field"] == field] == [
            {"field": field, "reason": "conflicting_source",
             "source_field": "masterData.name", "source_text": name},
            {"field": field, "reason": "conflicting_source",
             "source_field": "masterData.description", "source_text": description},
        ]


def test_multipack_wording_keeps_unit_without_calculating_pack_contents(row):
    row.update(name="Item 2 x 50/box", description="2 x 50/box")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.items[0]["selling_unit"] == "box"
    assert result.items[0]["pack_size"] is None
    assert _codes(result) == {"unresolved_pack_size"}
    final = finalize_product(result, recovery_skipped_reason="Recovery budget exhausted")
    assert {note["reason"] for note in final.items[0]["notes"]} == {"unparsed_source"}


def test_different_name_and_description_wording_are_both_retained(row):
    row.update(name="New gloves 100/bx", description="Small, box of 100")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["item_name"] == "New gloves 100/bx"
    assert result.items[0]["item_description"] == "Small, box of 100"
    final = finalize_product(result)
    assert final.items[0]["selling_unit"] == "box"
    assert final.items[0]["pack_size"] == "100/bx"
    assert final.items[0]["notes"] == [
        {"field": "selling_unit", "reason": "normalized_source_wording",
         "source_field": "masterData.name", "source_text": "New gloves 100/bx"}]


@pytest.mark.parametrize("source_field,target", [
    ("description", "item_description"), ("manufacturer_name", "manufacturer"),
    ("manufacturer_part_number", "manufacturer_part_number"), ("stock_availability", "availability"),
])
def test_absent_optional_text_gets_a_final_note_without_recovery(row, source_field, target):
    row.pop(source_field)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert not result.needs_recovery
    assert "notes" not in result.items[0]
    final = finalize_product(result)
    assert final.status == "complete"
    assert final.items[0][target] is None
    assert final.items[0]["notes"] == [
        {"field": target, "reason": "source_absent",
         "source_field": "masterData." + source_field, "source_text": None}]


def test_absent_category_and_currency_metadata_get_final_notes_without_recovery(row):
    source = _html({"a": row}, path=()).replace(
        '<meta property="product:price:currency" content="USD">', '')
    result = extract_product(source, PRODUCT_URL, CAPTURED_AT)
    final = finalize_product(result)
    assert final.status == "complete"
    assert final.items[0]["category_paths"] is None
    assert final.items[0]["currency"] is None
    assert final.items[0]["notes"] == [
        {"field": "category_paths", "reason": "source_absent",
         "source_field": "breadcrumbs", "source_text": None},
        {"field": "currency", "reason": "source_absent",
         "source_field": "priceCurrency", "source_text": None},
    ]


@pytest.mark.parametrize("source_field,target,value", [
    ("manufacturer_name", "manufacturer", 12),
    ("manufacturer_part_number", "manufacturer_part_number", {"unexpected": "source"}),
    ("description", "item_description", ["unexpected", "source"]),
])
def test_invalid_optional_source_values_remain_issues_and_keep_json_evidence(
        row, source_field, target, value):
    row[source_field] = value
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert result.items[0][target] is None
    assert "notes" not in result.items[0]
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.issues == result.issues
    note, = final.items[0]["notes"]
    assert note["field"] == target
    assert note["reason"] == "unparsed_source"
    assert note["source_field"] == "masterData." + source_field
    assert json.loads(note["source_text"]) == value


@pytest.mark.parametrize("price,expected", [
    ("16.490000", "16.49"), ("0.000000", "0.00"), ("16.495000", "16.495"),
])
def test_prices_remain_exact_decimal_strings_without_rounding(row, price, expected):
    row["product_price"] = price
    row["price"]["price"] = _price(price)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["base_price"] == expected


def test_call_for_price_is_explicit_nonpublic_price_evidence(row):
    row["stock_availability"] = "call_for_price"
    row["price"]["price"] = "Call for price"
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["base_price"] is None
    assert result.items[0]["availability"] == "call_for_price"
    final = finalize_product(result)
    assert final.status == "complete"
    assert final.items[0]["notes"] == [
        {"field": "base_price", "reason": "nonpublic",
         "source_field": "masterData.stock_availability", "source_text": "call_for_price"}]


@pytest.mark.parametrize("markup", [None, "", _price("NaN"), _price("-1")])
def test_unexplained_price_gaps_remain_visible(row, markup):
    row["price"]["price"] = markup
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert result.needs_recovery
    assert result.items[0]["base_price"] is None
    assert "unresolved_price" in _codes(result)
    with pytest.raises(ValueError, match="recovery/deferral"):
        finalize_product(result)
    final = finalize_product(result, recovery_skipped_reason="Recovery budget exhausted")
    assert final.status == "partial"
    assert final.issues == result.issues
    assert {note["field"] for note in final.items[0]["notes"]} == {"base_price"}
    assert {note["reason"] for note in final.items[0]["notes"]} == {"unparsed_source"}


def test_conflicting_numeric_and_displayed_prices_are_not_silently_chosen(row):
    row["product_price"] = "17.49"
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.items[0]["base_price"] is None
    assert result.status == "partial"
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.items[0]["notes"] == [
        {"field": "base_price", "reason": "conflicting_source",
         "source_field": "masterData.product_price", "source_text": "17.49"},
        {"field": "base_price", "reason": "conflicting_source",
         "source_field": "masterData.price.price", "source_text": _price("16.49")},
    ]


def test_conflicting_currency_metadata_keeps_both_values_after_recovery(row):
    source = _html({"a": row}) + '<meta itemprop="priceCurrency" content="EUR">'
    result = extract_product(source, PRODUCT_URL, CAPTURED_AT)
    assert result.items[0]["currency"] is None
    assert result.needs_recovery
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.items[0]["notes"] == [
        {"field": "currency", "reason": "conflicting_source",
         "source_field": "priceCurrency", "source_text": "USD"},
        {"field": "currency", "reason": "conflicting_source",
         "source_field": "priceCurrency", "source_text": "EUR"},
    ]


@pytest.mark.parametrize("as_list", [False, True])
def test_public_quantity_tiers_accept_both_observed_json_containers(row, as_list):
    _tiers(row, (10, "15.990000"), (4, "16.190000"), as_list=as_list)
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["quantity_tiers"] == [
        {"min_quantity": 4, "unit_price": "16.19"},
        {"min_quantity": 10, "unit_price": "15.99"},
    ]


@pytest.mark.parametrize("price", ["16.490000", "17.490000"])
def test_unrendered_tiers_at_or_above_base_are_not_public_discounts(row, price):
    _tiers(row, (10, price))
    row["price"]["tier_price"] = False
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert result.items[0]["base_price"] == "16.49"
    assert result.items[0]["quantity_tiers"] == []


def test_missing_markup_for_a_lower_tier_still_requires_recovery(row):
    _tiers(row, (10, "15.99"))
    row["price"]["tier_price"] = False
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert "unresolved_tiers" in _codes(result)
    assert result.items[0]["quantity_tiers"] == []
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.issues == result.issues
    assert {note["field"] for note in final.items[0]["notes"]} == {"quantity_tiers"}
    assert {note["reason"] for note in final.items[0]["notes"]} == {"unparsed_source"}


@pytest.mark.parametrize("field,value", [
    ("price_qty", "2.5"), ("price_qty", "0"), ("price", "invalid"),
    ("website_price", "14.99"), ("percentage_value", "10"), ("all_groups", "0"),
])
def test_invalid_or_unverified_tiers_do_not_become_public_prices(row, field, value):
    _tiers(row, (10, "15.99"))
    row["tier_price"]["1"][field] = value
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert "invalid_tier" in _codes(result)
    assert result.items[0]["quantity_tiers"] == []


def test_identical_sku_duplicates_collapse_with_reconciled_counts(row):
    result = extract_product(_html({"a": row, "b": copy.deepcopy(row)}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "complete"
    assert len(result.items) == result.identical_duplicates == 1
    assert result.decoded_item_count == result.expected_item_count == 2
    assert result.rejected_item_count == 0
    assert finalize_product(result).status == "complete"


def test_conflicting_sku_rows_are_excluded_without_losing_other_items(row):
    conflict = dict(row, name="Different item with the same SKU")
    valid = dict(row, sku="000099")
    result = extract_product(_html({"a": row, "b": conflict, "c": valid}),
                             PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert [item["sku"] for item in result.items] == ["000099"]
    assert result.rejected_item_count == 2
    assert result.identical_duplicates == 0
    assert result.decoded_item_count == 3
    assert "conflicting_sku" in _codes(result)
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.issues == result.issues
    assert [item["sku"] for item in final.items] == ["000099"]
    assert final.items[0]["notes"] == []


@pytest.mark.parametrize("field,value", [("sku", None), ("sku", 12), ("name", "")])
def test_missing_required_identity_excludes_only_affected_rows(row, field, value):
    bad = dict(row)
    bad[field] = value
    result = extract_product(_html({"a": row, "b": bad}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert len(result.items) == result.rejected_item_count == 1
    assert "missing_item_identity" in _codes(result)


def test_no_usable_rows_is_failed_rather_than_complete(row):
    row.pop("sku")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "failed"
    assert result.items == ()
    assert "no_usable_items" in _codes(result)


def test_explicitly_empty_source_reports_zero_items_without_using_family_metadata():
    source = _html([]) + '<script type="application/ld+json">' + json.dumps({
        "@type": "Product", "sku": "FAMILY-CODE", "offers": {"price": "10.29"},
    }) + '</script>'
    result = extract_product(source, PRODUCT_URL, CAPTURED_AT)
    assert result.status == "failed"
    assert result.items == ()
    assert result.expected_item_count == result.decoded_item_count == 0
    assert _codes(result) == {"no_usable_items"}
    final = finalize_product(result, recovery_skipped_reason="No items recovered; budget exhausted")
    assert final.status == "failed"
    assert final.items == ()
    assert final.issues == result.issues


@pytest.mark.parametrize("total", [0, 2])
def test_count_gaps_keep_usable_items_and_report_incomplete_coverage(row, total):
    result = extract_product(_html({"a": row}, total=total), PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert len(result.items) == result.decoded_item_count == 1
    assert result.expected_item_count == total
    assert "item_count_mismatch" in _codes(result)
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.expected_item_count == total
    assert final.issues == result.issues
    assert final.items[0]["notes"] == []
    with pytest.raises(ValueError, match="item count must retain"):
        finalize_product(replace(result, issues=()))


def test_missing_count_metadata_does_not_discard_decoded_rows(row):
    source = _html({"a": row}).split('<script>window.productConfig')[0]
    result = extract_product(source, PRODUCT_URL, CAPTURED_AT)
    assert result.status == "partial"
    assert result.expected_item_count is None
    assert len(result.items) == 1
    final = finalize_product(result, recovery_event_ids=(7,))
    assert final.status == "partial"
    assert final.expected_item_count is None
    assert final.issues == result.issues
    with pytest.raises(ValueError, match="item count must retain"):
        finalize_product(replace(result, issues=()))


@pytest.mark.parametrize("source", [
    '<h1>Family</h1>',
    '<h1>Family</h1><script>window.masterData = unsafe();</script>',
    '<h1>Family</h1><script>window.masterData = "{}" + anotherValue;</script>',
    '<h1>Family</h1><script>window.masterData = "{\\"a\\":{},\\"a\\":{}}";</script>',
])
def test_unfamiliar_or_ambiguous_dataset_requires_structural_recovery(source):
    with pytest.raises(ExtractionError):
        extract_product(source, PRODUCT_URL, CAPTURED_AT)


def test_product_heading_is_required(row):
    with pytest.raises(ExtractionError, match="heading"):
        extract_product(_html({"a": row}).replace('<h1>Unfamiliar family</h1>', ''),
                        PRODUCT_URL, CAPTURED_AT)


@pytest.mark.parametrize("url,timestamp", [
    ("https://other.test/product/a", CAPTURED_AT),
    (PRODUCT_URL, "2026-10-19T18:00:00"),
    (PRODUCT_URL, "2026-10-19T18:00:00-04:00"),
])
def test_invalid_caller_inputs_are_not_misclassified_as_recovery_cases(row, url, timestamp):
    with pytest.raises(ValueError) as error:
        extract_product(_html({"a": row}), url, timestamp)
    assert not isinstance(error.value, ExtractionError)


def test_finalization_is_repeatable_without_mutating_preliminary_candidates(row):
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    before = copy.deepcopy(result)
    final = finalize_product(result)
    assert result == before
    assert not result.finalized
    assert "notes" not in result.items[0]
    assert final.finalized
    assert final.status == "complete"
    assert not final.needs_recovery
    assert final.items[0]["notes"] == []
    assert finalize_product(result) == final
    assert finalize_product(final) == final
    assert final.details["finalized"] is True
    assert final.details["recovery_event_ids"] == []
    assert final.details["recovery_skipped_reason"] is None
    json.dumps(final.items, allow_nan=False)
    json.dumps(final.details, allow_nan=False)


@pytest.mark.parametrize("recovery_ids,skip_reason", [
    ((7,), None),
    ((), "Recovery deferred: budget exhausted"),
    ((7,), "Further recovery deferred: budget exhausted"),
])
def test_final_notes_preserve_unresolved_issues_and_the_recovery_decision(
        row, recovery_ids, skip_reason):
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    final = finalize_product(result, recovery_event_ids=recovery_ids,
                             recovery_skipped_reason=skip_reason)
    assert final.status == "partial"
    assert final.issues == result.issues
    assert not final.needs_recovery
    assert final.details["recovery_event_ids"] == list(recovery_ids)
    assert final.details["recovery_skipped_reason"] == skip_reason
    notes = final.items[0]["notes"]
    assert len(notes) == 4
    for field in ("selling_unit", "pack_size"):
        assert [note for note in notes if note["field"] == field] == [
            {"field": field, "reason": "unparsed_source",
             "source_field": "masterData.name", "source_text": row["name"]},
            {"field": field, "reason": "unparsed_source",
             "source_field": "masterData.description", "source_text": row["description"]},
        ]
    assert finalize_product(final, recovery_event_ids=recovery_ids,
                            recovery_skipped_reason=skip_reason) == final
    with pytest.raises(ValueError, match="different recovery decision"):
        finalize_product(final, recovery_event_ids=(99,))


@pytest.mark.parametrize("pack_recovered", [False, True])
def test_finalization_retains_recovered_fields_and_only_notes_remaining_gaps(row, pack_recovered):
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    preliminary = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    before = copy.deepcopy(preliminary)
    candidate = dict(preliminary.items[0], selling_unit="box")
    resolved = {"selling_unit"}
    evidence = copy.deepcopy(preliminary.field_evidence)
    evidence[row["sku"]]["selling_unit"] = [
        {"source_field": "recovery.observation", "source_text": "Sold by box"}]
    if pack_recovered:
        candidate["pack_size"] = "100/box"
        resolved.add("pack_size")
        evidence[row["sku"]]["pack_size"] = [
            {"source_field": "recovery.observation", "source_text": "100/box"}]
    recovered = replace(preliminary, items=(candidate,), field_evidence=evidence,
                        issues=tuple(issue for issue in preliminary.issues if issue["field"] not in resolved))
    final = finalize_product(recovered, recovery_event_ids=(7,))
    assert preliminary == before
    assert final.items[0]["selling_unit"] == "box"
    assert not final.needs_recovery
    assert final.recovery_event_ids == (7,)
    if pack_recovered:
        assert final.status == "complete"
        assert final.items[0]["pack_size"] == "100/box"
        assert final.issues == ()
        assert final.items[0]["notes"] == []
    else:
        assert final.status == "partial"
        assert final.items[0]["pack_size"] is None
        assert {issue["field"] for issue in final.issues} == {"pack_size"}
        assert {note["field"] for note in final.items[0]["notes"]} == {"pack_size"}


@pytest.mark.parametrize("updates", [
    pytest.param({"sku": " "}, id="missing-sku"),
    pytest.param({"manufacturer_part_number": 12}, id="numeric-manufacturer-part-number"),
    pytest.param({"base_price": 16.49}, id="float-price"),
    pytest.param({"base_price": "-1.00"}, id="negative-price"),
    pytest.param({"base_price": "16.490000"}, id="unnormalized-price"),
    pytest.param({"category_paths": ["Gloves"]}, id="invalid-category-path"),
    pytest.param({"currency": "usd"}, id="invalid-currency"),
    pytest.param({"product_url": "https://www.safcodental.com/product/another-family"},
                 id="different-product-identity"),
    pytest.param({"quantity_tiers": None}, id="missing-tier-list"),
    pytest.param({"quantity_tiers": [{"min_quantity": True, "unit_price": "15.99"}]},
                 id="boolean-tier-quantity"),
    pytest.param({"quantity_tiers": [{"min_quantity": 10, "unit_price": "15.99"},
                                    {"min_quantity": 10, "unit_price": "14.99"}]},
                 id="conflicting-tier-quantities"),
    pytest.param({"promo_description": "Unvalidated promotion"}, id="unsupported-field"),
])
def test_final_validation_rejects_invalid_candidates_before_acceptance(row, updates):
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    candidate = dict(result.items[0], **updates)
    with pytest.raises(ValueError):
        finalize_product(replace(result, items=(candidate,)))
    assert not result.finalized
    assert "notes" not in result.items[0]


def test_final_validation_rejects_duplicate_skus_in_recovered_candidates(row):
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    duplicate = replace(result, items=(result.items[0], copy.deepcopy(result.items[0])),
                        decoded_item_count=2, expected_item_count=2)
    with pytest.raises(ValueError, match="SKUs must be unique"):
        finalize_product(duplicate)


def test_final_validation_rejects_unreconciled_candidate_counts(row):
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    with pytest.raises(ValueError, match="counts do not reconcile"):
        finalize_product(replace(result, decoded_item_count=2, expected_item_count=2))


def test_clearing_an_issue_without_recovering_the_field_does_not_make_it_complete(row):
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    with pytest.raises(ValueError, match="unresolved field must retain"):
        finalize_product(replace(result, issues=()), recovery_event_ids=(7,))


def test_nullable_gaps_require_source_evidence_for_final_notes(row):
    row.pop("manufacturer_part_number")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    with pytest.raises(ValueError, match="Missing field evidence"):
        finalize_product(replace(result, field_evidence={}))


def test_nonpublic_prices_cannot_be_replaced_with_unverified_public_prices(row):
    row["stock_availability"] = "call_for_price"
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    candidate = dict(result.items[0], base_price="16.49")
    with pytest.raises(ValueError, match="nonpublic price"):
        finalize_product(replace(result, items=(candidate,)), recovery_event_ids=(7,))


@pytest.mark.parametrize("decision", [
    {"recovery_event_ids": (0,)}, {"recovery_event_ids": (True,)},
    {"recovery_event_ids": (7, 7)}, {"recovery_skipped_reason": " "},
])
def test_invalid_recovery_decisions_are_rejected(row, decision):
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    with pytest.raises(ValueError):
        finalize_product(result, **decision)


def test_progress_logs_include_counts_and_issue_codes(row, caplog):
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.extraction"):
        extract_product(_html({"a": row}, total=2), PRODUCT_URL, CAPTURED_AT)
    assert PRODUCT_URL in caplog.text
    assert "expected=2 decoded=1 accepted=1" in caplog.text
    assert "status=partial" in caplog.text
    assert "item_count_mismatch" in caplog.text
    assert row["promo_description"] not in caplog.text


def test_finalization_logs_counts_and_recovery_disposition_without_source_wording(row, caplog):
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    result = extract_product(_html({"a": row}), PRODUCT_URL, CAPTURED_AT)
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.extraction"):
        finalize_product(result, recovery_skipped_reason="Recovery budget exhausted")
    assert "Product finalized product=" + PRODUCT_URL in caplog.text
    assert "accepted=1 notes=4 unresolved=2" in caplog.text
    assert "recovery_events=0 recovery_skipped=True status=partial" in caplog.text
    assert row["description"] not in caplog.text
    assert row["promo_description"] not in caplog.text


def test_saved_response_resumes_to_identical_sqlite_records(tmp_path, row):
    row.update(name="Unfamiliar gloves small 100/bx", description="Small, 100/box",
               manufacturer_part_number=None)
    _tiers(row, (10, "15.99"), as_list=True)
    path = tmp_path / "run.sqlite"
    body = _html({"a": row}).encode()
    expected = finalize_product(extract_product(body, PRODUCT_URL, CAPTURED_AT))
    with Storage(path) as store:
        store.create_run("run", {}, "fixture-revision")
        task = store.enqueue_task("run", TaskSpec("product", PRODUCT_URL,
                                                {"method": "GET", "url": PRODUCT_URL}))
        fetch = store.start_event(task, "fetch")
        store.update_event(fetch, "succeeded", body=body, details={"captured_at": CAPTURED_AT})
        interrupted = store.start_event(task, "extract")

    with Storage(path) as store:
        store.resume_run("run", {}, "fixture-revision")
        assert store.list_events(task)[-1]["event_id"] == interrupted
        assert store.list_events(task)[-1]["status"] == "uncertain"
        assert store.list_items(task) == []
        source = store.list_events(task)[0]
        preliminary = extract_product(source["body"], PRODUCT_URL, source["details"]["captured_at"])
        result = finalize_product(preliminary)
        assert result == expected
        event = store.start_event(task, "extract", details={"source_event_id": fetch, **result.details})
        count = store.commit_product(event, result.items, status=result.status,
                                     expected_item_count=result.expected_item_count,
                                     decoded_item_count=result.decoded_item_count, issues=result.issues)
        assert count == 1
        rows = store.list_items(task)

    with Storage(path) as store:
        assert store.commit_product(event, expected.items, status=expected.status,
                                    expected_item_count=expected.expected_item_count,
                                    decoded_item_count=expected.decoded_item_count,
                                    issues=expected.issues) == 1
        assert store.list_items(task) == rows
        assert rows[0]["captured_at"] == CAPTURED_AT
        assert rows[0]["selling_unit"] == "box"
        assert rows[0]["pack_size"] == "100/bx"
        assert rows[0]["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
        assert rows[0]["notes"] == expected.items[0]["notes"]
        assert {note["reason"] for note in rows[0]["notes"]} == {
            "source_absent", "normalized_source_wording"}
        assert rows[0]["extraction_event_id"] == event
        details = store.list_events(task)[-1]["details"]
        assert details["source_event_id"] == fetch
        assert details["finalized"] is True
        assert details["recovery_event_ids"] == []


@pytest.mark.parametrize("recovery_attempted", [False, True])
def test_final_notes_and_recovery_disposition_survive_resumption(tmp_path, row, recovery_attempted):
    """Use a fixture observation to test the handoff; this does not execute an agent."""
    row.update(name="Unfamiliar item", description="Assorted sizes; pack quantities vary")
    path = tmp_path / "run.sqlite"
    body = _html({"a": row}).encode()
    with Storage(path) as store:
        store.create_run("run", {}, "fixture-revision")
        task = store.enqueue_task("run", TaskSpec("product", PRODUCT_URL,
                                                {"method": "GET", "url": PRODUCT_URL}))
        fetch = store.start_event(task, "fetch")
        store.update_event(fetch, "succeeded", body=body, details={"captured_at": CAPTURED_AT})
        preliminary = extract_product(body, PRODUCT_URL, CAPTURED_AT)
        initial = store.start_event(task, "extract", details={"source_event_id": fetch})
        store.update_event(initial, "failed", details=preliminary.details)
        if recovery_attempted:
            recovery = store.start_event(task, "recovery", details={"input_event_ids": [fetch, initial]})
            store.update_event(recovery, "succeeded", details={
                "outcome": "partial", "candidate_fields": {"selling_unit": "box"},
                "steps": [{"action": "inspect_saved_evidence", "observation": "Sold by box; count unresolved"}],
            })
        assert store.list_items(task) == []

    with Storage(path) as store:
        store.resume_run("run", {}, "fixture-revision")
        events = {event["event_id"]: event for event in store.list_events(task)}
        source = events[fetch]
        result = extract_product(source["body"], PRODUCT_URL, source["details"]["captured_at"])
        recovery_ids = ()
        skipped_reason = "Recovery deferred: budget exhausted"
        if recovery_attempted:
            observation = events[recovery]["details"]
            candidate = dict(result.items[0], **observation["candidate_fields"])
            result = replace(result, items=(candidate,), issues=tuple(
                issue for issue in result.issues if issue["field"] != "selling_unit"))
            recovery_ids = (recovery,)
            skipped_reason = None
        final = finalize_product(result, recovery_event_ids=recovery_ids,
                                 recovery_skipped_reason=skipped_reason)
        event = store.start_event(task, "extract", details={
            "input_event_ids": [fetch, *recovery_ids], **final.details})
        store.commit_product(event, final.items, status=final.status,
                             expected_item_count=final.expected_item_count,
                             decoded_item_count=final.decoded_item_count, issues=final.issues)
        before = store.list_items(task)

    with Storage(path) as store:
        assert store.list_items(task) == before
        item, = store.list_items(task)
        events = {saved["event_id"]: saved for saved in store.list_events(task)}
        assert item["selling_unit"] == ("box" if recovery_attempted else None)
        assert item["pack_size"] is None
        assert item["notes"] == final.items[0]["notes"]
        assert {note["reason"] for note in item["notes"]} == {"unparsed_source"}
        assert len(item["notes"]) == (2 if recovery_attempted else 4)
        assert item["extraction_event_id"] == event
        assert events[event]["details"]["recovery_event_ids"] == list(recovery_ids)
        assert events[event]["details"]["recovery_skipped_reason"] == skipped_reason
        assert events[fetch]["body"] == body
        assert [saved["event_id"] for saved in events.values() if saved["type"] == "recovery"] == list(recovery_ids)
        assert store.get_task(task)["status"] == "partial"
        assert store.get_task(task)["issues"] == list(final.issues)
