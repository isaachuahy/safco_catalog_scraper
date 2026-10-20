"""Extract Safco product evidence without network calls or database writes."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from html import unescape
from typing import Any
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TEXT_FIELDS = {"item_description": "description", "manufacturer": "manufacturer_name",
                "manufacturer_part_number": "manufacturer_part_number",
                "availability": "stock_availability"}
_NULLABLE_FIELDS = (*_TEXT_FIELDS, "category_paths", "currency", "base_price",
                    "selling_unit", "pack_size")
_UNIT_ALIASES = {"bx": "box", "pk": "pack", "pkg": "pack"}
_WORD = r"[^\W\d_]+(?:-[^\W\d_]+)*"
_COUNT = r"(?:[0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?|\.[0-9]+)"
_MEASURE = r"(?:cc|ml|l|mg|g|kg|oz|fl\.?\s*oz)"
_PACK = re.compile(
    rf"(?<![\w./+-])(?P<count>{_COUNT})(?:\s+(?P<contents>{_WORD}))?\s*/\s*"
    rf"(?P<unit>{_WORD})\b|\b(?P<of_unit>{_WORD})\s+of\s+"
    rf"(?P<of_count>{_COUNT})(?:\s+(?P<of_contents>pairs?))?\b", re.IGNORECASE)
_CAPACITY_PACK = re.compile(
    rf"(?<![\w./+-])(?P<count>{_COUNT})\s*(?P<contents>{_MEASURE})\s+(?P<unit>{_WORD})\b|"
    rf"\b(?P<of_unit>kit)\s+(?:-\s*)?(?P<of_count>{_COUNT})\s*(?P<of_contents>{_MEASURE})\b|"
    rf"(?<![\w./+-])(?P<hyphen_count>{_COUNT})-(?P<hyphen_unit>pack)\b|"
    rf"\b(?P<package_unit>pkg|pk)\s*\((?P<package_count>{_COUNT})\s*"
    rf"(?P<package_contents>{_MEASURE})(?:\s*,[^()]*)?\)", re.IGNORECASE)


class ExtractionError(ValueError):
    """A structural failure that prevents trustworthy product-level extraction."""

    def __init__(self, code: str, message: str):
        """Expose a stable issue code while retaining a readable error message."""
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class ProductExtraction:
    """Normalized rows and unresolved issues; the controller decides recovery/persistence.

    Preliminary items have no final notes. field_evidence retains source wording
    for finalize_product; it is not an agent response or the exported catalog.
    The controller persists evidence and recovery operations, then finalizes the
    validated candidates before Storage.commit_product. captured_at belongs to
    the saved response. Unresolved issues still determine the final status.
    """

    product_name: str
    product_url: str
    items: tuple[dict[str, Any], ...]
    expected_item_count: int | None
    decoded_item_count: int
    identical_duplicates: int
    rejected_item_count: int
    issues: tuple[dict[str, Any], ...]
    field_evidence: dict[str, dict[str, list[dict[str, Any]]]] = field(default_factory=dict)
    finalized: bool = False
    recovery_event_ids: tuple[int, ...] = ()
    recovery_skipped_reason: str | None = None

    @property
    def status(self) -> str:
        """Return complete, partial with usable rows, or failed with no usable rows."""
        return ("partial" if self.issues else "complete") if self.items else "failed"

    @property
    def needs_recovery(self) -> bool:
        """Indicate preliminary issues; final notes never schedule further recovery."""
        return bool(self.issues) and not self.finalized

    @property
    def details(self) -> dict[str, Any]:
        """Return JSON-safe counts and findings for an extraction operation/report."""
        return {"expected_item_count": self.expected_item_count,
                "decoded_item_count": self.decoded_item_count,
                "accepted_item_count": len(self.items),
                "identical_duplicates": self.identical_duplicates,
                "rejected_item_count": self.rejected_item_count,
                "issues": list(self.issues), "finalized": self.finalized,
                "recovery_event_ids": list(self.recovery_event_ids),
                "recovery_skipped_reason": self.recovery_skipped_reason}


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ExtractionError("ambiguous_json", "Source JSON contains duplicate object keys")
        result[key] = value
    return result


def _assignment(soup: BeautifulSoup, name: str, *, parse_wrapper: bool = False) -> dict[str, Any]:
    prefix = rf"(?:^|[;\n])\s*window\.{name}\s*=\s*"
    if parse_wrapper:
        prefix += r"JSON\.parse\(\s*"
    pattern = re.compile(prefix)
    matches = [text[match.end():] for script in soup.find_all("script")
               for text in (script.string or "",) for match in pattern.finditer(text)]
    if len(matches) != 1:
        raise ExtractionError("missing_" + name, f"Expected one window.{name} assignment")
    decoder = json.JSONDecoder(object_pairs_hook=_object, parse_float=Decimal)
    try:
        source = matches[0].lstrip()
        value, end = decoder.raw_decode(source)
        tail = source[end:].lstrip()
        if parse_wrapper:
            if not isinstance(value, str) or not tail.startswith(")"):
                raise ExtractionError("invalid_" + name, "Expected JSON.parse with one JSON string")
            tail = tail[1:].lstrip()
        if tail and not tail.startswith(";"):
            raise ExtractionError("invalid_" + name, "Unexpected expression after source JSON")
        if isinstance(value, str):
            value = json.loads(value, object_pairs_hook=_object, parse_float=Decimal)
    except (ValueError, RecursionError) as error:
        if isinstance(error, ExtractionError):
            raise
        raise ExtractionError("invalid_" + name, f"window.{name} is not valid JSON") from None
    if name == "masterData" and value == []:
        return {}
    if not isinstance(value, dict):
        raise ExtractionError("invalid_" + name, f"window.{name} must decode to an object")
    return value


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("Expected source text")
    return unescape(value).strip() or None


def _evidence(source_field: str, value: Any) -> dict[str, Any]:
    text = value if value is None or isinstance(value, str) else json.dumps(value, default=str)
    if isinstance(text, str) and not text.strip():
        text = None
    return {"source_field": source_field, "source_text": text}


def _pack_expression(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    text = unescape(value).strip()
    if re.search(r"</?[A-Za-z][^>]*>", text):
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    # Safco places packs at the end; earlier slash expressions include IV drops/ml.
    matches = [match for pattern in (_PACK, _CAPACITY_PACK) for match in pattern.finditer(text)
               if re.fullmatch(r"[\s.,;]*(?:\([^()]*\)[\s.,;]*)?", text[match.end():])]
    if len(matches) != 1:
        return None
    match = matches[0]
    parts = match.groupdict()
    token = next(parts[key] for key in ("unit", "of_unit", "hyphen_unit", "package_unit") if parts.get(key))
    unit = _UNIT_ALIASES.get(token.casefold(), token.casefold())
    count = Decimal(next(parts[key] for key in ("count", "of_count", "hyphen_count", "package_count")
                         if parts.get(key)).replace(",", ""))
    contents = next((parts[key] for key in ("contents", "of_contents", "package_contents") if parts.get(key)), "").casefold()
    contents = "pair" if contents == "pairs" else contents
    ambiguous = count <= 0 or bool(re.search(r"(?:[×*+-]|\bx|\bof)\s*$", text[:match.start()]))
    return {"selling_unit": unit, "pack_size": None if ambiguous else match[0],
            "count": count, "contents": contents, "source_value": token}


def _pack_fields(source: dict[str, Any], issues: list[dict[str, Any]],
                 context: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    facts = [_evidence("masterData." + key, source.get(key)) for key in ("name", "description")]
    parsed = [(fact, match) for key, fact in zip(("name", "description"), facts)
              if (match := _pack_expression(source.get(key))) is not None]
    values = {"selling_unit": None, "pack_size": None}
    evidence = {"selling_unit": facts, "pack_size": facts}
    if not parsed:
        for target in values:
            issues.append(context | {"code": "unresolved_" + target, "field": target})
        return values, evidence
    units = {match["selling_unit"] for _, match in parsed}
    if len(units) == 1:
        values["selling_unit"] = next(iter(units))
        evidence["selling_unit"] = [fact | {"source_value": match["source_value"],
                                             "normalized_value": match["selling_unit"]}
                                    for fact, match in parsed]
    else:
        issues.append(context | {"code": "conflicting_selling_unit", "field": "selling_unit",
                                 "reason": "conflicting_source"})
    counts = {match["count"] for _, match in parsed}
    contents = {match["contents"] for _, match in parsed if match["contents"]}
    if len(units) == len(counts) == 1 and len(contents) <= 1 and all(
            match["pack_size"] is not None for _, match in parsed):
        values["pack_size"] = parsed[0][1]["pack_size"]
    else:
        conflict = len(units) > 1 or len(counts) > 1 or len(contents) > 1
        issues.append(context | {"code": "unresolved_pack_size", "field": "pack_size",
                                 "reason": "conflicting_source" if conflict else "unparsed_source"})
    short = source.get("short_description")
    if values["selling_unit"] is not None and isinstance(short, str) and not any(
            issue.get("sku") == context.get("sku") and issue.get("field") == "pack_size" for issue in issues):
        text = BeautifulSoup(unescape(short), "html.parser").get_text(" ", strip=True)
        stated = re.fullmatch(rf"Contains:\s*(?P<contents>{_COUNT}\b.+)", text, re.IGNORECASE)
        if stated:
            values["pack_size"] = stated["contents"]
            evidence["pack_size"] = [_evidence("masterData.short_description", short)]
    return values, evidence


def _amount(value: Any) -> Decimal:
    if type(value) not in (str, int, Decimal):
        raise ValueError("Expected an exact source amount")
    text = str(value)
    if len(text) > 64 or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", text) is None:
        raise ValueError("Expected a nonnegative decimal amount")
    return Decimal(text)


def _money(value: Any) -> str:
    text = format(_amount(value), "f")
    whole, _, fraction = text.partition(".")
    return whole + "." + fraction.rstrip("0").ljust(2, "0")


def _quantity(value: Any) -> int:
    number = _amount(value)
    if number < 1 or number != number.to_integral_value():
        raise ValueError("Expected a positive whole selling-unit quantity")
    return int(number)


def _breadcrumbs(soup: BeautifulSoup) -> list[list[str]] | None:
    paths = []
    for trail in soup.select(".breadcrumbs"):
        path = []
        for item in trail.select("li"):
            if any(re.fullmatch(r"category[0-9]+", name) for name in item.get("class", [])):
                link = item.find("a")
                if link is None or not link.get_text(strip=True):
                    raise ValueError("A category breadcrumb has no label")
                path.append(link.get_text(" ", strip=True))
        if path and path not in paths:
            paths.append(path)
    return paths or None


def _currency(soup: BeautifulSoup) -> str | None:
    values = {tag.get("content") for tag in soup.select(
        'meta[property="product:price:currency"], meta[itemprop="priceCurrency"]')}
    if not values:
        return None
    if len(values) != 1 or re.fullmatch(r"[A-Z]{3}", next(iter(values)) or "") is None:
        raise ValueError("Missing or conflicting currency metadata")
    return values.pop()


def _rendered_price(markup: Any) -> str | None:
    if not isinstance(markup, str):
        raise ValueError("Missing price markup")
    amounts = {_money(node.get("data-price-amount")) for node in BeautifulSoup(
        markup, "html.parser").select('[data-price-type="finalPrice"][data-price-amount]')}
    if len(amounts) > 1:
        raise ValueError("Conflicting rendered prices")
    return amounts.pop() if amounts else None


def _rendered_tiers(markup: Any) -> dict[int, str]:
    # Safco emits false, rather than HTML, when no quantity offer is displayed.
    if markup is False:
        return {}
    if not isinstance(markup, str):
        raise ValueError("Missing quantity-tier markup")
    soup = BeautifulSoup(markup, "html.parser")
    tiers = {}
    for item in soup.select("ul.prices-tier > li"):
        quantity = _quantity("".join(item.find_all(string=True, recursive=False)).strip())
        prices = {_money(node.get("data-price-amount"))
                  for node in item.select("[data-price-amount]")}
        if len(prices) != 1:
            raise ValueError("Missing or conflicting rendered tier price")
        price = prices.pop()
        if quantity in tiers and tiers[quantity] != price:
            raise ValueError("Conflicting prices at the same quantity")
        tiers[quantity] = price
    if markup.strip() and not tiers:
        raise ValueError("Unrecognized quantity-tier markup")
    return tiers


def _pricing(row: dict[str, Any], issues: list[dict[str, Any]],
             context: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    # A call-for-price flag is explicit evidence; a numeric zero alone is not.
    if row.get("stock_availability") == "call_for_price":
        return None, []
    markup = row.get("price")
    markup = markup if isinstance(markup, dict) else {}
    base = None
    price_issue = context | {"code": "unresolved_price", "field": "base_price"}
    try:
        base = _rendered_price(markup.get("price"))
        if base is None:
            raise ValueError("No public price found")
        if row.get("product_price") not in (None, "") and _money(row["product_price"]) != base:
            price_issue["reason"] = "conflicting_source"
            raise ValueError("Rendered and numeric base prices differ")
    except ValueError:
        base = None
        issues.append(price_issue)

    tiers: dict[int, str] = {}
    invalid_quantities = set()
    try:
        rendered = _rendered_tiers(markup.get("tier_price"))
    except ValueError:
        rendered = None
    source = row.get("tier_price")
    if isinstance(source, dict):
        source = source.values()
    elif not isinstance(source, list):
        issues.append(context | {"code": "invalid_tiers", "field": "quantity_tiers"})
        source = ()
    for tier in source:
        try:
            if not isinstance(tier, dict):
                raise ValueError("Invalid quantity-tier row")
            # Only all-groups prices establish the public quantity offer in this pattern.
            if tier.get("all_groups") != "1":
                raise ValueError("Unverified public tier eligibility")
            quantity = _quantity(tier.get("price_qty"))
            price = _money(tier.get("price"))
            if tier.get("percentage_value") is not None:
                raise ValueError("Percentage tiers require a separately established public price")
            if "website_price" in tier and _money(tier["website_price"]) != price:
                raise ValueError("Conflicting numeric tier prices")
            # Safco retains non-discount tiers in data while omitting them from its price UI.
            if (rendered is not None and quantity not in rendered and base is not None
                    and _amount(price) >= _amount(base)):
                continue
            if quantity in tiers and tiers[quantity] != price:
                invalid_quantities.add(quantity)
                raise ValueError("Conflicting prices at the same quantity")
            tiers[quantity] = price
        except ValueError:
            issues.append(context | {"code": "invalid_tier", "field": "quantity_tiers"})
    for quantity in invalid_quantities:
        tiers.pop(quantity, None)
    if rendered is None or rendered != tiers:
        issues.append(context | {"code": "unresolved_tiers", "field": "quantity_tiers"})
        tiers = {}
    return base, [{"min_quantity": quantity, "unit_price": price}
                  for quantity, price in sorted(tiers.items())]


def _check_inputs(product_url: str, captured_at: str) -> None:
    try:
        url = urlsplit(product_url)
        valid = (isinstance(product_url, str) and url.scheme == "https"
                 and url.hostname == "www.safcodental.com" and url.port in (None, 443)
                 and url.username is None and url.password is None and not url.fragment
                 and url.path.startswith("/product/") and url.path != "/product/"
                 and not any(ord(char) <= 32 or ord(char) == 127 for char in product_url))
        timestamp = datetime.fromisoformat(captured_at)
        valid = valid and timestamp.utcoffset() == timedelta(0)
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise ValueError("Supply a public Safco product URL and the response capture time in UTC")


def reported_item_count(html: str | bytes) -> int | None:
    """Return an independently readable source total, even when item decoding fails."""
    try:
        value = _assignment(BeautifulSoup(html, "html.parser"), "productConfig", parse_wrapper=True).get("total")
    except ExtractionError:
        return None
    return value if type(value) is int and value >= 0 else None


def extract_product(html: str | bytes, product_url: str, captured_at: str, *,
                    source_rows: dict[str, Any] | list[Any] | None = None,
                    source_total: int | None = None) -> ProductExtraction:
    """Decode saved Safco grouped-product HTML into flat catalog rows and issues.

    Read the whole masterData dataset regardless of UI pagination. Required row
    identity failures and conflicting duplicate SKUs exclude those rows. Missing
    optional text is nullable; unexplained prices or counts remain explicit issues.
    Preserve item names/descriptions and explicit trailing pack expressions. Unit
    tokens have no allowlist; only known aliases are standardized. Unsupported or
    conflicting wording remains an issue. No promotions, inferred manufacturers
    or calculated prices enter the output. Final notes require finalize_product.

    Raise ExtractionError when the heading/dataset cannot be decoded; raise
    ValueError for invalid caller URL/time inputs. Replay with the original UTC
    captured_at to obtain identical results. The controller owns source persistence.
    Recovery callers may supply source_rows/source_total only after resolving and
    verifying them against saved evidence. These replace dataset location, never
    row validation, public pricing checks, identity checks or duplicate handling.
    """
    _check_inputs(product_url, captured_at)
    soup = BeautifulSoup(html, "html.parser")
    try:
        headings = soup.select("h1")
        if len(headings) != 1 or not headings[0].get_text(strip=True):
            raise ExtractionError("missing_product_name", "Expected one nonempty product heading")
        name = headings[0].get_text(" ", strip=True)
        if source_rows is None:
            data = _assignment(soup, "masterData")
        elif isinstance(source_rows, list):
            data = dict(enumerate(source_rows))
        elif isinstance(source_rows, dict):
            data = source_rows
        else:
            raise ValueError("Recovered source rows must be an object or list")
    except ExtractionError as error:
        logger.warning("Product structural failure product=%s code=%s", product_url, error.code)
        raise

    issues: list[dict[str, Any]] = []
    expected = None
    try:
        config = _assignment(soup, "productConfig", parse_wrapper=True)
        if type(config.get("total")) is not int or config["total"] < 0:
            raise ExtractionError("unknown_item_count", "Missing or invalid reported item count")
        expected = config["total"]
    except ExtractionError as error:
        issues.append({"code": error.code, "field": "expected_item_count"})
    if source_total is not None:
        if type(source_total) is not int or source_total < 0:
            raise ValueError("Recovered source total must be a nonnegative integer")
        if expected is not None and expected != source_total:
            issues.append({"code": "conflicting_item_count", "field": "expected_item_count"})
        elif expected is None:
            expected = source_total
            issues = [issue for issue in issues if issue.get("field") != "expected_item_count"]
    if expected is not None and expected != len(data):
        issues.append({"code": "item_count_mismatch", "expected": expected, "decoded": len(data)})

    metadata_evidence = {
        "category_paths": [_evidence("breadcrumbs", str(node)) for node in soup.select(
            '.breadcrumbs li[class*="category"]')] or [_evidence("breadcrumbs", None)],
        "currency": [_evidence("priceCurrency", node.get("content")) for node in soup.select(
            'meta[property="product:price:currency"], meta[itemprop="priceCurrency"]')]
        or [_evidence("priceCurrency", None)]}
    metadata = {}
    for field, parser in (("category_paths", _breadcrumbs), ("currency", _currency)):
        try:
            metadata[field] = parser(soup)
        except ValueError:
            metadata[field] = None
            issue = {"code": "invalid_" + field, "field": field}
            if field == "currency" and len({fact["source_text"] for fact in metadata_evidence[field]}) > 1:
                issue["reason"] = "conflicting_source"
            issues.append(issue)
    groups: dict[str, list[tuple[int, dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    rejected = 0
    for position, source in enumerate(data.values()):
        context = {"row": position}
        if not isinstance(source, dict):
            issues.append(context | {"code": "invalid_item"})
            rejected += 1
            continue
        try:
            sku, item_name = _text(source.get("sku")), _text(source.get("name"))
            if not sku or not item_name:
                raise ValueError("Missing required item identity")
        except ValueError:
            issues.append(context | {"code": "missing_item_identity"})
            rejected += 1
            continue
        context["sku"] = sku
        item = {"product_name": name, "product_url": product_url, "captured_at": captured_at,
                "sku": sku, "item_name": item_name, **metadata}
        evidence = dict(metadata_evidence)
        for target, key in _TEXT_FIELDS.items():
            evidence[target] = [_evidence("masterData." + key, source.get(key))]
            try:
                item[target] = _text(source.get(key))
            except ValueError:
                item[target] = None
                issues.append(context | {"code": "invalid_item_field", "field": target})
        packs, pack_evidence = _pack_fields(source, issues, context)
        item.update(packs)
        evidence.update(pack_evidence)
        item["base_price"], item["quantity_tiers"] = _pricing(source, issues, context)
        price_markup = source.get("price")
        price_markup = price_markup if isinstance(price_markup, dict) else {}
        evidence["base_price"] = [_evidence("masterData.product_price", source.get("product_price")),
                                  _evidence("masterData.price.price", price_markup.get("price"))]
        if item["availability"] == "call_for_price":
            evidence["base_price"] = evidence["availability"]
        evidence["quantity_tiers"] = [_evidence("masterData.tier_price", source.get("tier_price")),
                                      _evidence("masterData.price.tier_price", price_markup.get("tier_price"))]
        groups[sku].append((position, item, evidence))

    items = []
    field_evidence = {}
    duplicates = 0
    for sku, records in groups.items():
        first = records[0][1]
        if any(item != first for _, item, _ in records[1:]):
            rejected += len(records)
            issues.append({"code": "conflicting_sku", "sku": sku,
                           "rows": [position for position, _, _ in records]})
        else:
            items.append(first)
            field_evidence[sku] = records[0][2]
            duplicates += len(records) - 1
    if not items:
        issues.append({"code": "no_usable_items"})
    result = ProductExtraction(name, product_url, tuple(items), expected, len(data),
                               duplicates, rejected, tuple(issues), field_evidence)
    logger.info("Product extracted product=%s expected=%s decoded=%d accepted=%d "
                "duplicates=%d rejected=%d status=%s", product_url, expected, len(data),
                len(items), duplicates, rejected, result.status)
    if issues:
        logger.warning("Product issues product=%s count=%d codes=%s", product_url, len(issues),
                       ",".join(sorted({issue["code"] for issue in issues})))
    return result


def _validate_item(item: dict[str, Any], result: ProductExtraction) -> None:
    allowed = {"product_name", "product_url", "captured_at", "sku", "item_name",
               "quantity_tiers", "notes", *_NULLABLE_FIELDS}
    if item.keys() - allowed:
        raise ValueError("Candidate contains unsupported catalog fields")
    if item.get("product_name") != result.product_name or item.get("product_url") != result.product_url:
        raise ValueError("Candidate product identity differs from the extraction")
    _check_inputs(item.get("product_url"), item.get("captured_at"))
    for target in ("product_name", "sku", "item_name", *_NULLABLE_FIELDS):
        value = item.get(target)
        if target in ("category_paths", "base_price"):
            continue
        if value is None and target in _NULLABLE_FIELDS:
            continue
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            raise ValueError("Invalid candidate field: " + target)
    paths = item.get("category_paths")
    if paths is not None and (not isinstance(paths, list) or not paths or any(
            not isinstance(path, list) or not path or any(
                not isinstance(label, str) or not label.strip() for label in path) for path in paths)):
        raise ValueError("Invalid candidate category paths")
    currency = item.get("currency")
    if currency is not None and re.fullmatch(r"[A-Z]{3}", currency) is None:
        raise ValueError("Invalid candidate currency")
    price = item.get("base_price")
    if price is not None and (not isinstance(price, str) or _money(price) != price):
        raise ValueError("Candidate price must be an exact normalized decimal string")
    tiers = item.get("quantity_tiers")
    if not isinstance(tiers, list):
        raise ValueError("Candidate quantity tiers must be a list")
    previous = 0
    for tier in tiers:
        if not isinstance(tier, dict) or set(tier) != {"min_quantity", "unit_price"}:
            raise ValueError("Invalid candidate quantity tier")
        quantity, price = tier["min_quantity"], tier["unit_price"]
        if (type(quantity) is not int or quantity <= previous or not isinstance(price, str)
                or _money(price) != price):
            raise ValueError("Candidate tiers require increasing quantities and exact prices")
        previous = quantity
    if item["availability"] == "call_for_price" and (item["base_price"] is not None or tiers):
        raise ValueError("A nonpublic price cannot also contain public prices")


def finalize_product(result: ProductExtraction, *, recovery_event_ids: tuple[int, ...] = (),
                     recovery_skipped_reason: str | None = None) -> ProductExtraction:
    """Validate candidates and attach final field notes after a recovery decision.

    Supply the result re-extracted from additional evidence, or validated merged
    candidates with resolved issues removed and field_evidence retained. Unresolved
    issues require persisted recovery event IDs or an explicit reason recovery was
    unnecessary/deferred. The controller verifies those references and persists
    details with the acceptance event; this function performs no I/O or model call.

    Confirmed source absence/nonpublic prices need no recovery. Notes do not clear
    unresolved issues. Return a new result without mutating preliminary candidates;
    repeated finalization is idempotent. Raise ValueError for invalid candidates,
    missing evidence, or a missing recovery decision.
    """
    ids = tuple(recovery_event_ids)
    if any(type(event_id) is not int or event_id < 1 for event_id in ids) or len(set(ids)) != len(ids):
        raise ValueError("Recovery event IDs must be distinct positive integers")
    if recovery_skipped_reason is not None and (
            not isinstance(recovery_skipped_reason, str) or not recovery_skipped_reason.strip()):
        raise ValueError("Supply a nonempty reason for skipped/deferred recovery")
    if result.finalized:
        if (ids and ids != result.recovery_event_ids) or (
                recovery_skipped_reason is not None and recovery_skipped_reason != result.recovery_skipped_reason):
            raise ValueError("The result already has a different recovery decision")
        return result
    if result.issues and not ids and recovery_skipped_reason is None:
        raise ValueError("Resolve issues or record recovery/deferral before creating final notes")
    counts = (result.decoded_item_count, result.identical_duplicates, result.rejected_item_count)
    if any(type(count) is not int or count < 0 for count in counts) or (
            len(result.items) + result.identical_duplicates + result.rejected_item_count != result.decoded_item_count):
        raise ValueError("Candidate counts do not reconcile")
    if (result.rejected_item_count or not result.items) and not result.issues:
        raise ValueError("Rejected or absent items must retain their issues")
    expected = result.expected_item_count
    if expected is not None and (type(expected) is not int or expected < 0):
        raise ValueError("Invalid expected item count")
    unknown_count = expected is None and not any(
        issue.get("field") == "expected_item_count" for issue in result.issues)
    missing_count_gap = expected is not None and expected != result.decoded_item_count and not any(
        issue.get("code") == "item_count_mismatch" for issue in result.issues)
    if unknown_count or missing_count_gap:
        raise ValueError("An unresolved item count must retain its issue")
    items = []
    skus = set()
    for candidate in result.items:
        item = dict(candidate)
        for target in _NULLABLE_FIELDS:
            item.setdefault(target, None)
        _validate_item(item, result)
        sku = item["sku"]
        if sku in skus:
            raise ValueError("Candidate SKUs must be unique")
        skus.add(sku)
        evidence = result.field_evidence.get(sku, {})
        findings = [issue for issue in result.issues if issue.get("sku", sku) == sku]
        fields = dict.fromkeys((*_NULLABLE_FIELDS, *(issue["field"] for issue in findings
                                                    if issue.get("field") in item)))
        notes = []
        for target in fields:
            value = item.get(target)
            field_issues = [issue for issue in findings if issue.get("field") == target]
            facts = evidence.get(target)
            if value is None or field_issues:
                if not facts:
                    raise ValueError("Missing field evidence for " + target)
                absent = all(fact["source_text"] is None or not fact["source_text"].strip() for fact in facts)
                if target == "base_price" and item["availability"] == "call_for_price":
                    reason = "nonpublic"
                elif field_issues:
                    reason = ("conflicting_source" if any(issue.get("reason") == "conflicting_source"
                                                         for issue in field_issues) else "unparsed_source")
                elif absent and target != "base_price":
                    reason = "source_absent"
                else:
                    raise ValueError("An unresolved field must retain its issue: " + target)
                for fact in facts:
                    note = {"field": target, "reason": reason,
                            "source_field": fact["source_field"], "source_text": fact["source_text"]}
                    if note not in notes:
                        notes.append(note)
            elif target == "selling_unit":
                for fact in facts or ():
                    if fact.get("normalized_value") == value and fact.get("source_value") != value:
                        note = {"field": target, "reason": "normalized_source_wording",
                                "source_field": fact["source_field"], "source_text": fact["source_text"]}
                        if note not in notes:
                            notes.append(note)
        item["notes"] = notes
        items.append(item)
    final = replace(result, items=tuple(items), finalized=True, recovery_event_ids=ids,
                    recovery_skipped_reason=recovery_skipped_reason)
    logger.info("Product finalized product=%s accepted=%d notes=%d unresolved=%d "
                "recovery_events=%d recovery_skipped=%s status=%s", result.product_url, len(items),
                sum(len(item["notes"]) for item in items), len(result.issues), len(ids),
                recovery_skipped_reason is not None, final.status)
    return final
