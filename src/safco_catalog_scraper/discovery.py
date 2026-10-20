"""Decode Safco category evidence without network calls or database writes."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .storage import TaskSpec

logger = logging.getLogger(__name__)
_CONFIG_ASSIGNMENT = re.compile(r"(?:^|[;\n])\s*window\.algoliaConfig\s*=\s*")


class DiscoveryError(ValueError):
    """Saved evidence does not match the supported Safco category structure."""


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoveryError(f"Missing or invalid {name}")
    return value


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise DiscoveryError(f"Invalid {name}; expected an integer >= {minimum}")
    return value


def _url(value: Any, kind: str) -> str:
    value = _text(value, f"{kind} URL")
    if any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise DiscoveryError(f"Invalid whitespace in {kind} URL")
    try:
        parts = urlsplit(urljoin("https://www.safcodental.com/", value))
        valid = (parts.scheme == "https" and parts.hostname == "www.safcodental.com"
                 and parts.port in (None, 443) and parts.username is None
                 and parts.password is None and parts.path.startswith(f"/{kind}/")
                 and bool(parts.path.removeprefix(f"/{kind}/")))
    except ValueError:
        raise DiscoveryError(f"Invalid {kind} URL") from None
    if not valid:
        raise DiscoveryError(f"Expected a public Safco /{kind}/ URL")
    if kind == "catalog" and parts.query:
        raise DiscoveryError("Filtered category URLs are outside the supported scope")
    # Product query parameters may select a different item; retain them unchanged.
    return urlunsplit(("https", "www.safcodental.com", parts.path, parts.query, ""))


def category_task(category_url: str) -> TaskSpec:
    """Create the GET task whose saved HTML supplies category API configuration."""
    url = _url(category_url, "catalog")
    return TaskSpec("discovery", url + "#configuration", {"method": "GET", "url": url})


@dataclass(frozen=True)
class CategoryConfig:
    """Parsed storefront settings; never persist or log authentication_headers.

    Build instances with parse_category_config. Task requests exclude credentials;
    the controller supplies headers only when sending their HTTP requests.
    """

    category_url: str
    category_path: str
    application_id: str
    index_name: str
    page_size: int
    facet_filter: str
    rule_contexts: tuple[str, ...]
    api_key: str = field(repr=False)

    @property
    def authentication_headers(self) -> dict[str, str]:
        """Return transient Algolia headers; exclude these from logs and task state."""
        return {"X-Algolia-Application-Id": self.application_id,
                "X-Algolia-API-Key": self.api_key}

    def task(self, page: int = 0) -> TaskSpec:
        """Build a zero-based API page task with reproducible, non-secret inputs.

        request contains HTTP method/url/json plus category_url and page metadata.
        The controller passes only method/url/json to its HTTP client.
        """
        _integer(page, "requested page")
        params = urlencode({
            "query": "", "page": page, "hitsPerPage": self.page_size,
            "facetFilters": json.dumps([[self.facet_filter]], separators=(",", ":")),
            "numericFilters": '["visibility_catalog=1"]',
            "ruleContexts": json.dumps(self.rule_contexts, separators=(",", ":")),
        })
        request = {
            "method": "POST",
            "url": f"https://{self.application_id}-dsn.algolia.net/1/indexes/*/queries",
            "json": {"requests": [{"indexName": self.index_name, "params": params}]},
            "category_url": self.category_url, "page": page,
        }
        # Include filters and index in the identity; an API-key rotation changes neither.
        identity = urlencode({"index": self.index_name, "filter": self.facet_filter,
                              "contexts": json.dumps(self.rule_contexts),
                              "page_size": self.page_size, "page": page})
        return TaskSpec("discovery", self.category_url + "#" + identity, request)


def parse_category_config(html: str | bytes, category_url: str, *,
                          source_config: dict[str, Any] | None = None) -> CategoryConfig:
    """Read a single JSON window.algoliaConfig assignment; never evaluate JavaScript.

    Reject unsupported category modes instead of silently collecting another scope.
    Preserve the HTML as source evidence before calling this function.
    Recovery may supply source_config after resolving it from saved evidence;
    the same category, search mode and scope checks still apply.
    """
    url = _url(category_url, "catalog")
    config = source_config
    if config is None:
        assignments = []
        for script in BeautifulSoup(html, "html.parser").find_all("script"):
            text = script.string or ""
            for match in _CONFIG_ASSIGNMENT.finditer(text):
                assignments.append(text[match.end():])
        if len(assignments) != 1:
            raise DiscoveryError("Expected exactly one window.algoliaConfig assignment")
        try:
            config, _ = json.JSONDecoder().raw_decode(assignments[0].lstrip())
        except (ValueError, RecursionError):
            raise DiscoveryError("window.algoliaConfig is not valid JSON") from None
    if not isinstance(config, dict):
        raise DiscoveryError("window.algoliaConfig must be an object")
    request, instant = config.get("request"), config.get("instant")
    if not isinstance(request, dict) or not isinstance(instant, dict):
        raise DiscoveryError("Missing category request or instant-search settings")
    if (config.get("isCategoryPage") is not True
            or instant.get("isVisualMerchEnabled") is not False
            or config.get("isPersonalizationEnabled") is not False):
        raise DiscoveryError("Unsupported category search mode")
    if request.get("query") != "" or request.get("refinementValue") != "":
        raise DiscoveryError("Unexpected search query or refinement in category config")
    if request.get("landingPageId") != "":
        raise DiscoveryError("Landing-page rules are outside the supported category scope")
    if _url(request.get("url"), "catalog") != url:
        raise DiscoveryError("Category config URL differs from the requested category")
    application = _text(config.get("applicationId"), "application ID")
    if not re.fullmatch(r"[A-Za-z0-9]+", application):
        raise DiscoveryError("Invalid Algolia application ID")
    path = _text(request.get("path"), "category path")
    level = _integer(request.get("level"), "category level")
    category_id = _text(request.get("categoryId"), "category ID")
    contexts = request.get("ruleContexts")
    if not isinstance(contexts, dict):
        raise DiscoveryError("Missing category rule contexts")
    result = CategoryConfig(
        category_url=url, category_path=path, application_id=application,
        index_name=_text(config.get("indexName"), "index name") + "_products",
        page_size=_integer(config.get("hitsPerPage"), "page size", 1),
        facet_filter=f"categories.level{level}:{path}",
        rule_contexts=(_text(contexts.get("facetFilters"), "filter rule context"), "",
                       _text(contexts.get("merchCategoryPrefix"), "category rule prefix")
                       + category_id),
        api_key=_text(config.get("apiKey"), "API key"),
    )
    logger.info("Category configured category=%s page_size=%d", url, result.page_size)
    return result


@dataclass(frozen=True)
class DiscoveryPage:
    """Decoded targets and counts; the controller atomically commits targets/checkpoint.

    Local URL deduplication does not establish category completeness. Reconcile
    reported_hits against distinct object IDs and URLs across all committed pages.
    """

    category_url: str
    page: int
    reported_hits: int
    reported_pages: int
    decoded_hits: int
    object_ids: tuple[str, ...]
    products: tuple[TaskSpec, ...]
    next_page: TaskSpec | None

    @property
    def targets(self) -> tuple[TaskSpec, ...]:
        """Return products and the next page for Storage.commit_discovery."""
        return self.products + ((self.next_page,) if self.next_page else ())

    @property
    def checkpoint(self) -> dict[str, Any]:
        """Return non-secret reconciliation evidence suitable for SQLite JSON."""
        return {
            "category_url": self.category_url, "page": self.page,
            "reported_hits": self.reported_hits, "reported_pages": self.reported_pages,
            "decoded_hits": self.decoded_hits, "object_ids": list(self.object_ids),
            "product_urls": [task.target_key for task in self.products],
            "duplicate_object_ids": self.decoded_hits - len(set(self.object_ids)),
            "duplicate_urls": self.decoded_hits - len(self.products),
            "next_page": self.page + 1 if self.next_page else None,
        }


def decode_page(payload: str | bytes | Mapping[str, Any], config: CategoryConfig,
                page: int) -> DiscoveryPage:
    """Validate one Algolia response and return unique Safco product URL tasks.

    Require exact totals, expected page length and complete pagination. Malformed
    hits fail the whole decode so saved evidence remains available for recovery.
    No retries, network requests or persistence happen inside this function.
    """
    _integer(page, "requested page")
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except (ValueError, RecursionError):
            raise DiscoveryError("Category API response is not valid JSON") from None
    if not isinstance(payload, Mapping):
        raise DiscoveryError("Category API response must be an object")
    results = payload.get("results")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        raise DiscoveryError("Expected exactly one category API result")
    result = results[0]
    if result.get("index") != config.index_name:
        raise DiscoveryError("Category API returned a different index")
    total = _integer(result.get("nbHits"), "nbHits")
    pages = _integer(result.get("nbPages"), "nbPages")
    if _integer(result.get("page"), "response page") != page:
        raise DiscoveryError("Category API returned a different page")
    if _integer(result.get("hitsPerPage"), "hitsPerPage", 1) != config.page_size:
        raise DiscoveryError("Category API changed the requested page size")
    exhaustive = result.get("exhaustive", {})
    if not isinstance(exhaustive, dict):
        raise DiscoveryError("Invalid category exhaustiveness metadata")
    flags = [value for value in (result.get("exhaustiveNbHits"), exhaustive.get("nbHits"))
             if value is not None]
    if not flags or any(value is not True for value in flags):
        raise DiscoveryError("Category API did not establish an exact hit count")
    if pages != (total + config.page_size - 1) // config.page_size:
        raise DiscoveryError("Reported pages cannot cover the category hit count")
    if (total == 0 and page != 0) or (total > 0 and page >= pages):
        raise DiscoveryError("Requested page is outside the reported category range")
    hits = result.get("hits")
    expected = min(config.page_size, max(0, total - page * config.page_size))
    if not isinstance(hits, list) or len(hits) != expected:
        raise DiscoveryError("Decoded hit count differs from the expected page length")
    origins: dict[str, list[dict[str, Any]]] = {}
    object_urls: dict[str, str] = {}
    object_ids = []
    for position, hit in enumerate(hits):
        if not isinstance(hit, dict):
            raise DiscoveryError(f"Invalid category hit at position {position}")
        object_id = _text(hit.get("objectID"), f"objectID at position {position}")
        url = _url(hit.get("url"), "product")
        if object_id in object_urls and object_urls[object_id] != url:
            raise DiscoveryError("One category objectID points to conflicting product URLs")
        object_urls[object_id] = url
        object_ids.append(object_id)
        origin = {"category_url": config.category_url, "page": page, "object_id": object_id}
        associations = origins.setdefault(url, [])
        if origin not in associations:
            associations.append(origin)
    products = tuple(TaskSpec("product", url, {"method": "GET", "url": url}, tuple(source))
                     for url, source in origins.items())
    next_page = config.task(page + 1) if page + 1 < pages else None
    decoded = DiscoveryPage(config.category_url, page, total, pages, len(hits),
                            tuple(object_ids), products, next_page)
    logger.info("Category decoded category=%s page=%d/%d hits=%d unique_urls=%d total=%d",
                config.category_url, page + 1, max(1, pages), len(hits), len(products), total)
    if len(products) != len(hits) or len(set(object_ids)) != len(hits):
        logger.warning("Duplicate category hits category=%s page=%d; reconcile saved counts",
                       config.category_url, page + 1)
    return decoded
