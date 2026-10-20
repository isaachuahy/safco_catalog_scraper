"""Offline discovery contracts using synthetic Safco-shaped source evidence."""

import json
import logging
from urllib.parse import parse_qs

import pytest

from safco_catalog_scraper.discovery import (
    DiscoveryError,
    category_task,
    decode_page,
    parse_category_config,
)
from safco_catalog_scraper.storage import Storage

GLOVES_URL = "https://www.safcodental.com/catalog/gloves"
SURGICAL_URL = "https://www.safcodental.com/catalog/sutures-surgical-products"
PRODUCT_A = "https://www.safcodental.com/product/fixture-a"
PRODUCT_B = "https://www.safcodental.com/product/fixture-b?item=002"


def _html(settings):
    return ("<html><script>window.unrelated = {};</script><script>\n"
            "window.algoliaConfig = " + json.dumps(settings) + ";\n</script></html>")


@pytest.fixture
def settings():
    """Keep the observed config shape; credentials and catalog contents are fictional."""
    return {
        "applicationId": "FIXTUREAPP", "apiKey": "fixture-public-search-key",
        "indexName": "fixture_default", "hitsPerPage": 2,
        "isCategoryPage": True, "isPersonalizationEnabled": False,
        "instant": {"isVisualMerchEnabled": False},
        "request": {
            "url": GLOVES_URL, "categoryId": "385", "level": 1,
            "path": "Dental Supplies /// Dental Exam Gloves", "query": "",
            "refinementValue": "", "landingPageId": "",
            "ruleContexts": {"facetFilters": "magento_filters",
                             "merchCategoryPrefix": "magento-category-"},
        },
    }


@pytest.fixture
def config(settings):
    return parse_category_config(_html(settings), GLOVES_URL)


@pytest.fixture
def response():
    return {"results": [{
        "index": "fixture_default_products", "nbHits": 3, "nbPages": 2,
        "page": 0, "hitsPerPage": 2, "exhaustiveNbHits": True,
        "hits": [{"objectID": "001", "url": PRODUCT_A},
                 {"objectID": "002", "url": PRODUCT_B}],
    }]}


@pytest.mark.parametrize("category_url,path,category_id", [
    (GLOVES_URL, "Dental Supplies /// Dental Exam Gloves", "385"),
    (SURGICAL_URL, "Dental Supplies /// Sutures & surgical products", "937"),
])
def test_api_request_uses_category_settings(settings, category_url, path, category_id):
    settings["request"].update(url=category_url, path=path, categoryId=category_id)
    config = parse_category_config(_html(settings).encode(), category_url)
    task = config.task(1)
    request = task.request
    query = request["json"]["requests"][0]
    params = parse_qs(query["params"], keep_blank_values=True)

    assert task.kind == "discovery"
    assert request["method"] == "POST"
    assert request["url"] == "https://FIXTUREAPP-dsn.algolia.net/1/indexes/*/queries"
    assert request["category_url"] == category_url
    assert request["page"] == 1
    assert query["indexName"] == "fixture_default_products"
    assert params["query"] == [""]
    assert params["page"] == ["1"]
    assert params["hitsPerPage"] == ["2"]
    assert json.loads(params["facetFilters"][0]) == [["categories.level1:" + path]]
    assert json.loads(params["numericFilters"][0]) == ["visibility_catalog=1"]
    assert json.loads(params["ruleContexts"][0]) == [
        "magento_filters", "", "magento-category-" + category_id,
    ]


def test_key_rotation_preserves_task_identity_and_keeps_keys_out_of_logs(settings, caplog):
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.discovery"):
        original = parse_category_config(_html(settings), GLOVES_URL)
        settings["apiKey"] = "rotated-fixture-key"
        rotated = parse_category_config(_html(settings), GLOVES_URL)

    assert original.task() == rotated.task()
    assert original.authentication_headers["X-Algolia-API-Key"] == "fixture-public-search-key"
    assert rotated.authentication_headers["X-Algolia-API-Key"] == "rotated-fixture-key"
    for value in (original, rotated):
        assert value.api_key not in repr(value)
        assert value.api_key not in json.dumps(value.task().request)
        assert value.api_key not in caplog.text
    assert GLOVES_URL in caplog.text
    assert "page_size=2" in caplog.text


def test_changed_filters_or_page_produce_distinct_tasks(settings, config):
    assert config.task(0).target_key != config.task(1).target_key
    settings["request"]["path"] = "Dental Supplies /// Gloves /// Nitrile"
    settings["request"]["level"] = 2
    changed = parse_category_config(_html(settings), GLOVES_URL)
    assert changed.task().target_key != config.task().target_key


def test_category_seed_ignores_fragment_but_rejects_filtered_scope():
    task = category_task(GLOVES_URL)
    assert category_task(GLOVES_URL + "#results") == task
    assert task.request == {"method": "GET", "url": GLOVES_URL}
    with pytest.raises(DiscoveryError, match="Filtered category"):
        category_task(GLOVES_URL + "?page=2")


@pytest.mark.parametrize("source", [
    "<html>No configuration</html>",
    "<script>window.algoliaConfig = unsafeFunction();</script>",
    "<script>window.algoliaConfig = [];</script>",
    '<script>window.algoliaConfig = {"truncated":</script>',
])
def test_missing_or_non_json_config_is_a_structural_failure(source):
    with pytest.raises(DiscoveryError):
        parse_category_config(source, GLOVES_URL)


def test_multiple_assignments_are_ambiguous(settings):
    with pytest.raises(DiscoveryError, match="exactly one"):
        parse_category_config(_html(settings) + _html(settings), GLOVES_URL)


@pytest.mark.parametrize("path,value", [
    (("isCategoryPage",), False),
    (("isPersonalizationEnabled",), True),
    (("instant", "isVisualMerchEnabled"), True),
    (("request", "query"), "gloves"),
    (("request", "refinementValue"), "nitrile"),
    (("request", "landingPageId"), "new-landing-page"),
    (("request", "url"), SURGICAL_URL),
    (("request", "level"), -1),
    (("request", "ruleContexts"), None),
    (("request",), None),
    (("hitsPerPage",), 0),
    (("hitsPerPage",), True),
    (("applicationId",), "other.test/path"),
    (("apiKey",), ""),
])
def test_unsupported_or_incomplete_config_is_rejected(settings, path, value):
    target = settings
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(DiscoveryError):
        parse_category_config(_html(settings), GLOVES_URL)


@pytest.mark.parametrize("page", [-1, True, "1"])
def test_invalid_requested_page_is_rejected(config, response, page):
    with pytest.raises(DiscoveryError, match="requested page"):
        config.task(page)
    with pytest.raises(DiscoveryError, match="requested page"):
        decode_page(response, config, page)


def test_page_targets_and_checkpoints_preserve_counts_and_origins(config, response, caplog):
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.discovery"):
        page = decode_page(response, config, 0)

    assert [target.target_key for target in page.products] == [PRODUCT_A, PRODUCT_B]
    assert page.products[0].request == {"method": "GET", "url": PRODUCT_A}
    assert page.products[0].discovered_from == (
        {"category_url": GLOVES_URL, "page": 0, "object_id": "001"},
    )
    assert page.next_page == config.task(1)
    assert page.targets == (*page.products, page.next_page)
    assert page.checkpoint == {
        "category_url": GLOVES_URL, "page": 0, "reported_hits": 3,
        "reported_pages": 2, "decoded_hits": 2, "object_ids": ["001", "002"],
        "product_urls": [PRODUCT_A, PRODUCT_B], "duplicate_object_ids": 0,
        "duplicate_urls": 0, "next_page": 1,
    }
    assert "page=1/2 hits=2 unique_urls=2 total=3" in caplog.text
    assert config.api_key not in caplog.text


def test_text_and_byte_evidence_replay_identically(config, response):
    source = json.dumps(response)
    expected = decode_page(response, config, 0)
    assert decode_page(source, config, 0) == expected
    assert decode_page(source.encode(), config, 0) == expected


def test_last_page_and_empty_category_schedule_no_more_pages(config, response):
    result = response["results"][0]
    result.update(page=1, hits=[{"objectID": "003", "url": "/product/fixture-c"}])
    last = decode_page(response, config, 1)
    assert last.decoded_hits == 1
    assert last.next_page is None
    assert last.targets == last.products
    assert last.checkpoint["next_page"] is None

    result.update(page=0, nbHits=0, nbPages=0, hits=[])
    empty = decode_page(response, config, 0)
    assert empty.reported_hits == empty.decoded_hits == 0
    assert empty.targets == ()
    with pytest.raises(DiscoveryError, match="outside"):
        result["page"] = 1
        decode_page(response, config, 1)


@pytest.mark.parametrize("payload", [
    "invalid JSON", b"\xff", [], {"message": "API error", "status": 403},
    {"results": []}, {"results": [None]}, {"results": [{}, {}]},
])
def test_invalid_api_envelopes_are_structural_failures(config, payload):
    with pytest.raises(DiscoveryError):
        decode_page(payload, config, 0)


@pytest.mark.parametrize("key,value", [
    ("index", "another_index"), ("page", 1), ("nbHits", -1), ("nbHits", True),
    ("nbPages", 1), ("hitsPerPage", 3), ("hits", []),
    ("exhaustiveNbHits", False), ("exhaustiveNbHits", None),
    ("exhaustive", {"nbHits": False}),
])
def test_count_gaps_and_unverified_totals_are_rejected(config, response, key, value):
    response["results"][0][key] = value
    with pytest.raises(DiscoveryError):
        decode_page(response, config, 0)


def test_new_exhaustiveness_metadata_can_establish_exact_count(config, response):
    result = response["results"][0]
    del result["exhaustiveNbHits"]
    result["exhaustive"] = {"nbHits": True}
    assert decode_page(response, config, 0).reported_hits == 3


@pytest.mark.parametrize("hit", [None, {}, {"objectID": 1, "url": PRODUCT_A}])
def test_malformed_hits_are_not_silently_skipped(config, response, hit):
    response["results"][0]["hits"][1] = hit
    with pytest.raises(DiscoveryError):
        decode_page(response, config, 0)


@pytest.mark.parametrize("url", [
    "https://other.test/product/a", "//other.test/product/a",
    "http://www.safcodental.com/product/a",
    "https://user:pass@www.safcodental.com/product/a",
    "https://www.safcodental.com:8443/product/a",
    "https://www.safcodental.com/catalog/gloves", PRODUCT_A + "\n",
])
def test_product_targets_must_remain_public_safco_product_urls(config, response, url):
    response["results"][0]["hits"][0]["url"] = url
    with pytest.raises(DiscoveryError):
        decode_page(response, config, 0)


@pytest.mark.parametrize("second_id,duplicate_ids,origin_count", [("001", 1, 1), ("002", 0, 2)])
def test_duplicate_urls_keep_raw_counts_and_all_origins(
        config, response, caplog, second_id, duplicate_ids, origin_count):
    response["results"][0]["hits"] = [
        {"objectID": "001", "url": "https://www.safcodental.com:443/product/fixture-a#top"},
        {"objectID": second_id, "url": "/product/fixture-a#details"},
    ]
    with caplog.at_level(logging.WARNING, logger="safco_catalog_scraper.discovery"):
        page = decode_page(response, config, 0)
    assert page.decoded_hits == 2
    assert len(page.products) == 1
    assert page.products[0].target_key == PRODUCT_A
    assert len(page.products[0].discovered_from) == origin_count
    assert page.checkpoint["duplicate_urls"] == 1
    assert page.checkpoint["duplicate_object_ids"] == duplicate_ids
    assert "Duplicate category hits" in caplog.text


def test_distinct_item_queries_are_preserved(config, response):
    response["results"][0]["hits"] = [
        {"objectID": "001", "url": "/product/fixture-a?item=001"},
        {"objectID": "002", "url": "/product/fixture-a?item=002"},
    ]
    page = decode_page(response, config, 0)
    assert [task.target_key.rsplit("?", 1)[1] for task in page.products] == ["item=001", "item=002"]


def test_conflicting_urls_for_one_object_id_are_rejected(config, response):
    response["results"][0]["hits"][1]["objectID"] = "001"
    with pytest.raises(DiscoveryError, match="conflicting product URLs"):
        decode_page(response, config, 0)


def test_saved_discovery_resumes_before_commit_without_duplicate_targets(
        tmp_path, settings, config, response):
    path = tmp_path / "run.sqlite"
    run_config = {"categories": [GLOVES_URL]}
    source_html = _html(settings).encode()
    source_json = json.dumps(response).encode()
    expected = decode_page(source_json, config, 0)

    with Storage(path) as store:
        store.create_run("run", run_config, "fixture-revision")
        seed_id = store.enqueue_task("run", category_task(GLOVES_URL))
        seed_fetch = store.start_event(seed_id, "fetch")
        store.update_event(seed_fetch, "succeeded", body=source_html)
        seed_extract = store.start_event(seed_id, "extract")
        page_id = store.commit_discovery(seed_extract, [config.task()])[0]
        fetch = store.start_event(page_id, "fetch")
        store.update_event(fetch, "succeeded", body=source_json, checkpoint={"source_event_id": fetch})
        interrupted_extract = store.start_event(page_id, "extract")

    with Storage(path) as store:
        store.resume_run("run", run_config, "fixture-revision")
        assert store.get_task(seed_id)["status"] == "complete"
        assert store.get_task(page_id)["status"] == "pending"
        assert store.list_events(page_id)[-1]["status"] == "uncertain"
        assert store.list_events(page_id)[-1]["event_id"] == interrupted_extract
        restored_config = parse_category_config(store.list_events(seed_id)[0]["body"], GLOVES_URL)
        page = decode_page(store.list_events(page_id)[0]["body"], restored_config, 0)
        assert page == expected
        extract = store.start_event(page_id, "extract")
        ids = store.commit_discovery(extract, page.targets, checkpoint=page.checkpoint)
        assert store.commit_discovery(extract, page.targets, checkpoint=page.checkpoint) == ids
        assert store.get_task(page_id)["checkpoint"] == page.checkpoint | {"source_event_id": fetch}
        assert store.get_task(page_id)["attempt_count"] == 2
        assert len(store.list_tasks("run")) == 5
        before = store.list_tasks("run")

    with Storage(path) as store:
        store.resume_run("run", run_config, "fixture-revision")
        assert store.list_tasks("run") == before


def test_cross_category_discovery_merges_origins_without_resetting_product_progress(
        tmp_path, settings, config, response):
    settings["request"].update(url=SURGICAL_URL, categoryId="937",
                               path="Dental Supplies /// Sutures & surgical products")
    surgical = parse_category_config(_html(settings), SURGICAL_URL)
    with Storage(tmp_path / "run.sqlite") as store:
        store.create_run("run", {}, "fixture-revision")
        for category in (config, surgical):
            task = store.enqueue_task("run", category.task())
            extract = store.start_event(task, "extract")
            page = decode_page(response, category, 0)
            ids = store.commit_discovery(extract, page.targets, checkpoint=page.checkpoint)
            if category == config:
                product_id = ids[0]
                fetch = store.start_event(product_id, "fetch")
                store.update_event(fetch, "succeeded", body=b"saved product evidence")
                before = store.get_task(product_id)

        products = [task for task in store.list_tasks("run") if task["kind"] == "product"]
        assert len(products) == 2
        after = store.get_task(product_id)
        assert after["status"] == before["status"]
        assert after["attempt_count"] == before["attempt_count"]
        assert after["checkpoint"] == before["checkpoint"]
        assert [origin["category_url"] for origin in after["discovered_from"]] == [
            GLOVES_URL, SURGICAL_URL,
        ]
        assert all(len(task["discovered_from"]) == 2 for task in products)
