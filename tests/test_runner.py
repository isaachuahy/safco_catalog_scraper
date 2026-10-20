"""Offline runner contracts using real SQLite, a fixed clock, and crash workers."""

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

import safco_catalog_scraper.runner as runner
import safco_catalog_scraper.storage as storage_module
from safco_catalog_scraper.fetching import Fetcher
from safco_catalog_scraper.recovery import RecoveryPolicy
from safco_catalog_scraper.runner import RunConfig, run_catalog
from safco_catalog_scraper.storage import Storage

GLOVES_URL = "https://www.safcodental.com/catalog/gloves"
SURGICAL_URL = "https://www.safcodental.com/catalog/sutures-surgical-products"
PRODUCT_URLS = tuple("https://www.safcodental.com/product/fixture-" + str(i) for i in range(4))
CAPTURED_AT = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
SEARCH_KEY = "fixture-public-search-key"


class _Clock:
    def __init__(self):
        self.value = CAPTURED_AT
        self.waits = []
        self.interrupt_wait = False

    def now(self):
        return self.value

    def timestamp(self):
        return self.value.isoformat()

    def sleep(self, seconds):
        self.waits.append(seconds)
        if self.interrupt_wait:
            raise KeyboardInterrupt()
        self.value += timedelta(seconds=seconds)


def _set_clock(monkeypatch, clock):
    original_fetch = Fetcher.fetch

    def fetch(self, task_request, **kwargs):
        return replace(original_fetch(self, task_request, **kwargs), captured_at=clock.timestamp())

    monkeypatch.setattr(runner, "_now", clock.now)
    monkeypatch.setattr(storage_module, "_now", clock.timestamp)
    # Keep subprocess timeouts on the real clock while advancing runner waits instantly.
    monkeypatch.setattr(runner, "time", SimpleNamespace(sleep=clock.sleep))
    monkeypatch.setattr(Fetcher, "fetch", fetch)


def _config(**overrides):
    return RunConfig(**{"unresolved_product_limit": 10, "categories": (GLOVES_URL, SURGICAL_URL),
                        "request_interval_seconds": 0, "retry_base_seconds": 0, **overrides})


def _product_html(sku, *, name="Fixture item small 100/bx", description="Small, 100/box"):
    row = {
        "sku": sku, "name": name, "description": description,
        "manufacturer_name": "Private fixture source wording", "manufacturer_part_number": "0012-A",
        "stock_availability": "in_stock", "product_price": "16.490000",
        "price": {
            "price": '<span data-price-type="finalPrice" data-price-amount="16.49"></span>',
            "tier_price": '<ul class="prices-tier"><li>10 <span data-price-amount="15.99"></span></li></ul>',
        },
        "tier_price": [{"price_qty": "10", "price": "15.990000", "website_price": "15.990000",
                        "all_groups": "1", "percentage_value": None}],
    }
    return ('<h1>Fixture family</h1><nav class="breadcrumbs">'
            '<li class="category0"><a>Dental Supplies</a></li>'
            '<li class="category1"><a>Fixture category</a></li></nav>'
            '<meta property="product:price:currency" content="USD">'
            '<script>window.masterData = ' + json.dumps(json.dumps({"a": row})) + ';</script>'
            '<script>window.productConfig = JSON.parse('
            + json.dumps(json.dumps({"total": 1})) + ');</script>').encode()


class _FixtureSite:
    """Serve two categories, three API pages, and four distinct products with one shared URL."""

    def __init__(self, clock):
        self.clock = clock
        self.requests = []
        self.request_times = []
        self.product_calls = Counter()
        self.api_calls = []
        self.replies = {}
        self.category_html = {}
        self.pages = {}
        self.products = {url: _product_html(str(i).zfill(6)) for i, url in enumerate(PRODUCT_URLS)}
        self.filters = {}
        groups = ((0, 1, 2), (0, 3))
        for i, url in enumerate((GLOVES_URL, SURGICAL_URL)):
            path = "Dental Supplies /// Fixture category " + str(i)
            self.filters["categories.level1:" + path] = url
            settings = {
                "applicationId": "FIXTUREAPP", "apiKey": SEARCH_KEY,
                "indexName": "fixture_default", "hitsPerPage": 2,
                "isCategoryPage": True, "isPersonalizationEnabled": False,
                "instant": {"isVisualMerchEnabled": False},
                "request": {"url": url, "categoryId": str(i), "level": 1, "path": path,
                            "query": "", "refinementValue": "", "landingPageId": "",
                            "ruleContexts": {"facetFilters": "magento_filters",
                                             "merchCategoryPrefix": "magento-category-"}},
            }
            self.category_html[url] = ('<script>window.algoliaConfig = '
                                       + json.dumps(settings) + ';</script>').encode()
            hits = [{"objectID": str(index), "url": PRODUCT_URLS[index]} for index in groups[i]]
            for page in range((len(hits) + 1) // 2):
                self.pages[(url, page)] = {"results": [{
                    "index": "fixture_default_products", "nbHits": len(hits),
                    "nbPages": (len(hits) + 1) // 2, "page": page, "hitsPerPage": 2,
                    "exhaustiveNbHits": True, "hits": hits[page * 2:page * 2 + 2],
                }]}

    def __call__(self, request):
        self.requests.append(request)
        self.request_times.append(self.clock.now())
        url = str(request.url)
        if url in self.category_html:
            return httpx.Response(200, content=self.category_html[url])
        if request.method == "POST":
            query = json.loads(request.content)["requests"][0]
            params = parse_qs(query["params"])
            category = self.filters[json.loads(params["facetFilters"][0])[0][0]]
            page = int(params["page"][0])
            self.api_calls.append((category, page))
            return httpx.Response(200, json=self.pages[(category, page)])
        self.product_calls[url] += 1
        if url in self.replies:
            return self.replies[url](self.product_calls[url])
        return httpx.Response(200, content=self.products[url])


@pytest.fixture
def clock(monkeypatch):
    value = _Clock()
    _set_clock(monkeypatch, value)
    return value


@pytest.fixture
def site(clock):
    return _FixtureSite(clock)


def _run(path, site, config=None, *, resume=False):
    return run_catalog(path, "run", _config() if config is None else config, resume=resume,
                       transport=httpx.MockTransport(site))


def _product_task(store, url=PRODUCT_URLS[0]):
    return next(task for task in store.list_tasks("run") if task["target_key"] == url)


def _catalog(path):
    with Storage(path) as store:
        items = [{key: value for key, value in item.items()
                  if key not in {"item_id", "task_id", "extraction_event_id"}}
                 for task in store.list_tasks("run") for item in store.list_items(task["task_id"])]
    return sorted(items, key=lambda item: (item["product_url"], item["sku"]))


def test_complete_run_reconciles_both_categories_and_collects_the_shared_url_once(tmp_path, site):
    path = tmp_path / "run.sqlite"
    report = _run(path, site)
    assert report["status"] == "complete"
    assert report["stop_reason"] is None
    assert report["discovered_products"] == report["accepted_items"] == report["distinct_skus"] == 4
    assert report["fetch_attempts"] == len(site.requests) == 9
    assert report["product_outcomes"] == {"complete": 4}
    assert report["remaining_product_urls"] == report["unresolved"] == []
    assert set(site.api_calls) == {(GLOVES_URL, 0), (GLOVES_URL, 1), (SURGICAL_URL, 0)}
    assert site.product_calls == Counter({url: 1 for url in PRODUCT_URLS})
    assert all(category["complete"] for category in report["discovery"])
    assert [category["reported_hits"] for category in report["discovery"]] == [3, 2]
    with Storage(path) as store:
        task = _product_task(store)
        assert {origin["category_url"] for origin in task["discovered_from"]} == {GLOVES_URL, SURGICAL_URL}
        item, = store.list_items(task["task_id"])
        assert item["sku"] == "000000"
        assert item["captured_at"] == CAPTURED_AT.isoformat()
        assert item["base_price"] == "16.49"
        assert item["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
        assert item["selling_unit"] == "box" and item["pack_size"] == "100/bx"
        assert {note["reason"] for note in item["notes"]} == {"normalized_source_wording"}
        assert store.get_run("run")["status"] == "complete"
        assert all(event["type"] != "recovery" for task in store.list_tasks("run")
                   for event in store.list_events(task["task_id"]))
    json.dumps(report, allow_nan=False)

@pytest.mark.parametrize("interrupt_after_enable", [False, True])
def test_recovery_can_be_enabled_on_saved_work_without_repeating_discovery_or_fetch(
        tmp_path, site, monkeypatch, interrupt_after_enable):
    path = tmp_path / "enable-recovery.sqlite"
    site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(
        b"window.masterData", b"window.variantData")
    original = _config(unresolved_product_limit=1)
    assert _run(path, site, original)["status"] == "failed"
    discovery_calls = list(site.api_calls)
    configured = replace(original, recovery_endpoint="https://recovery.example/api/recovery",
                         recovery_policy=RecoveryPolicy("service", "model", "prompt"))
    decisions = []

    async def decide(request, timeout):
        decisions.append(request["request_id"])
        source = request["sources"][0]["source_event_id"]
        return {"request_id": request["request_id"], **request["expected"],
                "decision": {"action": "submit_candidates", "arguments": {"candidates": [
                    {"record": {"kind": "items"}, "evidence": [
                        {"evidence_id": "source:" + str(source), "locator": "script:0:variantData"}]}]}},
                "usage": {"input_tokens": 10, "output_tokens": 5}}

    def collect(**kwargs):
        return run_catalog(path, "run", configured, transport=httpx.MockTransport(site),
                           recovery_decide=decide, **kwargs)

    if interrupt_after_enable:
        enable = Storage.enable_recovery

        def interrupted(store, *args):
            enable(store, *args)
            raise KeyboardInterrupt()

        with monkeypatch.context() as patch:
            patch.setattr(Storage, "enable_recovery", interrupted)
            with pytest.raises(KeyboardInterrupt):
                collect(recover=True)
        report = collect(resume=True)
    else:
        report = collect(recover=True)
    assert report["status"] == "complete" and report["accepted_items"] == 4
    assert len(decisions) == report["recovery"]["budget"]["completed_model_turns"] == 1
    assert site.product_calls == Counter(dict.fromkeys(PRODUCT_URLS, 1))
    assert site.api_calls == discovery_calls
    with Storage(path) as store:
        audits = [event for task in store.list_tasks("run") for event in store.list_events(task["task_id"])
                  if event["details"].get("operation") == "enable_recovery"]
        assert len(audits) == 1



def test_threshold_stops_new_products_after_saving_usable_partial_results(tmp_path, site, caplog):
    path = tmp_path / "run.sqlite"
    site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item",
                                                description="Assorted sizes; pack quantities vary")
    site.replies[PRODUCT_URLS[1]] = lambda attempt: httpx.Response(404, content=b"missing product")
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.runner"):
        report = _run(path, site, _config(unresolved_product_limit=2))
    assert report["status"] == "partial"
    assert report["stop_reason"] == "unresolved_product_limit_reached"
    assert report["product_outcomes"] == {"partial": 1, "failed": 1, "pending": 2}
    assert set(report["remaining_product_urls"]) == set(PRODUCT_URLS[2:])
    assert report["accepted_items"] == 1
    assert site.product_calls[PRODUCT_URLS[2]] == site.product_calls[PRODUCT_URLS[3]] == 0
    assert "unresolved=2 limit=2 remaining=2" in caplog.text
    with Storage(path) as store:
        task = _product_task(store)
        item, = store.list_items(task["task_id"])
        assert item["selling_unit"] is item["pack_size"] is None
        assert {note["reason"] for note in item["notes"]} == {"unparsed_source"}
        reported = next(entry for entry in report["unresolved"] if entry["task_id"] == task["task_id"])
        assert reported["notes"] == [{"sku": item["sku"], **note} for note in item["notes"]]
        assert {note["source_text"] for note in reported["notes"]} == {
            "Unfamiliar item", "Assorted sizes; pack quantities vary"}
        failed = next(entry for entry in report["unresolved"] if entry["status"] == "failed")
        assert failed["notes"] == []
        assert failed["issues"]
        assert {issue["field"] for issue in task["issues"]} == {"selling_unit", "pack_size"}
        final = store.list_events(task["task_id"])[-1]
        assert final["details"]["finalized"] is True
        assert final["details"]["recovery_skipped_reason"]
        assert final["details"]["recovery_event_ids"] == []


@pytest.mark.parametrize("interrupt", [False, True])
def test_no_unresolved_limit_attempts_every_product_and_survives_resume(tmp_path, site, interrupt):
    path = tmp_path / "full-pass.sqlite"
    config = _config(unresolved_product_limit=None)
    site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item",
                                                description="Assorted sizes; pack quantities vary")
    site.replies[PRODUCT_URLS[1]] = lambda attempt: httpx.Response(404, content=b"missing product")
    if interrupt:
        def reply(attempt):
            if attempt == 1:
                raise KeyboardInterrupt()
            return httpx.Response(200, content=site.products[PRODUCT_URLS[2]])
        site.replies[PRODUCT_URLS[2]] = reply
        with pytest.raises(KeyboardInterrupt):
            _run(path, site, config)
    report = _run(path, site, config, resume=interrupt)
    assert report["status"] == "partial"
    assert report["stop_reason"] == "unresolved_work"
    assert report["product_outcomes"] == {"partial": 1, "failed": 1, "complete": 2}
    assert report["remaining_product_urls"] == []
    assert report["accepted_items"] == 3
    assert set(site.product_calls) == set(PRODUCT_URLS)
    with Storage(path) as store:
        assert store.get_run("run")["config"]["unresolved_product_limit"] is None


@pytest.mark.parametrize("empty", [False, True])
def test_products_without_usable_rows_remain_visible_when_the_limit_stops_collection(tmp_path, site, empty):
    path = tmp_path / "run.sqlite"
    body = (b'<h1>Family</h1><script>window.masterData = "[]";</script>'
            b'<script>window.productConfig = JSON.parse("{\\"total\\":0}");</script>'
            if empty else b"<h1>Family</h1>")
    site.products[PRODUCT_URLS[0]] = body
    report = _run(path, site, _config(unresolved_product_limit=1))
    assert report["status"] == "failed"
    assert report["accepted_items"] == 0
    assert set(report["remaining_product_urls"]) == set(PRODUCT_URLS[1:])
    unresolved, = report["unresolved"]
    assert unresolved["status"] == ("failed" if empty else "deferred")
    with Storage(path) as store:
        task = _product_task(store)
        assert task["stage"] == "recovery"
        assert store.list_items(task["task_id"]) == []
        events = store.list_events(task["task_id"])
        assert events[0]["body"] == body
        assert events[-1]["details"]["recovery_skipped_reason"]
        assert task["issues"][0]["source_event_id"] == events[0]["event_id"]


def test_a_broken_category_does_not_block_the_other_category(tmp_path, site):
    site.category_html[GLOVES_URL] = b"<html>Unfamiliar category structure</html>"
    report = _run(tmp_path / "run.sqlite", site)
    assert report["status"] == "partial"
    assert report["discovered_products"] == report["accepted_items"] == 2
    assert not report["discovery"][0]["complete"]
    assert report["discovery"][1]["complete"]
    assert report["unresolved"][0]["kind"] == "discovery"
    assert report["unresolved"][0]["status"] == "deferred"


@pytest.mark.parametrize("failure", ["status", "timeout"])
def test_transient_product_failures_retry_and_clear_the_resolved_issue(tmp_path, site, failure):
    path = tmp_path / "run.sqlite"

    def reply(attempt):
        if attempt == 1:
            if failure == "timeout":
                raise httpx.ReadTimeout("transient fixture failure")
            return httpx.Response(503, content=b"temporarily unavailable")
        return httpx.Response(200, content=site.products[PRODUCT_URLS[0]])

    site.replies[PRODUCT_URLS[0]] = reply
    report = _run(path, site, _config(max_fetch_attempts=2))
    assert report["status"] == "complete"
    assert site.product_calls[PRODUCT_URLS[0]] == 2
    with Storage(path) as store:
        task = _product_task(store)
        assert task["status"] == "complete" and task["issues"] == []
        assert task["attempt_count"] == 2
        fetches = [event for event in store.list_events(task["task_id"]) if event["type"] == "fetch"]
        assert [event["status"] for event in fetches] == ["failed", "succeeded"]
        assert fetches[0]["details"]["retry_decision"]["attempts"] == 1


@pytest.mark.parametrize("status,attempts,reason", [(404, 1, "permanent_failure"), (503, 3, "attempt_limit")])
def test_failed_products_stop_retrying_without_hiding_the_failure(tmp_path, site, status, attempts, reason):
    site.replies[PRODUCT_URLS[0]] = lambda attempt: httpx.Response(status, content=b"source failure")
    report = _run(tmp_path / "run.sqlite", site)
    assert report["status"] == "partial"
    assert report["accepted_items"] == 3
    assert report["remaining_product_urls"] == []
    assert site.product_calls[PRODUCT_URLS[0]] == attempts
    unresolved, = report["unresolved"]
    assert unresolved["status"] == "failed"
    assert unresolved["issues"][0]["reason"] == reason
    assert unresolved["issues"][0]["fetch_attempts"] == attempts


@pytest.mark.parametrize("header", ["120", "Tue, 20 Oct 2026 12:02:00 GMT",
                                    "Tuesday, 20-Oct-26 12:02:00 GMT", "Tue Oct 20 12:02:00 2026"])
def test_retry_after_pauses_all_requests_and_emits_waiting_logs(tmp_path, site, clock, caplog, header):
    site.replies[PRODUCT_URLS[0]] = lambda attempt: (
        httpx.Response(429, headers={"Retry-After": header}) if attempt == 1
        else httpx.Response(200, content=site.products[PRODUCT_URLS[0]]))
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.runner"):
        report = _run(tmp_path / "run.sqlite", site, _config(retry_max_seconds=1))
    assert report["status"] == "complete"
    first_product = next(i for i, request in enumerate(site.requests) if str(request.url) == PRODUCT_URLS[0])
    assert all(value >= CAPTURED_AT + timedelta(seconds=120)
               for value in site.request_times[first_product + 1:])
    assert sum(clock.waits) == 120
    assert max(clock.waits) <= 30
    assert "Run waiting run_id=run" in caplog.text


@pytest.mark.parametrize("status,header", [(429, "120"), (503, "Tue, 20 Oct 2026 12:02:00 GMT")])
@pytest.mark.parametrize("interrupt", [False, True])
def test_recovery_retry_after_pauses_collection_including_after_resume(
        tmp_path, site, clock, monkeypatch, status, header, interrupt):
    path = tmp_path / "recovery.sqlite"
    site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(
        b"window.masterData", b"window.variantData")
    monkeypatch.setattr("safco_catalog_scraper.evidence.datetime", SimpleNamespace(now=lambda zone: clock.now()))
    original_tools = runner.EvidenceTools
    fetches, decisions = [], []

    def response(request):
        fetches.append(request)
        return httpx.Response(status, headers={"Retry-After": header})

    def tools(*args, **kwargs):
        result = original_tools(*args, **kwargs)
        result.transport = httpx.MockTransport(response)
        return result

    async def decide(request, timeout):
        decisions.append(request["request_id"])
        return {"request_id": request["request_id"], **request["expected"],
                "decision": {"action": "fetch_evidence", "arguments": {"request": request["task_request"]}},
                "usage": {"input_tokens": 1, "output_tokens": 1}}

    monkeypatch.setattr(runner, "EvidenceTools", tools)
    config = _config(recovery_endpoint="https://recovery.example/api/recovery",
                     recovery_policy=RecoveryPolicy("service", "model", "prompt"))

    def collect(*, resume=False):
        return run_catalog(path, "run", config, resume=resume,
                           transport=httpx.MockTransport(site), recovery_decide=decide)

    if interrupt:
        original_update = Storage.update_event

        def save(store, event, state, **kwargs):
            original_update(store, event, state, **kwargs)
            if (kwargs.get("details") or {}).get("http_status") == status:
                raise KeyboardInterrupt()

        with monkeypatch.context() as patch:
            patch.setattr(Storage, "update_event", save)
            with pytest.raises(KeyboardInterrupt):
                collect()
        report = collect(resume=True)
    else:
        report = collect()
    assert report["status"] == "partial" and report["accepted_items"] == 3
    assert len(fetches) == len(decisions) == 1
    first_product = next(i for i, request in enumerate(site.requests) if str(request.url) == PRODUCT_URLS[0])
    assert all(value >= CAPTURED_AT + timedelta(seconds=120)
               for value in site.request_times[first_product + 1:])
    with Storage(path) as store:
        events = store.list_events(_product_task(store)["task_id"])
        fetched = next(event for event in events if event["details"].get("recovery_evidence"))
        assert fetched["details"]["response_headers"]["retry-after"] == header


def test_invalid_retry_after_uses_bounded_backoff_and_is_visible(tmp_path, site, clock, caplog):
    path = tmp_path / "run.sqlite"
    site.replies[PRODUCT_URLS[0]] = lambda attempt: httpx.Response(503, headers={"Retry-After": "tomorrow"})
    report = _run(path, site, _config(max_fetch_attempts=4, retry_base_seconds=1, retry_max_seconds=2))
    assert report["status"] == "partial"
    assert site.product_calls[PRODUCT_URLS[0]] == 4
    assert clock.waits == [1, 2, 2]
    assert "invalid_retry_after=True" in caplog.text
    with Storage(path) as store:
        decisions = [event["details"]["retry_decision"]
                     for event in store.list_events(_product_task(store)["task_id"])]
    assert all(decision["invalid_retry_after"] for decision in decisions)
    assert all(decision["server_not_before"] is None for decision in decisions)
    assert [decision["retry"] for decision in decisions] == [True, True, True, False]


def test_server_pause_still_applies_when_the_failed_product_has_no_attempts_left(tmp_path, site, clock):
    site.replies[PRODUCT_URLS[0]] = lambda attempt: httpx.Response(429, headers={"Retry-After": "5"})
    report = _run(tmp_path / "run.sqlite", site, _config(max_fetch_attempts=1))
    assert report["status"] == "partial"
    assert site.product_calls[PRODUCT_URLS[0]] == 1
    later_requests = [at for request, at in zip(site.requests, site.request_times)
                      if str(request.url) in PRODUCT_URLS[1:]]
    assert all(at >= CAPTURED_AT + timedelta(seconds=5) for at in later_requests)
    assert sum(clock.waits) == 5


def test_global_request_spacing_applies_to_html_api_and_product_requests(tmp_path, site, clock):
    report = _run(tmp_path / "run.sqlite", site, _config(request_interval_seconds=0.5))
    assert report["status"] == "complete"
    assert all((later - earlier).total_seconds() >= 0.5
               for earlier, later in zip(site.request_times, site.request_times[1:]))
    assert sum(clock.waits) == 4


def test_resume_honors_the_saved_global_retry_after_deadline(tmp_path, site, clock):
    path = tmp_path / "run.sqlite"
    site.replies[PRODUCT_URLS[0]] = lambda attempt: (
        httpx.Response(429, headers={"Retry-After": "5"}) if attempt == 1
        else httpx.Response(200, content=site.products[PRODUCT_URLS[0]]))
    clock.interrupt_wait = True
    with pytest.raises(KeyboardInterrupt):
        _run(path, site)
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "interrupted"
        event, = store.list_events(_product_task(store)["task_id"])
        deadline = event["details"]["retry_decision"]["server_not_before"]
        assert deadline == (CAPTURED_AT + timedelta(seconds=5)).isoformat()
    clock.interrupt_wait = False
    before = len(site.requests)
    report = _run(path, site, resume=True)
    assert report["status"] == "complete"
    assert all(at >= datetime.fromisoformat(deadline) for at in site.request_times[before:])
    assert len(site.api_calls) == 3
    assert site.product_calls[PRODUCT_URLS[0]] == 2


@pytest.mark.parametrize("attempt_limit", [1, 2])
def test_uncertain_fetches_consume_the_saved_attempt_allowance(tmp_path, site, attempt_limit):
    path = tmp_path / "run.sqlite"

    def reply(attempt):
        if attempt == 1:
            raise KeyboardInterrupt()
        return httpx.Response(200, content=site.products[PRODUCT_URLS[0]])

    site.replies[PRODUCT_URLS[0]] = reply
    config = _config(max_fetch_attempts=attempt_limit)
    with pytest.raises(KeyboardInterrupt):
        _run(path, site, config)
    report = _run(path, site, config, resume=True)
    assert report["status"] == ("partial" if attempt_limit == 1 else "complete")
    assert report["accepted_items"] == (3 if attempt_limit == 1 else 4)
    assert report["uncertain_fetches"] == 1
    assert site.product_calls[PRODUCT_URLS[0]] == attempt_limit
    assert len(site.api_calls) == 3


def test_resume_preserves_partial_items_notes_and_the_saved_agent_budget(tmp_path, site):
    path = tmp_path / "run.sqlite"
    budget = {"reserved_cases": 2, "consumed_cases": 1}
    site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")

    def reply(attempt):
        if attempt == 1:
            raise KeyboardInterrupt()
        return httpx.Response(200, content=site.products[PRODUCT_URLS[1]])

    site.replies[PRODUCT_URLS[1]] = reply
    with pytest.raises(KeyboardInterrupt):
        _run(path, site)
    with Storage(path) as store:
        first = _product_task(store)
        saved_items = store.list_items(first["task_id"])
        saved_events = store.list_events(first["task_id"])
        interrupted = _product_task(store, PRODUCT_URLS[1])
        store.update_event(store.list_events(interrupted["task_id"])[-1]["event_id"], "started", budget=budget)
    report = _run(path, site, resume=True)
    assert report["status"] == "partial"
    assert report["accepted_items"] == 4
    assert report["remaining_product_urls"] == []
    assert report["recovery"]["mode"] == "deferred"
    assert report["recovery"]["budget"] == budget
    assert site.product_calls[PRODUCT_URLS[0]] == 1
    with Storage(path) as store:
        assert store.list_items(first["task_id"]) == saved_items
        assert store.list_events(first["task_id"]) == saved_events
        assert all(event["type"] != "recovery" for task in store.list_tasks("run")
                   for event in store.list_events(task["task_id"]))


@pytest.mark.parametrize("change", ["configuration", "code_version"])
def test_incompatible_resume_fails_before_requests_or_state_changes(tmp_path, site, monkeypatch, change):
    path = tmp_path / "run.sqlite"

    def interrupt(attempt):
        raise KeyboardInterrupt()

    site.replies[PRODUCT_URLS[0]] = interrupt
    with pytest.raises(KeyboardInterrupt):
        _run(path, site)
    with Storage(path) as store:
        saved_run = store.get_run("run")
        saved_tasks = store.list_tasks("run")
        saved_events = [store.list_events(task["task_id"]) for task in saved_tasks]
    before = len(site.requests)
    config = _config(max_fetch_attempts=2) if change == "configuration" else _config()
    if change == "code_version":
        monkeypatch.setattr(runner, "_code_version", lambda: "changed-code-version")
    with pytest.raises(ValueError):
        _run(path, site, config, resume=True)
    assert len(site.requests) == before
    with Storage(path) as store:
        assert store.get_run("run") == saved_run
        assert store.list_tasks("run") == saved_tasks
        assert [store.list_events(task["task_id"]) for task in saved_tasks] == saved_events


def test_storage_failure_stops_without_another_write_and_resume_reuses_saved_source(tmp_path, site, monkeypatch):
    path = tmp_path / "run.sqlite"
    finishes = []

    def fail_commit(self, *args, **kwargs):
        raise sqlite3.OperationalError("fixture disk failure")

    with monkeypatch.context() as patch:
        patch.setattr(Storage, "commit_product", fail_commit)
        patch.setattr(Storage, "finish_run", lambda *args, **kwargs: finishes.append(args))
        with pytest.raises(sqlite3.OperationalError, match="fixture disk failure"):
            _run(path, site)
    assert finishes == []
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "running"
        task = _product_task(store)
        events = store.list_events(task["task_id"])
        assert events[0]["body"] == site.products[PRODUCT_URLS[0]]
        assert events[-1]["status"] == "started"
        assert store.list_items(task["task_id"]) == []
    report = _run(path, site, resume=True)
    assert report["status"] == "complete"
    assert site.product_calls[PRODUCT_URLS[0]] == 1


@pytest.mark.parametrize("pattern,expected_issue", [
    ("duplicate_object", "category_count_mismatch"),
    ("conflicting_url", "conflicting_object_urls"),
    ("changing_totals", "unknown_or_changing_totals"),
])
def test_discovery_reconciles_across_pages_before_claiming_complete_scope(tmp_path, site, pattern, expected_issue):
    page = site.pages[(GLOVES_URL, 1)]["results"][0]
    if pattern == "duplicate_object":
        page["hits"] = [{"objectID": "0", "url": PRODUCT_URLS[0]}]
    elif pattern == "conflicting_url":
        page["hits"] = [{"objectID": "0", "url": PRODUCT_URLS[2]}]
    else:
        page["nbHits"] = 4
        page["hits"].append({"objectID": "3", "url": PRODUCT_URLS[3]})
    report = _run(tmp_path / "run.sqlite", site)
    assert report["status"] == "partial"
    assert report["stop_reason"] == "discovery_incomplete"
    assert report["unresolved"] == []
    assert expected_issue in report["discovery"][0]["issues"]
    assert not report["discovery"][0]["complete"]
    assert report["discovery"][1]["complete"]


def test_empty_categories_complete_without_creating_product_tasks(tmp_path, site):
    for category in (GLOVES_URL, SURGICAL_URL):
        page = site.pages[(category, 0)]["results"][0]
        page.update(nbHits=0, nbPages=0, hits=[])
    report = _run(tmp_path / "run.sqlite", site)
    assert report["status"] == "complete"
    assert report["accepted_items"] == report["discovered_products"] == 0
    assert report["product_outcomes"] == {}
    assert all(category["complete"] for category in report["discovery"])
    assert site.product_calls == Counter()


def test_logs_surface_progress_and_recovery_without_raw_source_or_search_credentials(tmp_path, site, caplog):
    path = tmp_path / "run.sqlite"
    site.products[PRODUCT_URLS[0]] = b"<h1>Private fixture source wording</h1>"
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.runner"):
        report = _run(path, site)
    assert "Task processing run_id=run task_id=" in caplog.text
    assert "Run progress run_id=run discovered_products=4" in caplog.text
    assert "Recovery deferred run_id=run" in caplog.text
    assert "source_event_id=" in caplog.text
    assert "Run finished run_id=run status=partial discovered_products=4 accepted_items=3" in caplog.text
    assert SEARCH_KEY not in caplog.text
    assert "Private fixture source wording" not in caplog.text
    with Storage(path) as store:
        settings = store.get_run("run")["config"]
    assert SEARCH_KEY not in json.dumps(settings)
    assert SEARCH_KEY not in json.dumps(report)


_CRASH_WORKER = """
import os
import runpy
import signal
import sys
import pytest

fixtures = runpy.run_path(sys.argv[1])
clock = fixtures['_Clock']()
patch = pytest.MonkeyPatch()
fixtures['_set_clock'](patch, clock)
site = fixtures['_FixtureSite'](clock)
Storage = fixtures['Storage']
phase = sys.argv[3]
original_product = Storage.commit_product
original_discovery = Storage.commit_discovery

def product_commit(self, *args, **kwargs):
    if phase == 'product_before':
        os.kill(os.getpid(), signal.SIGKILL)
    result = original_product(self, *args, **kwargs)
    if phase == 'product_after':
        os.kill(os.getpid(), signal.SIGKILL)
    return result

def discovery_commit(self, *args, **kwargs):
    is_page = kwargs.get('checkpoint', {}).get('page') == 0
    if is_page and phase == 'discovery_before':
        os.kill(os.getpid(), signal.SIGKILL)
    result = original_discovery(self, *args, **kwargs)
    if is_page and phase == 'discovery_after':
        os.kill(os.getpid(), signal.SIGKILL)
    return result

def uncertain_fetch(attempt):
    os.kill(os.getpid(), signal.SIGKILL)

Storage.commit_product = product_commit
Storage.commit_discovery = discovery_commit
if phase == 'fetch_started':
    site.replies[fixtures['PRODUCT_URLS'][0]] = uncertain_fetch
fixtures['_run'](sys.argv[2], site)
"""


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="Requires an uncatchable process exit")
@pytest.mark.parametrize("phase", ["discovery_before", "discovery_after", "product_before",
                                 "product_after", "fetch_started"])
def test_abrupt_process_exit_resumes_to_the_same_catalog_as_an_uninterrupted_run(tmp_path, clock, phase):
    baseline = tmp_path / "baseline.sqlite"
    _run(baseline, _FixtureSite(clock))
    expected = _catalog(baseline)
    path = tmp_path / "crashed.sqlite"
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    crashed = subprocess.run([sys.executable, "-B", "-c", _CRASH_WORKER,
                              str(Path(__file__).resolve()), str(path), phase],
                             env=environment, capture_output=True, text=True, timeout=15)
    assert crashed.returncode == -signal.SIGKILL, crashed.stderr
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "running"
        tasks = store.list_tasks("run")
        saved_fetches = sum(event["type"] == "fetch" for task in tasks
                            for event in store.list_events(task["task_id"]))
        assert sum(len(store.list_items(task["task_id"])) for task in tasks) == (phase == "product_after")
    resumed_site = _FixtureSite(clock)
    report = _run(path, resumed_site, resume=True)
    uncertain = int(phase == "fetch_started")
    assert report["status"] == "complete"
    assert report["fetch_attempts"] == 9 + uncertain
    assert report["uncertain_fetches"] == uncertain
    assert len(resumed_site.requests) == 9 + uncertain - saved_fetches
    assert _catalog(path) == expected
