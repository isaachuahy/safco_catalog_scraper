"""Saved-source scope, bounded observations and exact evidence location contracts."""

import asyncio
import json

import httpx
import pytest

from safco_catalog_scraper.evidence import EvidenceTools, _resolve
from safco_catalog_scraper.recovery import RecoveryActionError, RecoveryPolicy
from safco_catalog_scraper.storage import Storage, TaskSpec

POLICY = RecoveryPolicy("service", "model", "prompt")


@pytest.fixture
def evidence(tmp_path):
    with Storage(tmp_path / "catalog.sqlite") as store:
        store.create_run("run", {}, "fixture")
        request = {"method": "GET", "url": "https://www.safcodental.com/product/fixture"}
        task = store.enqueue_task("run", TaskSpec("product", request["url"], request))
        event = store.start_event(task, "fetch")
        data = {"items": [{"sku": "001", "price": "16.490000"}], "apiKey": "private-search-key"}
        body = ('<script>window.alternate = JSON.parse(' + json.dumps(json.dumps(data)) + ');</script>').encode()
        store.update_event(event, "succeeded", body=body, details={"http_status": 200, "body_complete": True,
            "request": request, "captured_at": "2026-10-20T00:00:00+00:00"}, checkpoint={"source_event_id": event})
        yield store, task, event, EvidenceTools(store, task, POLICY, max_response_bytes=1024)


def test_script_inventory_and_json_reads_are_bounded_and_redact_credentials(evidence):
    store, task, event, tools = evidence
    async def read(view, **args):
        return await tools({"task_id": task, "action": "read_source",
                            "arguments": {"source_event_id": event, "view": view, **args}}, 1)
    inventory = asyncio.run(read("scripts"))
    assert "script:0:alternate" in inventory["content"]
    data = asyncio.run(read("json", locator="script:0:alternate"))
    assert "private-search-key" not in data["content"]
    assert "[redacted]" in data["content"] and "16.490000" in data["content"]
    short = asyncio.run(read("json", locator="script:0:alternate", limit_bytes=10))
    assert short["truncated"] and len(short["content"].encode()) <= 10
    assert tools.resolve_location(event, "script:0:alternate#/items/0/sku") == "001"


def test_source_reads_reject_other_tasks_and_changed_saved_bytes(evidence):
    store, task, event, tools = evidence
    other = store.enqueue_task("run", TaskSpec("product", "other", {}))
    source = store.start_event(other, "fetch")
    store.update_event(source, "succeeded", body=b"{}", details={"http_status": 200, "body_complete": True})
    with pytest.raises(ValueError):
        tools.source(source)
    store._db.execute("UPDATE task_events SET body = ? WHERE event_id = ?", (b"changed", event))
    with pytest.raises(ValueError):
        tools.source(event)


def test_additional_fetch_does_not_promote_evidence_or_follow_redirects(evidence):
    store, task, event, tools = evidence
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(302, headers={"location": "https://other.example/"})
    tools.transport = httpx.MockTransport(respond)
    request = store.get_task(task)["request"]
    with pytest.raises(RecoveryActionError, match="evidence_http_failed"):
        asyncio.run(tools({"task_id": task, "action": "fetch_evidence", "arguments": {"request": request}}, 1))
    assert len(calls) == 1
    assert store.get_task(task)["checkpoint"]["source_event_id"] == event
    assert store.list_events(task)[-1]["status"] == "failed"


def test_cross_product_fetch_is_rejected_before_network_or_persistence(evidence):
    store, task, _, tools = evidence
    before = store.list_events(task)
    request = {"method": "GET", "url": "https://www.safcodental.com/product/other"}
    with pytest.raises(RecoveryActionError, match="evidence_request_mismatch"):
        asyncio.run(tools({"task_id": task, "action": "fetch_evidence", "arguments": {"request": request}}, 1))
    assert store.list_events(task) == before


def test_cancelled_evidence_fetch_is_persisted_as_uncertain(evidence):
    store, task, primary, tools = evidence
    async def slow(_):
        await asyncio.sleep(10)
    tools.transport = httpx.MockTransport(slow)
    async def invoke():
        async with asyncio.timeout(0.01):
            await tools({"task_id": task, "action": "fetch_evidence",
                         "arguments": {"request": store.get_task(task)["request"]}}, 1)
    with pytest.raises(TimeoutError):
        asyncio.run(invoke())
    assert store.list_events(task)[-1]["status"] == "uncertain"
    assert store.get_task(task)["checkpoint"]["source_event_id"] == primary


@pytest.mark.parametrize("locator", ["script:0:alternate#/", "script:0:alternate#bad", "script:0:alternate#/items/-1",
                                     "script:0:alternate#/items/00", "script:0:alternate#/bad~x"])
def test_invalid_evidence_locations_are_rejected(evidence, locator):
    _, task, event, tools = evidence
    with pytest.raises(RecoveryActionError, match="evidence_location_invalid"):
        asyncio.run(tools({"task_id": task, "action": "read_source",
            "arguments": {"source_event_id": event, "view": "json", "locator": locator}}, 1))


def test_empty_json_pointer_key_is_distinct_from_the_root():
    body = b'<script>window.variantData = {"": {"sku": "empty-key"}, "a": {"sku": "000000"}};</script>'
    assert _resolve(body, "script:0:variantData#/") == {"sku": "empty-key"}
    assert _resolve(body, "script:0:variantData")["a"]["sku"] == "000000"
