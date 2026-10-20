"""Offline HTTP collection contracts and saved-evidence handoff to extraction."""

import copy
import gzip
import hashlib
import json
import logging
import ssl
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from safco_catalog_scraper.discovery import CategoryConfig, category_task
from safco_catalog_scraper.extraction import extract_product, finalize_product
from safco_catalog_scraper.fetching import Fetcher
from safco_catalog_scraper.storage import Storage, TaskSpec

PRODUCT_URL = "https://www.safcodental.com/product/fixture-gloves"
PRODUCT_REQUEST = {"method": "GET", "url": PRODUCT_URL}


class _Stream(httpx.SyncByteStream):
    def __init__(self, chunks, *, failure=None):
        self.chunks = chunks
        self.failure = failure
        self.chunks_read = 0
        self.closed = False

    def __iter__(self):
        for chunk in self.chunks:
            self.chunks_read += 1
            yield chunk
        if self.failure is not None:
            raise self.failure

    def close(self):
        self.closed = True


def _fetch(response, *, request=None, headers=None, **options):
    calls = []

    def handler(outgoing):
        calls.append(outgoing)
        return response

    with Fetcher(transport=httpx.MockTransport(handler), **options) as fetcher:
        result = fetcher.fetch(PRODUCT_REQUEST if request is None else request, headers=headers)
    assert len(calls) == 1
    return result, calls[0]


@pytest.fixture
def config():
    return CategoryConfig(
        category_url="https://www.safcodental.com/catalog/gloves",
        category_path="Dental Supplies /// Dental Exam Gloves",
        application_id="FIXTUREAPP", index_name="fixture_default_products", page_size=20,
        facet_filter="categories.level1:Dental Supplies /// Dental Exam Gloves",
        rule_contexts=("magento_filters", "", "magento-category-385"),
        api_key="fixture-public-search-key")


@pytest.fixture
def product_body():
    """Keep the observed embedded-data shape with fictional product identity."""
    row = {
        "sku": "000012", "name": "Fixture gloves small 100/bx", "description": "Small, 100/box",
        "manufacturer_name": "Source manufacturer", "manufacturer_part_number": None,
        "stock_availability": "in_stock", "product_price": "16.490000",
        "price": {
            "price": '<span data-price-type="finalPrice" data-price-amount="16.49"></span>',
            "tier_price": '<ul class="prices-tier"><li>10 <span data-price-amount="15.99"></span></li></ul>',
        },
        "tier_price": [{"price_qty": "10", "price": "15.990000", "website_price": "15.990000",
                        "all_groups": "1", "percentage_value": None}],
    }
    config = {"total": 1, "paginate": False, "page_limit": 20}
    return ('<h1>Fixture family</h1><nav class="breadcrumbs">'
            '<li class="category0"><a>Dental Supplies</a></li>'
            '<li class="category1"><a>Dental Exam Gloves</a></li></nav>'
            '<meta property="product:price:currency" content="USD">'
            '<script>window.masterData = ' + json.dumps(json.dumps({"a": row})) + ';</script>'
            '<script>window.productConfig = JSON.parse('
            + json.dumps(json.dumps(config)) + ');</script>').encode()


@pytest.mark.parametrize("url", [
    "https://www.safcodental.com/catalog/gloves",
    "https://www.safcodental.com/catalog/sutures-surgical-products",
    PRODUCT_URL + "?item=000012",
])
def test_public_get_returns_complete_evidence_and_utc_capture_time(url):
    body = "Fixed source wording ©".encode()
    before = datetime.now(timezone.utc)
    result, request = _fetch(httpx.Response(200, content=body),
                             request={"method": "GET", "url": url}, timeout_seconds=7.5)
    after = datetime.now(timezone.utc)
    assert result.succeeded
    assert result.status_code == 200
    assert result.body == body
    assert result.body_complete
    assert not result.retryable
    assert result.error_code is result.error_type is None
    captured = datetime.fromisoformat(result.captured_at)
    assert captured.utcoffset() == timedelta(0)
    assert before <= captured <= after
    assert type(result.elapsed_ms) is int and result.elapsed_ms >= 0
    assert request.method == "GET"
    assert str(request.url) == url
    assert request.content == b""
    assert request.headers["accept-encoding"] == "identity"
    assert set(request.extensions["timeout"]) == {"connect", "read", "write", "pool"}
    assert set(request.extensions["timeout"].values()) == {7.5}
    assert result.details["saved_bytes"] == len(body)
    assert "body" not in result.details
    json.dumps(result.details, allow_nan=False)


def test_empty_success_is_complete_evidence_for_the_parser_to_assess():
    result, _ = _fetch(httpx.Response(204))
    assert result.succeeded
    assert result.body_complete
    assert result.body == b""


def test_algolia_task_metadata_is_not_sent_as_request_parameters(config):
    task = config.task(1)
    before = copy.deepcopy(task.request)
    result, request = _fetch(httpx.Response(200, json={"results": []}), request=task.request,
                             headers=config.authentication_headers)
    assert result.succeeded
    assert request.method == "POST"
    assert request.url.host == "fixtureapp-dsn.algolia.net"
    assert request.url.path == "/1/indexes/*/queries"
    assert not request.url.query
    assert json.loads(request.content) == task.request["json"]
    assert request.headers["x-algolia-application-id"] == config.application_id
    assert request.headers["x-algolia-api-key"] == config.api_key
    assert task.request == before


def test_credentials_and_cookies_do_not_carry_into_the_next_request_or_logs(config, caplog):
    calls = []
    headers = config.authentication_headers
    before = dict(headers)

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=b"private response wording",
                              headers={"Set-Cookie": "session=private-cookie",
                                       "X-Api-Key": config.api_key})

    with caplog.at_level(logging.INFO), Fetcher(transport=httpx.MockTransport(handler)) as fetcher:
        api = fetcher.fetch(config.task().request, headers=headers)
        product = fetcher.fetch(PRODUCT_REQUEST)
    assert len(calls) == 2
    assert calls[0].headers["x-algolia-api-key"] == config.api_key
    assert "x-algolia-api-key" not in calls[1].headers
    assert "x-algolia-application-id" not in calls[1].headers
    assert all("cookie" not in request.headers for request in calls)
    assert headers == before
    for result in (api, product):
        assert "set-cookie" not in result.headers
        assert "x-api-key" not in result.headers
        assert config.api_key not in json.dumps(result.details)
        assert config.api_key not in repr(result)
        assert "private response wording" not in repr(result)
    assert config.api_key not in caplog.text
    assert "private-cookie" not in caplog.text
    assert "private response wording" not in caplog.text
    assert "HTTP fetch started method=POST" in caplog.text
    assert "HTTP fetch finished method=GET" in caplog.text
    assert "status=200 saved_bytes=24 complete=True" in caplog.text
    assert "elapsed_ms=" in caplog.text


@pytest.mark.parametrize("status,retryable", [
    (400, False), (401, False), (403, False), (404, False), (408, True), (429, True),
    (500, True), (501, False), (502, True), (503, True), (504, True), (505, False),
])
def test_http_errors_keep_the_body_and_return_retry_hints_without_retrying(status, retryable):
    result, _ = _fetch(httpx.Response(status, content=b"Source error evidence"))
    assert not result.succeeded
    assert result.status_code == status
    assert result.body == b"Source error evidence"
    assert result.body_complete
    assert result.error_code == "http_status"
    assert result.error_type is None
    assert result.retryable is retryable


@pytest.mark.parametrize("value", ["120", "Tue, 20 Oct 2026 12:02:00 GMT", "unrecognized delay"])
def test_retry_after_is_preserved_for_controller_validation(value):
    result, _ = _fetch(httpx.Response(429, headers={"Retry-After": value}))
    assert result.retryable
    assert result.retry_after == value
    assert result.details["response_headers"]["retry-after"] == value


@pytest.mark.parametrize("authenticated", [False, True])
def test_redirects_are_returned_without_following_or_forwarding_credentials(config, authenticated):
    location = "https://untrusted.invalid/another-page"
    result, outgoing = _fetch(httpx.Response(302, headers={"Location": location}),
                               request=config.task().request if authenticated else PRODUCT_REQUEST,
                               headers=config.authentication_headers if authenticated else None)
    assert outgoing.url.host != "untrusted.invalid"
    assert result.status_code == 302
    assert result.error_code == "redirect"
    assert result.headers["location"] == location
    assert result.body_complete
    assert not result.retryable
    assert not result.succeeded


def test_response_exactly_at_the_size_limit_remains_complete():
    stream = _Stream([b"ab", b"cd"])
    result, _ = _fetch(httpx.Response(200, stream=stream), max_response_bytes=4)
    assert result.succeeded
    assert result.body == b"abcd"
    assert result.body_complete
    assert stream.closed


@pytest.mark.parametrize("status,retryable", [(200, False), (429, True)])
def test_oversized_response_keeps_a_bounded_prefix_and_closes_the_stream(status, retryable):
    stream = _Stream([b"abc", b"def", b"must not be read"])
    result, _ = _fetch(httpx.Response(status, stream=stream, headers={"Retry-After": "20"}),
                       max_response_bytes=4)
    assert result.body == b"abcd"
    assert result.details["saved_bytes"] == 4
    assert not result.body_complete
    assert not result.succeeded
    assert result.error_code == "response_too_large"
    assert result.retryable is retryable
    assert stream.chunks_read == 2
    assert stream.closed


def test_response_limit_counts_decoded_bytes_when_the_server_still_uses_gzip():
    encoded = gzip.compress(b"a" * 4096)
    assert len(encoded) < 64
    stream = _Stream([encoded])
    result, _ = _fetch(httpx.Response(200, stream=stream, headers={
        "Content-Encoding": "gzip", "Content-Length": str(len(encoded))}), max_response_bytes=64)
    assert result.body == b"a" * 64
    assert result.details["saved_bytes"] == 64
    assert result.headers["content-length"] == str(len(encoded))
    assert result.error_code == "response_too_large"
    assert not result.succeeded
    assert not result.body_complete
    assert stream.closed


@pytest.mark.parametrize("error_class,code,retryable", [
    (httpx.ConnectTimeout, "timeout", True), (httpx.ReadTimeout, "timeout", True),
    (httpx.WriteTimeout, "timeout", True), (httpx.PoolTimeout, "timeout", True),
    (httpx.ConnectError, "network_error", True), (httpx.ReadError, "network_error", True),
    (httpx.RemoteProtocolError, "remote_protocol_error", True),
    (httpx.ProxyError, "request_error", False), (httpx.DecodingError, "request_error", False),
])
def test_transport_failures_return_safe_diagnostics_without_retrying(error_class, code, retryable, caplog):
    calls = []

    def handler(request):
        calls.append(request)
        raise error_class("sensitive exception wording")

    with caplog.at_level(logging.WARNING), Fetcher(transport=httpx.MockTransport(handler)) as fetcher:
        result = fetcher.fetch(PRODUCT_REQUEST)
    assert len(calls) == 1
    assert result.status_code is None
    assert result.body == b""
    assert not result.body_complete
    assert not result.succeeded
    assert result.error_code == code
    assert result.error_type == error_class.__name__
    assert result.retryable is retryable
    assert "sensitive exception wording" not in json.dumps(result.details)
    assert "sensitive exception wording" not in caplog.text
    assert "HTTP fetch finished" in caplog.text
    assert "error_type=" + error_class.__name__ in caplog.text


@pytest.mark.parametrize("status,retryable", [(200, True), (404, False), (429, True), (503, True)])
def test_interrupted_body_keeps_status_and_partial_evidence(status, retryable):
    stream = _Stream([b"Partial evidence"], failure=httpx.ReadTimeout("interrupted body"))
    result, _ = _fetch(httpx.Response(status, stream=stream))
    assert result.status_code == status
    assert result.body == b"Partial evidence"
    assert not result.body_complete
    assert not result.succeeded
    assert result.error_code == "timeout"
    assert result.error_type == "ReadTimeout"
    assert result.retryable is retryable
    assert stream.closed


def test_certificate_verification_failure_is_not_retried(caplog):
    calls = []

    def handler(request):
        calls.append(request)
        try:
            raise ssl.SSLCertVerificationError("private certificate details")
        except ssl.SSLCertVerificationError as error:
            raise httpx.ConnectError("private connection details") from error

    with caplog.at_level(logging.WARNING), Fetcher(transport=httpx.MockTransport(handler)) as fetcher:
        result = fetcher.fetch(PRODUCT_REQUEST)
    assert len(calls) == 1
    assert result.error_code == "certificate_verification"
    assert result.error_type == "ConnectError"
    assert not result.retryable
    assert not result.succeeded
    assert "private certificate details" not in caplog.text
    assert "private connection details" not in json.dumps(result.details)


@pytest.mark.parametrize("error_class", [httpx.LocalProtocolError, httpx.UnsupportedProtocol, RuntimeError])
def test_programmer_failures_propagate_instead_of_becoming_source_failures(error_class):
    error = error_class("local mistake")
    calls = []

    def handler(request):
        calls.append(request)
        raise error

    with Fetcher(transport=httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(error_class) as raised:
            fetcher.fetch(PRODUCT_REQUEST)
    assert raised.value is error
    assert len(calls) == 1


@pytest.mark.parametrize("task_request", [
    pytest.param({"method": "DELETE", "url": PRODUCT_URL}, id="unsupported-method"),
    pytest.param({"method": "GET", "url": "http://www.safcodental.com/product/a"}, id="insecure-scheme"),
    pytest.param({"method": "GET", "url": "https://other.invalid/product/a"}, id="external-host"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com.evil.invalid/product/a"},
                 id="hostname-suffix"),
    pytest.param({"method": "GET", "url": "https://user:secret@www.safcodental.com/product/a"},
                 id="embedded-credentials"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com:8443/product/a"}, id="different-port"),
    pytest.param({"method": "GET", "url": PRODUCT_URL + "#fragment"}, id="fragment"),
    pytest.param({"method": "GET", "url": PRODUCT_URL + "\r\n"}, id="control-characters"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com/account"}, id="unsupported-path"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com/product/"}, id="missing-product"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com/product/../account"}, id="dot-segment"),
    pytest.param({"method": "GET", "url": "https://www.safcodental.com/product/%2e%2e%2faccount"},
                 id="encoded-dot-segment"),
    pytest.param({**PRODUCT_REQUEST, "json": {}}, id="get-with-payload"),
    pytest.param({**PRODUCT_REQUEST, "headers": {"Authorization": "secret"}}, id="persisted-credentials"),
    pytest.param({"method": "POST", "url": PRODUCT_URL, "json": {}}, id="post-to-storefront"),
])
def test_invalid_request_is_rejected_before_any_network_operation(task_request):
    calls = []
    with Fetcher(transport=httpx.MockTransport(lambda outgoing: calls.append(outgoing))) as fetcher:
        with pytest.raises(ValueError):
            fetcher.fetch(task_request)
    assert calls == []


@pytest.mark.parametrize("case", ["missing-auth", "different-app", "extra-auth", "duplicate-auth",
                                 "invalid-value", "wrong-endpoint", "query-credentials", "invalid-json"])
def test_invalid_algolia_inputs_are_rejected_before_sending_credentials(config, case):
    request = copy.deepcopy(config.task().request)
    headers = config.authentication_headers
    if case == "missing-auth":
        headers = None
    elif case == "different-app":
        headers["X-Algolia-Application-Id"] = "ANOTHERAPP"
    elif case == "extra-auth":
        headers["Authorization"] = "secret"
    elif case == "duplicate-auth":
        headers["x-algolia-api-key"] = "another-key"
    elif case == "invalid-value":
        headers["X-Algolia-API-Key"] = "invalid\nheader"
    elif case == "wrong-endpoint":
        request["url"] = "https://FIXTUREAPP-dsn.algolia.net/1/indexes/another-index"
    elif case == "query-credentials":
        request["url"] += "?api_key=secret"
    else:
        request["json"] = {"bad": float("nan")}
    calls = []
    with Fetcher(transport=httpx.MockTransport(lambda outgoing: calls.append(outgoing))) as fetcher:
        with pytest.raises(ValueError):
            fetcher.fetch(request, headers=headers)
    assert calls == []


def test_algolia_credentials_cannot_be_sent_to_the_storefront(config):
    calls = []
    with Fetcher(transport=httpx.MockTransport(lambda outgoing: calls.append(outgoing))) as fetcher:
        with pytest.raises(ValueError):
            fetcher.fetch(PRODUCT_REQUEST, headers=config.authentication_headers)
    assert calls == []


@pytest.mark.parametrize("options", [
    {"timeout_seconds": 0}, {"timeout_seconds": float("inf")}, {"timeout_seconds": float("nan")},
    {"timeout_seconds": True}, {"max_response_bytes": 0}, {"max_response_bytes": True},
])
def test_invalid_fetch_limits_are_rejected(options):
    with pytest.raises(ValueError):
        Fetcher(**options)


def test_context_closes_the_transport_and_fetching_after_close_is_a_caller_error():
    class Transport(httpx.MockTransport):
        closed = False

        def close(self):
            self.closed = True
            super().close()

    transport = Transport(lambda request: httpx.Response(200))
    with Fetcher(transport=transport) as fetcher:
        assert fetcher.fetch(PRODUCT_REQUEST).succeeded
    assert transport.closed
    fetcher.close()
    with pytest.raises(RuntimeError, match="closed"):
        fetcher.fetch(PRODUCT_REQUEST)


def test_saved_fetch_replays_after_reopening_without_another_request(tmp_path, product_body):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=product_body, headers={"Content-Type": "text/html"})

    path = tmp_path / "run.sqlite"
    with Storage(path) as store:
        store.create_run("run", {}, "fixture-revision")
        task = store.enqueue_task("run", TaskSpec("product", PRODUCT_URL, PRODUCT_REQUEST))
        event = store.start_event(task, "fetch")
        with Fetcher(transport=httpx.MockTransport(handler)) as fetcher:
            fetched = fetcher.fetch(PRODUCT_REQUEST)
        assert fetched.succeeded
        store.update_event(event, "succeeded", body=fetched.body, details=fetched.details)
        interrupted = store.start_event(task, "extract")
        expected = finalize_product(extract_product(fetched.body, PRODUCT_URL, fetched.captured_at))

    with Storage(path) as store:
        store.resume_run("run", {}, "fixture-revision")
        events = {saved["event_id"]: saved for saved in store.list_events(task)}
        source = events[event]
        assert source["body"] == product_body
        assert source["content_hash"] == hashlib.sha256(product_body).hexdigest()
        assert source["details"] == fetched.details
        assert events[interrupted]["status"] == "uncertain"
        assert store.list_items(task) == []
        result = finalize_product(extract_product(source["body"], PRODUCT_URL, source["details"]["captured_at"]))
        assert result == expected
        acceptance = store.start_event(task, "extract", details={"source_event_id": event, **result.details})
        store.commit_product(acceptance, result.items, status=result.status,
                             expected_item_count=result.expected_item_count,
                             decoded_item_count=result.decoded_item_count, issues=result.issues)
        before = store.list_items(task)

    with Storage(path) as store:
        assert store.commit_product(acceptance, expected.items, status=expected.status,
                                    expected_item_count=expected.expected_item_count,
                                    decoded_item_count=expected.decoded_item_count,
                                    issues=expected.issues) == 1
        assert store.list_items(task) == before
        item, = before
        assert {key: item[key] for key in expected.items[0]} == expected.items[0]
        assert item["sku"] == "000012"
        assert item["base_price"] == "16.49"
        assert item["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
        assert item["selling_unit"] == "box" and item["pack_size"] == "100/bx"
        assert {note["reason"] for note in item["notes"]} == {"source_absent", "normalized_source_wording"}
        assert store.get_task(task)["status"] == "complete"
        assert store.list_events(task)[-1]["details"]["source_event_id"] == event
    assert len(calls) == 1


@pytest.mark.parametrize("interrupted", [False, True])
def test_failed_fetch_evidence_and_retry_hints_survive_reopening(tmp_path, interrupted):
    stream = _Stream([b"Saved error evidence"], failure=httpx.ReadTimeout("interrupted") if interrupted else None)
    response = httpx.Response(200 if interrupted else 429, stream=stream, headers={"Retry-After": "30"})
    path = tmp_path / "run.sqlite"
    with Storage(path) as store:
        store.create_run("run", {}, "fixture-revision")
        task = store.enqueue_task("run", category_task("https://www.safcodental.com/catalog/gloves"))
        event = store.start_event(task, "fetch")
        result, _ = _fetch(response, request=store.get_task(task)["request"])
        assert not result.succeeded
        store.update_event(event, "failed", body=result.body, details=result.details)

    with Storage(path) as store:
        store.resume_run("run", {}, "fixture-revision")
        saved, = store.list_events(task)
        assert saved["status"] == "failed"
        assert saved["body"] == b"Saved error evidence"
        assert saved["details"] == result.details
        assert saved["details"]["http_status"] == (200 if interrupted else 429)
        assert saved["details"]["body_complete"] is not interrupted
        assert saved["details"]["retryable"] is True
        assert saved["details"]["error_code"] == ("timeout" if interrupted else "http_status")
        assert saved["details"]["response_headers"]["retry-after"] == "30"
        assert store.list_items(task) == []
