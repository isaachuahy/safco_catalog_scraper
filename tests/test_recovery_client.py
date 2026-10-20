"""Offline transport guarantees for the single-attempt recovery client."""

import asyncio
import json

import httpx
import pytest

from safco_catalog_scraper.recovery import RecoveryActionError
from safco_catalog_scraper.recovery_client import RecoveryClient

URL = "https://recovery.example/api/recovery"
TOKEN = "private-reviewer-token"
REQUEST = {"request_id": "case-1:1"}


def _call(handler, *, request=REQUEST, timeout=1, token=TOKEN, **limits):
    async def invoke():
        async with RecoveryClient(URL, token, transport=httpx.MockTransport(handler), **limits) as client:
            return await client(request, timeout)
    return asyncio.run(invoke())


def test_public_demo_call_omits_authorization():
    def respond(request):
        assert "authorization" not in request.headers
        return httpx.Response(200, json={"ok": True})

    assert _call(respond, token=None) == {"ok": True}


def test_authentication_rejection_never_retries_anonymously():
    calls = []

    def reject(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer " + TOKEN
        return httpx.Response(401, json={"request_id": REQUEST["request_id"],
            "error": {"code": "unauthorized", "inference_started": False}})

    with pytest.raises(RecoveryActionError, match="service_unauthorized") as raised:
        _call(reject)
    assert not raised.value.uncertain and len(calls) == 1


@pytest.mark.parametrize("started,matching,uncertain", [(False, True, False), (True, True, True), (False, False, True)])
def test_only_matching_preflight_rejection_is_certain(started, matching, uncertain):
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(503, json={"request_id": REQUEST["request_id"] if matching else "different",
            "error": {"code": "service_unavailable", "inference_started": started}})
    with pytest.raises(RecoveryActionError) as raised:
        _call(respond)
    assert raised.value.uncertain is uncertain
    assert len(calls) == 1


@pytest.mark.parametrize("error,uncertain", [(httpx.ConnectError, False), (httpx.ConnectTimeout, False),
                                           (httpx.ReadError, True), (httpx.WriteError, True), (httpx.ReadTimeout, True)])
def test_transport_failures_are_single_attempt_and_do_not_expose_exception_text(error, uncertain, caplog):
    calls = []
    def fail(request):
        calls.append(request)
        raise error("secret transport details", request=request)
    with pytest.raises(RecoveryActionError) as raised:
        _call(fail)
    assert raised.value.uncertain is uncertain
    assert len(calls) == 1
    assert TOKEN not in caplog.text and "secret transport details" not in caplog.text


@pytest.mark.parametrize("body", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'[]', b'\xff', b'not json'])
def test_invalid_response_json_is_uncertain(body):
    with pytest.raises(RecoveryActionError, match="service_invalid_json") as raised:
        _call(lambda _: httpx.Response(200, content=body, headers={"content-type": "application/json"}))
    assert raised.value.uncertain


def test_redirect_is_not_followed_and_credentials_stay_on_the_configured_request():
    calls = []
    def redirect(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer " + TOKEN
        assert request.headers["x-request-id"] == REQUEST["request_id"]
        return httpx.Response(307, headers={"location": "https://other.example/steal"})
    with pytest.raises(RecoveryActionError, match="service_redirect"):
        _call(redirect)
    assert len(calls) == 1 and str(calls[0].url) == URL


def test_request_and_response_limits_bound_the_exchange():
    calls = []
    def response(request):
        calls.append(request)
        return httpx.Response(200, json={"data": "x" * 100})
    with pytest.raises(RecoveryActionError, match="service_request_limit") as raised:
        _call(response, max_request_bytes=1)
    assert not raised.value.uncertain and calls == []
    with pytest.raises(RecoveryActionError, match="service_response_limit") as raised:
        _call(response, max_response_bytes=16)
    assert raised.value.uncertain and len(calls) == 1


def test_timeout_cancels_the_request_without_retry():
    calls, cancelled = [], []
    async def slow(request):
        calls.append(request)
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.append(True)
    with pytest.raises(RecoveryActionError, match="service_timeout"):
        _call(slow, timeout=0.01)
    assert len(calls) == 1 and cancelled == [True]


def test_cookies_do_not_carry_between_decisions():
    calls = []
    def respond(request):
        calls.append(request)
        assert "cookie" not in request.headers
        assert json.loads(request.content) == REQUEST
        return httpx.Response(200, json={"ok": True}, headers={"set-cookie": "private=session"})
    async def invoke():
        async with RecoveryClient(URL, TOKEN, transport=httpx.MockTransport(respond)) as client:
            assert await client(REQUEST, 1) == {"ok": True}
            assert await client(REQUEST, 1) == {"ok": True}
    asyncio.run(invoke())
    assert len(calls) == 2
