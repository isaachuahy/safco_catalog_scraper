"""Hosted authentication, preflight certainty and bounded provider-call contracts."""

import json
from pathlib import Path

import httpx
import pytest

from test_recovery_flow import endpoint, SETTINGS, POLICY


def _request():
    return {"request_id": "case-1:1", "protocol_version": 1, "expected": {
        "service_version": POLICY.service_version, "model": POLICY.model, "prompt_version": POLICY.prompt_version},
        "case": {}, "history": [], "available_actions": ["finish_unresolved"]}


def test_luna_uses_xhigh_and_allows_reasoning_tokens():
    request, settings = _request(), dict(SETTINGS)
    request["expected"]["model"] = settings["SAFCO_RECOVERY_MODEL"] = "gpt-6-luna"

    def provider(received):
        body = json.loads(received.content)
        assert body["model"] == "gpt-6-luna"
        assert body["reasoning"] == {"effort": "xhigh"}
        assert body["max_output_tokens"] == 25_000
        assert body["store"] is False
        return httpx.Response(200, json={"id": "response-luna", "status": "completed", "output": [
            {"type": "function_call", "name": "finish_unresolved",
             "arguments": '{"reason":"No supporting evidence"}'}],
            "usage": {"input_tokens": 100, "output_tokens": 200}})

    code, result = endpoint.decide(request, token="Bearer " + settings["SAFCO_RECOVERY_TOKEN"],
                                  settings=settings, transport=httpx.MockTransport(provider))
    assert code == 200
    assert result["model"] == "gpt-6-luna"
    assert result["usage"] == {"input_tokens": 100, "output_tokens": 200}


def test_native_functions_constrain_evidence_ids_and_normalize_optional_read_arguments():
    request = _request()
    request.update(available_actions=["read_source", "submit_candidates"],
                   sources=[{"source_event_id": 11, "evidence_id": "source:11"}],
                   history=[{"evidence_id": "observation:4", "observation": {"source_event_id": 11}}])
    rejected = json.loads((Path(__file__).parent / "fixtures/recovery_rejected_references.json").read_text())

    def provider(received):
        body = json.loads(received.content)
        assert body["tool_choice"] == "required" and body["parallel_tool_calls"] is False
        tools = {tool["name"]: tool for tool in body["tools"]}
        assert all(tool["strict"] for tool in tools.values())
        reference = tools["submit_candidates"]["parameters"]["properties"]["candidates"]["items"]["properties"]["evidence"]["items"]
        permitted = reference["properties"]["evidence_id"]["enum"]
        assert set(permitted) == {"source:11", "observation:4"}
        assert all(value not in permitted for value in rejected["rejected_evidence_ids"])
        assert "text" not in body
        return httpx.Response(200, json={"id": "response-native", "status": "completed", "output": [
            {"type": "function_call", "name": "read_source", "arguments": json.dumps({
                "source_event_id": 11, "view": "json", "locator": "script:0:variantData",
                "offset": None, "limit_bytes": None})}], "usage": {"input_tokens": 100, "output_tokens": 20}})

    code, result = endpoint.decide(request, token="Bearer " + SETTINGS["SAFCO_RECOVERY_TOKEN"],
                                  settings=SETTINGS, transport=httpx.MockTransport(provider))
    assert code == 200
    assert result["decision"] == {"action": "read_source", "arguments": {
        "source_event_id": 11, "view": "json", "locator": "script:0:variantData"}}


def test_fetch_function_repeats_the_saved_request_without_model_supplied_urls():
    request = _request()
    request.update(available_actions=["fetch_evidence"], task_request={
        "method": "GET", "url": "https://www.safcodental.com/product/fixture"})
    def provider(received):
        body = json.loads(received.content)
        assert body["tools"][0]["parameters"]["properties"] == {}
        return httpx.Response(200, json={"id": "response-fetch", "status": "completed", "output": [
            {"type": "function_call", "name": "fetch_evidence", "arguments": "{}"}],
            "usage": {"input_tokens": 10, "output_tokens": 5}})
    code, result = endpoint.decide(request, token="Bearer " + SETTINGS["SAFCO_RECOVERY_TOKEN"],
                                  settings=SETTINGS, transport=httpx.MockTransport(provider))
    assert code == 200
    assert result["decision"] == {"action": "fetch_evidence", "arguments": {"request": request["task_request"]}}


def test_client_and_hosted_runtime_identities_agree():
    from safco_catalog_scraper.recovery import PROMPT_VERSION, SERVICE_VERSION
    assert (SERVICE_VERSION, PROMPT_VERSION) == (endpoint.SERVICE_VERSION, endpoint.PROMPT_VERSION)


@pytest.mark.parametrize("problem,status", [("token", 401), ("model", 400), ("key", 503), ("size", 413)])
def test_preflight_rejections_make_no_provider_request(problem, status):
    request, settings, token = _request(), dict(SETTINGS), "Bearer " + SETTINGS["SAFCO_RECOVERY_TOKEN"]
    if problem == "token":
        token = "Bearer wrong"
    elif problem == "model":
        request["expected"]["model"] = "different"
    elif problem == "key":
        settings.pop("OPENAI_API_KEY")
    else:
        request["history"] = ["x" * (endpoint.MAX_BYTES + 1)]
    def forbidden(_):
        pytest.fail("Preflight rejection must precede provider submission")
    code, result = endpoint.decide(request, token=token, settings=settings, transport=httpx.MockTransport(forbidden))
    assert code == status and result["request_id"] == request["request_id"]
    assert result["error"]["inference_started"] is False
    assert SETTINGS["OPENAI_API_KEY"] not in json.dumps(result)


@pytest.mark.parametrize("problem", ["timeout", "http_error", "incomplete", "refusal"])
def test_provider_failures_are_uncertain_and_never_retried(problem):
    calls = []
    def fail(request):
        calls.append(request)
        if problem == "timeout":
            raise httpx.ReadTimeout("private provider diagnostic", request=request)
        if problem == "http_error":
            return httpx.Response(429, text="private provider diagnostic")
        if problem == "incomplete":
            return httpx.Response(200, json={"status": "incomplete"})
        return httpx.Response(200, json={"status": "completed", "output": [
            {"type": "message", "content": [{"type": "refusal", "refusal": "private refusal"}]}]})
    code, result = endpoint.decide(_request(), token="Bearer " + SETTINGS["SAFCO_RECOVERY_TOKEN"],
                                   settings=SETTINGS, transport=httpx.MockTransport(fail))
    assert code == 502 and len(calls) == 1 and result["error"]["inference_started"] is True
    assert "private" not in json.dumps(result)


def test_public_demo_needs_no_client_token_and_keeps_provider_key_on_server(caplog):
    settings = {name: value for name, value in SETTINGS.items() if name != "SAFCO_RECOVERY_TOKEN"}
    settings.update(SAFCO_RECOVERY_ACCESS_MODE="public_demo",
                    SAFCO_RECOVERY_DEMO_EXPIRES_AT="2099-01-01T00:00:00Z")
    calls = []

    def provider(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer " + settings["OPENAI_API_KEY"]
        assert settings["OPENAI_API_KEY"] not in request.content.decode()
        return httpx.Response(200, json={"id": "public-demo", "status": "completed", "output": [
            {"type": "function_call", "name": "finish_unresolved", "arguments": '{"reason":"No evidence"}'}],
            "usage": {"input_tokens": 10, "output_tokens": 5}})

    code, result = endpoint.decide(_request(), token="", settings=settings,
                                   transport=httpx.MockTransport(provider))
    assert code == 200 and len(calls) == 1
    assert settings["OPENAI_API_KEY"] not in json.dumps(result) + caplog.text


@pytest.mark.parametrize("mode,expiry,status,reason", [
    ("public_demo", "", 503, "demo_not_configured"),
    ("public_demo", "invalid", 503, "demo_not_configured"),
    ("public_demo", "2099-01-01T00:00:00", 503, "demo_not_configured"),
    ("public_demo", "2099-01-01T00:00:00+01:00", 503, "demo_not_configured"),
    ("public_demo", "2000-01-01T00:00:00Z", 410, "demo_expired"),
    ("public", "2099-01-01T00:00:00Z", 503, "service_not_configured"),
])
def test_public_access_requires_explicit_mode_and_unexpired_utc_deadline(mode, expiry, status, reason):
    settings = dict(SETTINGS, SAFCO_RECOVERY_ACCESS_MODE=mode, SAFCO_RECOVERY_DEMO_EXPIRES_AT=expiry)

    def forbidden(_):
        pytest.fail("Invalid or expired demo must not call the provider")

    code, result = endpoint.decide(_request(), token="", settings=settings,
                                   transport=httpx.MockTransport(forbidden))
    assert code == status
    assert result["error"] == {"code": reason, "inference_started": False}


@pytest.mark.parametrize("code", ["project_spend_limit_exceeded", "organization_spend_limit_exceeded"])
def test_provider_spend_limit_is_reported_without_retry_or_inference(code):
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(429, json={"error": {"code": code, "message": "private billing details"}})

    status, result = endpoint.decide(_request(), token="Bearer " + SETTINGS["SAFCO_RECOVERY_TOKEN"],
                                     settings=SETTINGS, transport=httpx.MockTransport(provider))
    assert status == 429 and len(calls) == 1
    assert result["error"] == {"code": "provider_spend_limit", "inference_started": False}
    assert "private" not in json.dumps(result)
