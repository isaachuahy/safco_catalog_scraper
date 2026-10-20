"""Explicitly opt into one paid live recovery case over fixed fixture source pages."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from safco_catalog_scraper.exporting import export_catalog
from safco_catalog_scraper.recovery import PROMPT_VERSION, SERVICE_VERSION, RecoveryPolicy
from safco_catalog_scraper.runner import run_catalog
from test_runner import PRODUCT_URLS, _Clock, _FixtureSite, _config, _set_clock


@pytest.mark.skipif(os.environ.get("SAFCO_LIVE_RECOVERY") != "1",
                    reason="Set SAFCO_LIVE_RECOVERY=1 explicitly to allow paid inference")
def test_real_endpoint_recovers_saved_fixture_data(tmp_path, monkeypatch):
    """Use real endpoint/model calls; require source-grounded acceptance and report usage."""
    required = ("SAFCO_RECOVERY_ENDPOINT", "SAFCO_RECOVERY_TOKEN", "SAFCO_RECOVERY_MODEL")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.fail("Configure these environment variables before the live check: " + ", ".join(missing))
    run_id = "live-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    directory = Path(__file__).parents[1] / "runs" / "live-recovery" / run_id
    directory.mkdir(parents=True)
    clock = _Clock()
    _set_clock(monkeypatch, clock)
    site = _FixtureSite(clock)
    site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(
        b"window.masterData", b"window.variantData")
    policy = RecoveryPolicy(SERVICE_VERSION, os.environ["SAFCO_RECOVERY_MODEL"], PROMPT_VERSION,
        max_cases=1, max_model_turns_per_run=4, max_model_turns_per_case=4, max_http_attempts_per_case=0)
    config = _config(unresolved_product_limit=1,
        recovery_endpoint=os.environ["SAFCO_RECOVERY_ENDPOINT"], recovery_policy=policy)
    report = run_catalog(directory / "catalog.sqlite", run_id, config,
        transport=httpx.MockTransport(site), recovery_access_token=os.environ["SAFCO_RECOVERY_TOKEN"])
    receipt = {"verification": {"mode": "real endpoint and model; fixed fixture source pages",
        "max_model_calls": 4, "model": policy.model}, "final_report": report}
    report_path = directory / "report.json"
    report_path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n")
    catalog_path = export_catalog(directory / "catalog.sqlite", run_id, directory / "catalog.json")
    print("Live recovery report:", report_path)
    print("Live recovery catalog:", catalog_path)
    assert report["status"] == "complete", "Recovery remains unresolved; inspect the saved report"
    assert report["accepted_items"] == 4
    assert report["recovery"]["outcomes"] == {"proposal": 1}
    assert 1 <= report["recovery"]["budget"]["completed_model_turns"] <= 4
    catalog = json.loads(catalog_path.read_text())
    item = next(item for product in catalog["products"] for item in product["items"] if item["sku"] == "000000")
    assert item["base_price"] == "16.49"
    assert item["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
    assert site.product_calls[PRODUCT_URLS[0]] == 1
