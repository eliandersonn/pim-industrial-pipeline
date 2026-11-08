import copy
import json
from unittest.mock import Mock

import pytest
import requests

from pim_pipeline.sidra import SidraClient, SidraError, plan_batches, validate_catalog, validate_coverage, values_url


def response(status=200, content=b"[]", headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = content
    result._content_consumed = True
    result.headers.update(headers or {})
    return result


def test_real_sample_has_complete_keys(catalog, sample):
    assert validate_coverage(sample, catalog, ["202501"]) == {
        "rows": 3078, "expected_rows": 3078, "markers": {"-": 1970}}


def test_full_plan_includes_every_period_once(catalog):
    batches = plan_batches(catalog)
    assert len(batches) == 25
    assert [p for batch in batches for p in batch] == [p["id"] for p in catalog["periods"]]
    assert all(len(batch) * 3078 <= 40000 for batch in batches)
    assert plan_batches(catalog, "202501", "202512") == [[f"2025{month:02}" for month in range(1, 13)]]
    assert len(plan_batches(catalog, "202501", "202512", max_values=6156)) == 6
    url = values_url(catalog, ["202501"])
    assert "/n1/1/n2/2/n3/" in url
    assert "56689" in url
    assert "/p/202501/" in url


@pytest.mark.parametrize("start,end", [("202513", None), ("202502", "202501"), ("2025", None), ("190001", "190002")])
def test_invalid_periods(catalog, start, end):
    with pytest.raises(ValueError):
        plan_batches(catalog, start, end)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unexpected", "swapped_header", "invalid_type"])
def test_bad_coverage_is_rejected(catalog, sample, mutation):
    rows = json.loads(sample)
    if mutation == "missing":
        rows.pop()
    elif mutation == "duplicate":
        rows[-1] = rows[1]  # Same count, but a missing key is hidden by a duplicate.
    elif mutation == "unexpected":
        rows[1]["D3C"] = "202502"
    elif mutation == "swapped_header":
        rows[0]["D2C"], rows[0]["D3C"] = rows[0]["D3C"], rows[0]["D2C"]
    else:
        rows[1]["V"] = None
    with pytest.raises(SidraError):
        validate_coverage(json.dumps(rows).encode(), catalog, ["202501"])


def test_invalid_catalog_rejected(catalog):
    changed = copy.deepcopy(catalog)
    changed["periods"].append(changed["periods"][0])
    with pytest.raises(SidraError):
        validate_catalog(changed)


@pytest.mark.parametrize("failure", [requests.Timeout(), requests.ConnectionError(), response(500), response(429, headers={"Retry-After": "3"})])
def test_retry_transient_errors(failure):
    session, sleep = Mock(), Mock()
    session.get.side_effect = [failure, response()]
    assert SidraClient(session, sleep).get("https://example.test") == b"[]"
    assert session.get.call_count == 2
    assert sleep.call_count == 1
    assert session.get.call_args.kwargs["timeout"] == (10, 120)


def test_retry_limit():
    session = Mock()
    session.get.side_effect = requests.Timeout()
    with pytest.raises(requests.Timeout):
        SidraClient(session, Mock()).get("https://example.test")
    assert session.get.call_count == 4


@pytest.mark.parametrize("status", [400, 403, 404])
def test_permanent_error_not_retried(status):
    session = Mock()
    session.get.return_value = response(status)
    with pytest.raises(requests.HTTPError):
        SidraClient(session, Mock()).get("https://example.test")
    assert session.get.call_count == 1


@pytest.mark.parametrize("raw", [b"<html>error</html>", b"{", b""])
def test_invalid_json_response(raw):
    session = Mock()
    session.get.return_value = response(content=raw)
    with pytest.raises(SidraError):
        SidraClient(session, Mock()).get("https://example.test")


def test_json_error_object_is_not_a_dataset(catalog):
    with pytest.raises(SidraError):
        validate_coverage(b'{"error":"unavailable"}', catalog, ["202501"])
