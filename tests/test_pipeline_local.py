import json

import pytest

from pim_pipeline.ingestion import ingest
from pim_pipeline.pipeline import analyze_local, overlay, read_batch
from pim_pipeline.storage import BronzeStore
from pim_pipeline.transform import transform
from test_ingestion import client_for


def test_fixture_integration_and_idempotence(tmp_path, catalog, sample):
    bronze = BronzeStore(str(tmp_path / "bronze"))
    silver = BronzeStore(str(tmp_path / "silver"))
    ingest(client_for(catalog, sample), bronze, "fixture", "202501", "202501")
    first = transform(bronze, silver, "fixture")
    second = transform(bronze, silver, "fixture")
    assert first == second and first["rows"] == 3078
    one = analyze_local(silver, "fixture", tmp_path / "result")
    two = analyze_local(silver, "fixture", tmp_path / "result")
    assert one == two
    assert one["gold_rows"] == 408
    assert one["valid_indices"] == 167
    assert one["top3_rows"] == 51


def test_partial_backfill_preserves_other_months(tmp_path, tiny_catalog, sample):
    bronze = BronzeStore(str(tmp_path / "bronze"))
    silver = BronzeStore(str(tmp_path / "silver"))
    rows = json.loads(sample)
    jan, feb = rows[1], {**rows[1], "D3C": "202502", "V": "200"}
    client = client_for(tiny_catalog, json.dumps([rows[0], jan, feb]).encode())
    ingest(client, bronze, "baseline")
    transform(bronze, silver, "baseline")
    patch_client = client_for(tiny_catalog, json.dumps([rows[0], {**jan, "V": "999"}]).encode())
    ingest(patch_client, bronze, "patch", "202501", "202501")
    transform(bronze, silver, "patch")
    merged = overlay(silver, "baseline", "patch")
    result = read_batch(silver, merged["batches"][0])
    assert {r["periodo"]: str(r["valor"]) for r in result} == {"202501": "999.000000000", "202502": "200.000000000"}
    assert merged == overlay(silver, "baseline", "patch")
    assert merged["scope"] == "full" and merged["rows"] == 2


def test_transform_rejects_raw_corruption(tmp_path, catalog, sample):
    bronze = BronzeStore(str(tmp_path / "bronze"))
    ingest(client_for(catalog, sample), bronze, "fixture", "202501", "202501")
    import gzip
    file = next((tmp_path / "bronze").rglob("*.json.gz"))
    file.write_bytes(gzip.compress(b"[]"))
    with pytest.raises(ValueError, match="Checksum"):
        transform(bronze, BronzeStore(str(tmp_path / "silver")), "fixture")


def test_cloud_cli_reexecution_keeps_receipts_for_both_publications(tmp_path, monkeypatch):
    """Same snapshot may produce new BigQuery jobs without a receipt conflict."""
    import sys
    from pim_pipeline import pipeline, loading

    class Loader:
        publication = 0

        def __init__(self, *args):
            pass

        def load(self, manifest):
            return {}

        def build_and_validate(self, state):
            return state

        def publish(self, state):
            Loader.publication += 1
            return {"publish_job_id": f"publish-{Loader.publication}"}

        def top3(self):
            return [1, 2, 3]

    monkeypatch.setattr(pipeline, "cloud_config", lambda: {
        "bronze": str(tmp_path / "bronze"), "silver": str(tmp_path / "silver"),
        "project": "project-test", "dataset": "pim", "location": "southamerica-east1"})
    monkeypatch.setattr(pipeline, "transform", lambda *args: {"scope": "full", "run_id": "same-snapshot"})
    monkeypatch.setattr(loading, "GoldLoader", Loader)
    monkeypatch.setattr(sys, "argv", ["pipeline", "--cloud", "--from-bronze", "--run-id", "same-snapshot"])
    pipeline.main()
    pipeline.main()
    receipts = [json.loads(p.read_text()) for p in (tmp_path / "silver/results").rglob("*.json")]
    assert {r["publish_job_id"] for r in receipts} == {"publish-1", "publish-2"}
