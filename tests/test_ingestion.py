import copy
import gzip
import json
from unittest.mock import Mock

import pytest

from pim_pipeline.ingestion import ingest
from pim_pipeline.sidra import SidraError
from pim_pipeline.storage import BronzeStore


def client_for(catalog, raw):
    client = Mock()
    client.catalog.return_value = catalog
    client.get.return_value = raw
    return client


def test_real_fixture_roundtrip_and_repeated_run(tmp_path, catalog, sample):
    client = client_for(catalog, sample)
    store = BronzeStore(str(tmp_path))
    first = ingest(client, store, "fixture", "202501", "202501")
    second = ingest(client, store, "fixture", "202501", "202501")
    assert first == second
    assert first["rows"] == 3078
    assert first["scope"] == "partial"
    assert client.get.call_count == 1
    assert len(list(tmp_path.rglob("*.json.gz"))) == 1
    assert gzip.decompress(next(tmp_path.rglob("*.json.gz")).read_bytes()) == sample
    revised = ingest(client, store, "fixture_new", "202501", "202501")
    assert revised["rows"] == first["rows"]


def test_resume_validated_batch_after_partial_failure(tmp_path, tiny_catalog, sample):
    rows = json.loads(sample)
    january = json.dumps([rows[0], rows[1]]).encode()
    february_rows = copy.deepcopy([rows[0], rows[1]])
    february_rows[1]["D3C"] = "202502"
    # Dois anos geram lotes separados para testar a retomada após uma falha.
    tiny_catalog["periods"][1]["id"] = "202602"
    february_rows[1]["D3C"] = "202602"
    february = json.dumps(february_rows).encode()
    client = client_for(tiny_catalog, january)
    client.get.side_effect = [january, RuntimeError("simulated interruption")]
    store = BronzeStore(str(tmp_path))
    with pytest.raises(RuntimeError):
        ingest(client, store, "resume")
    assert not list(tmp_path.rglob("manifest.json"))
    client.get.side_effect = [february]
    final = ingest(client, store, "resume")
    assert final["rows"] == 2
    assert final["status"] == "complete"
    assert client.get.call_count == 3  # January was not fetched twice.


def test_catalog_change_prevents_publication(tmp_path, catalog, sample):
    client = client_for(catalog, sample)
    updated = copy.deepcopy(catalog)
    updated["periods"][0]["modificacao"] = "changed"
    client.catalog.side_effect = [catalog, updated]
    with pytest.raises(SidraError, match="Catálogo mudou"):
        ingest(client, BronzeStore(str(tmp_path)), "change", "202501", "202501")
    assert not list(tmp_path.rglob("manifest.json"))


def test_invalid_coverage_preserves_raw_but_no_success(tmp_path, catalog):
    client = client_for(catalog, b"[]")
    with pytest.raises(SidraError):
        ingest(client, BronzeStore(str(tmp_path)), "bad", "202501", "202501")
    assert len(list(tmp_path.rglob("*.json.gz"))) == 1
    assert not list(tmp_path.rglob("manifest.json"))


def test_immutable_objects_and_safe_local_path(tmp_path):
    store = BronzeStore(str(tmp_path))
    store.put("one.json", b"first")
    store.put("one.json", b"first")
    with pytest.raises(ValueError, match="diferente"):
        store.put("one.json", b"second")
    with pytest.raises(ValueError, match="fora"):
        store.put("../outside.json", b"bad")


def test_reusing_run_with_different_scope_fails(tmp_path, catalog, sample):
    client = client_for(catalog, sample)
    store = BronzeStore(str(tmp_path))
    ingest(client, store, "same_run", "202501", "202501")
    with pytest.raises(ValueError, match="diferente"):
        ingest(client, store, "same_run", "202501", "202502")


def test_resume_checks_raw_checksum(tmp_path, catalog, sample):
    client = client_for(catalog, sample)
    store = BronzeStore(str(tmp_path))
    ingest(client, store, "check", "202501", "202501")
    path = next(tmp_path.rglob("*.json.gz"))
    path.write_bytes(gzip.compress(b"[]"))
    with pytest.raises(SidraError, match="Checksum"):
        ingest(client, store, "check", "202501", "202501")


def test_gcs_creation_uses_precondition_without_real_cloud(monkeypatch):
    from google.cloud import storage
    from google.api_core.exceptions import PreconditionFailed

    client = Mock()
    client.bucket.return_value.name = "test-bucket"
    blob = client.bucket.return_value.blob.return_value
    monkeypatch.setattr(storage, "Client", lambda: client)
    store = BronzeStore("gs://test-bucket/bronze")
    assert store.put("one.json", b"payload") == "gs://test-bucket/bronze/one.json"
    assert blob.upload_from_string.call_args.kwargs["if_generation_match"] == 0
    blob.upload_from_string.side_effect = PreconditionFailed("exists")
    blob.download_as_bytes.return_value = b"payload"
    store.put("one.json", b"payload")
    blob.download_as_bytes.return_value = b"different"
    with pytest.raises(ValueError, match="diferente"):
        store.put("one.json", b"payload")
