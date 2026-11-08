import copy
import json
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pim_pipeline.transform import normalize, parquet_bytes

RECEIPT = {"uri": "fixture.json.gz", "sha256": "fixture", "ingested_at": "2026-11-05T12:00:00+00:00"}


def normalize_sample(sample):
    return normalize(json.loads(sample)[1:], "fixture", RECEIPT)


def test_types_and_parquet_roundtrip(sample):
    rows, rejected, duplicates = normalize_sample(sample)
    assert rejected == [] and duplicates == 0
    assert len(rows) == 3078
    assert rows[0]["valor"] == Decimal("94.97520")
    assert rows[0]["mes_referencia"] == date(2025, 1, 1)
    restored = pq.read_table(pa.BufferReader(parquet_bytes(rows))).to_pylist()
    assert restored == rows
    assert sum(r["valor"] is None for r in rows) == 1970


@pytest.mark.parametrize("raw,expected,status", [
    ("0", Decimal(0), "valido"), ("-", None, "sem_indice"), ("...", None, "indisponivel"),
    ("..", None, "nao_aplicavel"), ("X", None, "sigilo"), (None, None, "ausente"), ("", None, "ausente"),
])
def test_special_values(sample, raw, expected, status):
    row = json.loads(sample)[1]
    row["V"] = raw
    rows, rejected, _ = normalize([row], "fixture", RECEIPT)
    assert not rejected
    assert rows[0]["valor"] == expected
    assert rows[0]["status_valor"] == status
    assert rows[0]["valor_original"] == raw


@pytest.mark.parametrize("field,value", [("V", "NaN"), ("V", "n/a"), ("V", "1.1234567891"),
    ("V", "-1"), ("D3C", "202513"), ("NC", "6"), ("D1C", None)])
def test_invalid_values_are_reported(sample, field, value):
    row = json.loads(sample)[1]
    row[field] = value
    rows, rejected, _ = normalize([row], "fixture", RECEIPT)
    assert rows == []
    assert len(rejected) == 1 and rejected[0]["reason"]


def test_negative_percentage_is_valid(sample):
    row = json.loads(sample)[1]
    row.update(D2C="11602", V="-2.5")
    rows, rejected, _ = normalize([row], "fixture", RECEIPT)
    assert not rejected and rows[0]["valor"] == Decimal("-2.5")


def test_historical_unavailable_value_has_blank_unit(sample):
    row = json.loads(sample)[1]
    row.update(V="...", MC="", MN="")
    rows, rejected, _ = normalize([row], "fixture", RECEIPT)
    assert not rejected and rows[0]["valor"] is None
    assert rows[0]["unidade_codigo"] == ""


def test_duplicates_and_conflicts(sample):
    row = json.loads(sample)[1]
    rows, rejected, duplicates = normalize([row, copy.deepcopy(row)], "fixture", RECEIPT)
    assert len(rows) == 1 and duplicates == 1 and not rejected
    conflict = {**row, "V": "900"}
    _, rejected, duplicates = normalize([row, conflict], "fixture", RECEIPT)
    assert "conflitantes" in rejected[0]["reason"]
