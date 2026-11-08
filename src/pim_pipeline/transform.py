"""Normalização tipada e Silver por lote; marcadores nunca são imputados."""

import gzip
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from .ingestion import json_bytes, sha256
from .sidra import SidraError, validate_coverage

RULES_VERSION = "pim-1"
KEY = ("tabela_id", "nivel_territorial", "localidade_id", "periodo", "variavel_id", "classificacao_id", "categoria_id")
STRINGS = ["tabela_id", "nivel_territorial", "localidade_id", "localidade_nome", "periodo",
           "variavel_id", "variavel_nome", "classificacao_id", "categoria_id", "categoria_nome",
           "unidade_codigo", "unidade_nome", "valor_original", "status_valor", "run_id",
           "source_uri", "source_sha256", "rules_version"]
SCHEMA = pa.schema([(name, pa.string()) for name in STRINGS] + [
    ("mes_referencia", pa.date32()), ("valor", pa.decimal128(38, 9)),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])
# Na PIM, '-' também aparece em séries não divulgadas. Preservamos o sinal, sem virar zero.
MARKERS = {"-": "sem_indice", "..": "nao_aplicavel", "...": "indisponivel", "X": "sigilo", "x": "sigilo", "": "ausente"}


def normalize(rows, run_id, receipt):
    """Traduz os campos SIDRA, valida os valores e separa as linhas rejeitadas."""
    result, rejected, seen = [], [], {}
    duplicates = 0
    for index, row in enumerate(rows, 1):
        try:
            for field in ("NC", "D1C", "D2C", "D3C", "D4C"):
                if not isinstance(row[field], str) or not row[field].isdigit():
                    raise ValueError(f"Código inválido: {field}")
            if not isinstance(row["MC"], str) or (row["MC"] and not row["MC"].isdigit()):
                raise ValueError("Código de unidade inválido")
            if row["NC"] not in {"1", "2", "3"}:
                raise ValueError("Nível territorial inválido")
            if not re.fullmatch(r"\d{6}", row["D3C"]):
                raise ValueError("Período inválido")
            month = datetime.strptime(row["D3C"], "%Y%m").date()
            raw = row["V"]
            if raw is None:
                value, status = None, "ausente"
            elif isinstance(raw, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", raw):
                value, status = Decimal(raw), "valido"
                if value.as_tuple().exponent < -9 or value.copy_abs() >= Decimal("1e29"):
                    raise ValueError("Precisão excede NUMERIC(38,9)")
                if row["D2C"] in {"12606", "12607"} and value < 0:
                    raise ValueError("Número-índice negativo")
            elif raw in MARKERS:
                value, status = None, MARKERS[raw]
            else:
                raise ValueError("Valor ou marcador desconhecido")
            # Cada código da API recebe um nome de negócio; o valor original fica auditável.
            normalized = {
                "tabela_id": "8888", "nivel_territorial": row["NC"],
                "localidade_id": row["D1C"], "localidade_nome": row["D1N"], "periodo": row["D3C"],
                "variavel_id": row["D2C"], "variavel_nome": row["D2N"], "classificacao_id": "544",
                "categoria_id": row["D4C"], "categoria_nome": row["D4N"],
                "unidade_codigo": row["MC"], "unidade_nome": row["MN"],
                "valor_original": raw, "status_valor": status, "run_id": run_id,
                "source_uri": receipt["uri"], "source_sha256": receipt["sha256"], "rules_version": RULES_VERSION,
            }
            normalized.update(mes_referencia=month, valor=value, ingested_at=datetime.fromisoformat(receipt["ingested_at"]))
            key = tuple(normalized[k] for k in KEY)
            if key in seen:
                if seen[key] != normalized:
                    raise ValueError("Duplicatas conflitantes")
                duplicates += 1
                continue
            seen[key] = normalized
            result.append(normalized)
        except (KeyError, TypeError, ValueError) as exc:
            rejected.append({"row_number": index, "row": row, "reason": str(exc)})
    return result, rejected, duplicates


def parquet_bytes(rows):
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), buffer, compression="snappy")
    return buffer.getvalue().to_pybytes()


def transform(bronze, silver, run_id):
    """Lê um lote por vez, grava qualidade e Parquet, e fecha o manifesto Silver."""
    prefix = f"ibge/sidra/tabela=8888/run_id={run_id}"
    manifest_raw = bronze.get(f"{prefix}/manifest.json")
    if manifest_raw is None:
        raise ValueError("Bronze sem manifesto completo")
    source = json.loads(manifest_raw)
    if source["status"] != "complete":
        raise ValueError("Bronze incompleta")
    catalog = json.loads(bronze.get(f"{prefix}/catalog.json"))
    batches = []
    for receipt in source["batches"]:
        periods = receipt["periods"]
        key = f"{prefix}/ano={periods[0][:4]}/lote={periods[0]}-{periods[-1]}"
        raw = gzip.decompress(bronze.get(f"{key}.json.gz"))
        if sha256(raw) != receipt["sha256"]:
            raise SidraError("Checksum Bronze divergente")
        validate_coverage(raw, catalog, periods)
        rows, rejected, duplicates = normalize(json.loads(raw)[1:], run_id, receipt)
        report = {"input_rows": receipt["rows"], "output_rows": len(rows), "duplicates": duplicates,
                  "rejected": rejected, "statuses": dict(Counter(r["status_valor"] for r in rows))}
        report_raw = json_bytes(report)
        quality_uri = silver.put(f"{key}.quality-{sha256(report_raw)[:12]}.json", report_raw)
        if rejected:
            raise ValueError(f"Lote rejeitado: {len(rejected)} anomalias; consultar relatório de qualidade")
        content = parquet_bytes(rows)
        uri = silver.put(f"{key}.parquet", content, "application/octet-stream")
        batches.append({"key": f"{key}.parquet", "uri": uri, "periods": periods,
                        "rows": len(rows), "sha256": sha256(content), "statuses": report["statuses"], "quality_uri": quality_uri})
    manifest = {"run_id": run_id, "status": "complete", "scope": source["scope"],
                "bronze_manifest_sha256": sha256(manifest_raw), "rules_version": RULES_VERSION,
                "periods": source["periods"], "rows": sum(b["rows"] for b in batches), "batches": batches}
    silver.put(f"{prefix}/manifest.json", json_bytes(manifest))
    return manifest
