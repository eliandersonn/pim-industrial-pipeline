"""Bronze: guarda a resposta original do IBGE e comprova sua cobertura."""

import gzip
import hashlib
import json
import logging
import re
from datetime import datetime, timezone

from .sidra import SidraError, plan_batches, validate_coverage, values_url

LOG = logging.getLogger(__name__)


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def ingest(client, store, run_id, start=None, end=None):
    """Coleta por lote; uma retomada reutiliza os lotes que já foram validados."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("run_id deve ter até 100 letras, números, '_' ou '-'")
    prefix = f"ibge/sidra/tabela=8888/run_id={run_id}"
    catalog = client.catalog()
    batches = plan_batches(catalog, start, end)
    catalog_raw = json_bytes(catalog)
    catalog_hash = sha256(catalog_raw)
    # O contrato imutável impede reutilizar run_id com catálogo ou recorte diferente.
    store.put(f"{prefix}/catalog.json", catalog_raw)
    request = {"run_id": run_id, "start": start, "end": end, "catalog_sha256": catalog_hash}
    store.put(f"{prefix}/request.json", json_bytes(request))
    previous = store.get(f"{prefix}/manifest.json")
    LOG.info(json.dumps({"event": "ingestion_started", "run_id": run_id, "batches": len(batches)}))
    receipts = []
    for periods in batches:
        key = f"{prefix}/ano={periods[0][:4]}/lote={periods[0]}-{periods[-1]}"
        url = values_url(catalog, periods)
        existing = store.get(f"{key}.metadata.json")
        if existing:
            receipt = json.loads(existing)
            compressed = store.get(f"{key}.json.gz")
            if compressed is None:
                raise SidraError("Recibo existe sem o objeto Bronze")
            raw = gzip.decompress(compressed)
            if sha256(raw) != receipt["sha256"] or receipt["url"] != url:
                raise SidraError("Checksum ou consulta diverge do recibo persistido")
            validate_coverage(raw, catalog, periods)
        else:
            raw = client.get(url)
            # Guarda o corpo original antes de validar suas observações.
            uri = store.put(f"{key}.json.gz", gzip.compress(raw, mtime=0), "application/gzip")
            coverage = validate_coverage(raw, catalog, periods)
            receipt = {"url": url, "uri": uri, "periods": periods, "sha256": sha256(raw),
                       "bytes": len(raw), "ingested_at": now(), **coverage}
            store.put(f"{key}.metadata.json", json_bytes(receipt))
        receipts.append(receipt)
        LOG.info(json.dumps({"event": "batch_validated", "run_id": run_id,
                             "periods": periods, "rows": receipt["rows"]}))
    # Um catálogo diferente no fim pode indicar atualização da fonte durante a coleta.
    if sha256(json_bytes(client.catalog())) != catalog_hash:
        raise SidraError("Catálogo mudou durante extração; iniciar novo run_id")
    if previous:
        manifest = json.loads(previous)
        if manifest["batches"] != receipts:
            raise SidraError("Manifesto diverge dos recibos persistidos")
        LOG.info(json.dumps({"event": "ingestion_reused", "run_id": run_id, "rows": manifest["rows"]}))
        return manifest
    manifest = {**request, "table_id": "8888", "status": "complete", "completed_at": now(),
                "scope": "full" if start is None and end is None else "partial",
                "periods": [p for batch in batches for p in batch],
                "rows": sum(r["rows"] for r in receipts), "batches": receipts}
    # O manifesto é o sinal de conclusão: só existe quando todos os lotes passaram.
    store.put(f"{prefix}/manifest.json", json_bytes(manifest))
    LOG.info(json.dumps({"event": "ingestion_complete", "run_id": run_id, "rows": manifest["rows"]}))
    return manifest
