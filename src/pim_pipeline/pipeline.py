"""Entradas de operação; Airflow transporta somente referências a manifestos."""

import argparse
import json
import logging
import os
import re
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .analytics import gold_rows, top3_rows
from .ingestion import ingest, json_bytes, sha256
from .sidra import SidraClient
from .storage import BronzeStore
from .transform import RULES_VERSION, parquet_bytes, transform


def read_manifest(store, run_id):
    """Abre somente uma execução concluída; lotes isolados não formam um snapshot."""
    raw = store.get(f"ibge/sidra/tabela=8888/run_id={run_id}/manifest.json")
    if raw is None:
        raise ValueError("Manifesto não encontrado")
    result = json.loads(raw)
    if result["status"] != "complete":
        raise ValueError("Snapshot incompleto")
    return result


def read_batch(store, batch):
    """Confere o hash antes de usar o Parquet no reprocessamento ou na análise."""
    raw = store.get(batch["key"])
    if raw is None or sha256(raw) != batch["sha256"]:
        raise ValueError("Objeto Silver ausente ou checksum divergente")
    return pq.read_table(pa.BufferReader(raw)).to_pylist()


def overlay(silver, baseline_id, patch_id):
    """Substitui somente meses solicitados; nunca apaga meses fora do backfill."""
    baseline, patch = read_manifest(silver, baseline_id), read_manifest(silver, patch_id)
    if baseline["scope"] != "full" or baseline["rules_version"] != patch["rules_version"]:
        raise ValueError("Baseline deve ser completo e usar as mesmas regras")
    result_id = patch_id + "_merged"
    prefix = f"ibge/sidra/tabela=8888/run_id={result_id}"
    patch_periods = set(patch["periods"])
    periods = sorted(set(baseline["periods"]) | patch_periods)
    batches = []
    for year in sorted({p[:4] for p in periods}):
        rows = []
        for source, replacing in ((baseline, False), (patch, True)):
            for batch in source["batches"]:
                if not any(p.startswith(year) for p in batch["periods"]):
                    continue
                rows.extend(r for r in read_batch(silver, batch) if r["periodo"].startswith(year)
                            and (replacing or r["periodo"] not in patch_periods))
        keys = {(r["nivel_territorial"], r["localidade_id"], r["periodo"], r["variavel_id"], r["categoria_id"]) for r in rows}
        if len(keys) != len(rows):
            raise ValueError("Backfill produziu chaves duplicadas")
        for row in rows:
            row["run_id"] = result_id
        key = f"{prefix}/ano={year}/part-000.parquet"
        content = parquet_bytes(rows)
        uri = silver.put(key, content, "application/octet-stream")
        batches.append({"key": key, "uri": uri, "periods": [p for p in periods if p.startswith(year)],
                        "rows": len(rows), "sha256": sha256(content)})
    manifest = {"run_id": result_id, "scope": "full", "status": "complete", "periods": periods,
                "rules_version": RULES_VERSION, "baseline_run_id": baseline_id, "patch_run_id": patch_id,
                "rows": sum(b["rows"] for b in batches), "batches": batches}
    silver.put(f"{prefix}/manifest.json", json_bytes(manifest))
    return manifest


def analyze_local(silver, run_id, output):
    """Referência local da Gold; não substitui execução SQL no BigQuery."""
    manifest = read_manifest(silver, run_id)
    rows = []
    for batch in manifest["batches"]:
        if any(p.startswith("2025") for p in batch["periods"]):
            rows.extend(read_batch(silver, batch))
    gold = gold_rows(rows)
    top = top3_rows(gold)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for name, data in (("gold_2025", gold), ("top3_2025", top)):
        table = pa.Table.from_pylist(data)
        temporary = output / f".{name}.{uuid4().hex}.parquet"
        pq.write_table(table, temporary, compression="snappy")
        temporary.replace(output / f"{name}.parquet")
        (output / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
    metrics = {"run_id": run_id, "silver_rows": manifest["rows"], "gold_rows": len(gold),
               "scope": manifest["scope"],
               "valid_indices": sum(r["indice_atividade"] is not None for r in gold), "top3_rows": len(top),
               "uf_month_groups": len({(r["uf_codigo"], r["mes_referencia"]) for r in gold}),
               "cloud_executed": False}
    (output / "metrics.json").write_bytes(json_bytes(metrics))
    return metrics


def cloud_config():
    """Exige destinos explícitos para não gravar em outro projeto por engano."""
    required = ("GCS_BRONZE_BUCKET", "GCS_SILVER_BUCKET", "GOOGLE_CLOUD_PROJECT", "BQ_DATASET", "BQ_LOCATION")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Configuração ausente: {', '.join(missing)}")
    for name in ("GCS_BRONZE_BUCKET", "GCS_SILVER_BUCKET"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", os.environ[name]):
            raise ValueError("Nome de bucket inválido")
    from .loading import table_id
    table_id(os.environ["GOOGLE_CLOUD_PROJECT"], os.environ["BQ_DATASET"], "pim_transformacao_2025")
    return {"bronze": f"gs://{os.environ['GCS_BRONZE_BUCKET']}", "silver": f"gs://{os.environ['GCS_SILVER_BUCKET']}",
            "project": os.environ["GOOGLE_CLOUD_PROJECT"], "dataset": os.environ["BQ_DATASET"], "location": os.environ["BQ_LOCATION"]}


def main():
    """Executa o mesmo fluxo pelo terminal; sem --cloud, tudo fica em disco local."""
    parser = argparse.ArgumentParser(description="Pipeline IBGE: Bronze, Silver e análise")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bronze", default="data/bronze")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--output", default="data/analytics")
    parser.add_argument("--from-bronze", action="store_true", help="Sem HTTP: reutilizar Bronze completa existente")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--baseline-run-id", help="Snapshot Silver completo a preservar no backfill")
    parser.add_argument("--cloud", action="store_true", help="Executa GCS/BigQuery; requer ambiente autorizado")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    config = cloud_config() if args.cloud else None
    bronze = BronzeStore(config["bronze"] if config else args.bronze)
    silver = BronzeStore(config["silver"] if config else args.silver)
    if not args.from_bronze:
        ingest(SidraClient(), bronze, args.run_id, args.start, args.end)
    manifest = transform(bronze, silver, args.run_id)
    if manifest["scope"] != "full" and args.baseline_run_id:
        manifest = overlay(silver, args.baseline_run_id, args.run_id)
    if args.cloud:
        from .loading import GoldLoader
        loader = GoldLoader(config["project"], config["dataset"], config["location"])
        state = loader.build_and_validate(loader.load(manifest))
        result = loader.publish(state)
        result["top3_rows"] = len(loader.top3())
        # Cada publicação tem novos IDs de jobs; guardamos seu próprio recibo.
        key = f"results/{manifest['run_id']}/{result['publish_job_id']}.json"
        result["metrics_uri"] = silver.put(key, json_bytes(result))
    else:
        result = analyze_local(silver, manifest["run_id"], args.output)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
