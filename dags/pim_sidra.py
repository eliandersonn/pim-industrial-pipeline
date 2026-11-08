"""Orquestração; datasets persistidos em GCS, XCom somente com referências."""

from datetime import timedelta
import hashlib
import json

import pendulum
from airflow.sdk import dag, task, get_current_context, Param


@dag(
    # Uma execução por vez evita duas publicações concorrentes da mesma Gold.
    dag_id="pim_sidra_8888", schedule="0 9 15 * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Fortaleza"), catchup=False,
    max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(minutes=5),
                                   "retry_exponential_backoff": True,
                                   "execution_timeout": timedelta(minutes=45)},
    params={"start": Param(None, type=["null", "string"], pattern=r"^\d{6}$"),
            "end": Param(None, type=["null", "string"], pattern=r"^\d{6}$"),
            "baseline_run_id": Param(None, type=["null", "string"])}
)
def pim_sidra():
    @task
    def validate_configuration():
        """Confere os destinos e identifica os arquivos desta execução."""
        from pim_pipeline.pipeline import cloud_config
        cfg = cloud_config()
        context = get_current_context()
        cfg["run_id"] = "airflow_" + hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
        cfg.update({key: context["params"][key] for key in ("start", "end", "baseline_run_id")})
        if (cfg["start"] or cfg["end"]) and not cfg["baseline_run_id"]:
            raise ValueError("Backfill parcial exige baseline_run_id completo")
        return cfg

    @task
    def ingest_bronze(cfg):
        """Guarda a resposta original; o XCom leva só o identificador e a contagem."""
        from pim_pipeline.ingestion import ingest
        from pim_pipeline.sidra import SidraClient
        from pim_pipeline.storage import BronzeStore
        manifest = ingest(SidraClient(), BronzeStore(cfg["bronze"]), cfg["run_id"], cfg["start"], cfg["end"])
        return {"run_id": manifest["run_id"], "rows": manifest["rows"]}

    @task
    def transform_silver(cfg, bronze_ref):
        """Trata os dados e, em um backfill, preserva os meses não revisados."""
        from pim_pipeline.storage import BronzeStore
        from pim_pipeline.transform import transform
        from pim_pipeline.pipeline import overlay
        store = BronzeStore(cfg["silver"])
        manifest = transform(BronzeStore(cfg["bronze"]), store, bronze_ref["run_id"])
        if manifest["scope"] == "partial":
            manifest = overlay(store, cfg["baseline_run_id"], bronze_ref["run_id"])
        return {"run_id": manifest["run_id"], "rows": manifest["rows"]}

    def loader(cfg):
        from pim_pipeline.loading import GoldLoader
        return GoldLoader(cfg["project"], cfg["dataset"], cfg["location"])

    @task
    def load_staging(cfg, silver_ref):
        """Leva os Parquet de 2025 para uma tabela temporária do BigQuery."""
        from pim_pipeline.pipeline import read_manifest
        from pim_pipeline.storage import BronzeStore
        state = loader(cfg).load(read_manifest(BronzeStore(cfg["silver"]), silver_ref["run_id"]))
        return {**state, "run_id": silver_ref["run_id"]}

    @task
    def build_validate_gold(cfg, state):
        """Calcula e valida a candidata sem tocar na tabela final."""
        return loader(cfg).build_and_validate(state)

    @task
    def publish_gold(cfg, state):
        """Publica somente a candidata que passou pelas validações anteriores."""
        return loader(cfg).publish(state)

    @task
    def validate_top3(cfg, state):
        """Confere a tabela publicada e executa o SQL Top 3 sobre ela."""
        client = loader(cfg)
        validation = client.validate(client.gold)
        top = client.top3()
        return {**state, "validation": validation, "top3_rows": len(top)}

    @task
    def record_metrics(cfg, state):
        """Registra os IDs dos jobs para rastrear e conferir a execução."""
        from pim_pipeline.ingestion import json_bytes
        from pim_pipeline.storage import BronzeStore
        result = {**state, "status": "complete"}
        key = f"results/{state['run_id']}/{state['publish_job_id']}.json"
        uri = BronzeStore(cfg["silver"]).put(key, json_bytes(result))
        print(json.dumps({"event": "pipeline_complete", "run_id": state["run_id"], "metrics_uri": uri}))
        return {"metrics_uri": uri}

    # Cada etapa usa a referência persistida pela anterior, sem datasets no XCom.
    cfg = validate_configuration()
    bronze = ingest_bronze(cfg)
    silver = transform_silver(cfg, bronze)
    staging = load_staging(cfg, silver)
    candidate = build_validate_gold(cfg, staging)
    published = publish_gold(cfg, candidate)
    verified = validate_top3(cfg, published)
    record_metrics(cfg, verified)


pim_sidra()
