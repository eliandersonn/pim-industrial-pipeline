from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from pim_pipeline.loading import COLUMN_DESCRIPTIONS, GoldLoader


def table_fixture():
    return bigquery.Table("project-test.pim.candidate", schema=[
        bigquery.SchemaField("indice_atividade", "NUMERIC", precision=38, scale=9)])


def test_partial_snapshot_never_loads():
    client = Mock()
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    with pytest.raises(ValueError, match="completo"):
        loader.load({"scope": "partial", "status": "complete"})
    client.load_table_from_uri.assert_not_called()


def test_candidate_validation_failure_does_not_publish():
    client = Mock()
    build = Mock()
    validation = Mock()
    validation.result.return_value = [{"rows_count": 4896, "invalid_rows": 1}]
    client.query.side_effect = [build, validation]
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    with pytest.raises(ValueError, match="inválida"):
        loader.build_and_validate({"stage": "project-test.pim.staging", "candidate": "project-test.pim.candidate"})
    assert client.query.call_count == 2
    assert "CREATE OR REPLACE" not in client.query.call_args.args[0]


def test_publish_is_atomic_replacement_with_explicit_no_expiration():
    client = Mock()
    client.get_table.return_value = table_fixture()
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    loader.retain_auxiliaries = Mock(return_value=[])
    loader.publish({"candidate": "project-test.pim.val_pim_transformacao_2025_aaaaaaaaaaaaaaaa"})
    sql = client.query.call_args.args[0]
    assert "CREATE OR REPLACE TABLE" in sql and "expiration_timestamp=NULL" in sql
    assert COLUMN_DESCRIPTIONS["indice_atividade"] in sql
    assert '"camada", "gold"' in sql
    assert client.query.call_args.kwargs["job_config"].maximum_bytes_billed == 1_000_000_000
    client.query.return_value.result.assert_called_once()
    loader.retain_auxiliaries.assert_called_once_with("aaaaaaaaaaaaaaaa", None)


def test_load_uses_only_2025_uris_and_checks_count():
    client = Mock()
    client.get_table.return_value = table_fixture()
    client.load_table_from_uri.return_value.output_rows = 36936
    client.load_table_from_uri.return_value.job_id = "load_fixture"
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    manifest = {"run_id": "fixture", "scope": "full", "status": "complete",
        "periods": [f"2025{month:02}" for month in range(1, 13)],
        "batches": [{"periods": ["202401"], "uri": "gs://silver/2024.parquet", "rows": 1},
                    {"periods": ["202501"], "uri": "gs://silver/2025.parquet", "rows": 36936}]}
    state = loader.load(manifest)
    assert state["load_job_id"] == "load_fixture"
    assert ".stg_pim_sidra_2025_" in state["stage"]
    assert ".val_pim_transformacao_2025_" in state["candidate"]
    assert client.update_table.call_args.args[0].schema[0].description
    assert client.load_table_from_uri.call_args.args[0] == ["gs://silver/2025.parquet"]
    assert client.load_table_from_uri.call_args.kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"
    client.load_table_from_uri.return_value.output_rows = 1
    with pytest.raises(ValueError, match="divergente"):
        loader.load(manifest)


def test_validation_and_ranking_success_before_publish():
    client = Mock()
    client.get_table.return_value = table_fixture()
    validation = Mock()
    validation.result.return_value = [{"rows_count": 4896, "invalid_rows": 0}]
    ranking = Mock()
    ranking.result.return_value = [{"atividade_codigo": "129317"}]
    client.query.side_effect = [Mock(), validation, ranking]
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    state = loader.build_and_validate({"stage": "project-test.pim.staging", "candidate": "project-test.pim.candidate"})
    assert state["validation"]["invalid_rows"] == 0 and state["top3_rows"] == 1
    assert all("CREATE OR REPLACE" not in call.args[0] for call in client.query.call_args_list)


def test_descriptions_cover_silver_and_gold_columns():
    from pathlib import Path
    import pyarrow.parquet as pq
    from pim_pipeline.transform import SCHEMA
    gold = pq.read_schema(Path(__file__).resolve().parents[1] / "docs/evidencias/gold_2025.parquet")
    assert set(SCHEMA.names + gold.names) <= COLUMN_DESCRIPTIONS.keys()
    assert all(COLUMN_DESCRIPTIONS[name].strip() for name in SCHEMA.names + gold.names)


def test_describe_preserves_schema_properties_and_existing_labels():
    client = Mock()
    table = table_fixture()
    table.labels = {"responsavel": "equipe_dados"}
    client.get_table.return_value = table
    GoldLoader("project-test", "pim", "southamerica-east1", client).describe(table.full_table_id, "validacao", "coleta_1")
    result = client.update_table.call_args.args[0]
    assert result.schema[0].precision == 38 and result.schema[0].scale == 9
    assert result.labels["responsavel"] == "equipe_dados" and result.labels["camada"] == "validacao"
    assert "coleta_1" in result.description


def test_unknown_column_requires_a_description_before_publication():
    client = Mock()
    client.get_table.return_value = bigquery.Table("project-test.pim.candidate", schema=[
        bigquery.SchemaField("campo_novo", "STRING")])
    with pytest.raises(ValueError, match="sem descrição"):
        GoldLoader("project-test", "pim", "southamerica-east1", client).publish({"candidate": "project-test.pim.candidate"})
    client.query.assert_not_called()


@pytest.mark.parametrize("republication", [False, True])
def test_publication_retains_previous_successful_execution_on_retry(republication):
    client = Mock()
    candidate, gold = table_fixture(), table_fixture()
    gold.labels = {"execucao_atual": "aaaaaaaaaaaaaaaa" if republication else "bbbbbbbbbbbbbbbb",
                   "execucao_anterior": "bbbbbbbbbbbbbbbb" if republication else "cccccccccccccccc"}
    client.get_table.side_effect = [candidate, gold]
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    loader.retain_auxiliaries = Mock(return_value=[])
    loader.publish({"candidate": "project-test.pim.val_pim_transformacao_2025_aaaaaaaaaaaaaaaa"})
    loader.retain_auxiliaries.assert_called_once_with("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")
    assert '("execucao_anterior", "bbbbbbbbbbbbbbbb")' in client.query.call_args.args[0]


def test_failed_publication_never_cleans_auxiliaries():
    client = Mock()
    client.get_table.return_value = table_fixture()
    client.query.return_value.result.side_effect = RuntimeError("Publicação falhou")
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    loader.retain_auxiliaries = Mock()
    with pytest.raises(RuntimeError):
        loader.publish({"candidate": "project-test.pim.val_pim_transformacao_2025_aaaaaaaaaaaaaaaa"})
    loader.retain_auxiliaries.assert_not_called()


def test_first_publication_without_gold_retains_only_current_execution():
    from google.api_core.exceptions import NotFound
    client = Mock()
    client.get_table.side_effect = [table_fixture(), NotFound("Primeira carga")]
    loader = GoldLoader("project-test", "pim", "southamerica-east1", client)
    loader.retain_auxiliaries = Mock(return_value=[])
    loader.publish({"candidate": "project-test.pim.val_pim_transformacao_2025_aaaaaaaaaaaaaaaa"})
    loader.retain_auxiliaries.assert_called_once_with("aaaaaaaaaaaaaaaa", None)


def test_retention_preserves_two_pairs_lineage_and_unrelated_tables():
    from datetime import datetime, timezone
    client = Mock()
    tables = {}
    for suffix in ("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"):
        for prefix in ("stg_pim_sidra_2025_", "val_pim_transformacao_2025_"):
            name = "project-test.pim." + prefix + suffix
            table = bigquery.Table(name, schema=table_fixture().schema)
            table.expires = datetime(2026, 11, 8, tzinfo=timezone.utc)
            table.description = f"Descrição antiga. Coleta de origem (run_id): coleta_{suffix}."
            tables[name] = table
    tables["project-test.pim.outra_tabela"] = bigquery.Table("project-test.pim.outra_tabela")
    client.get_table.side_effect = tables.__getitem__
    client.update_table.side_effect = lambda table, fields: table
    client.list_tables.return_value = list(tables.values())
    removed = GoldLoader("project-test", "pim", "southamerica-east1", client).retain_auxiliaries(
        "aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")
    assert removed == ["stg_pim_sidra_2025_cccccccccccccccc", "val_pim_transformacao_2025_cccccccccccccccc"]
    assert client.delete_table.call_count == 2
    for name, table in tables.items():
        if name.endswith(("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")):
            assert table.expires is None
            assert "Coleta de origem (run_id): coleta_" in table.description
            assert all(field.description for field in table.schema)


def test_missing_retained_pair_prevents_deletions():
    from google.api_core.exceptions import NotFound
    client = Mock()
    client.get_table.side_effect = NotFound("Tabela ausente")
    with pytest.raises(NotFound):
        GoldLoader("project-test", "pim", "southamerica-east1", client).retain_auxiliaries("aaaaaaaaaaaaaaaa")
    client.delete_table.assert_not_called()
