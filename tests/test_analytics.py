import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from pim_pipeline.analytics import ACTIVITIES, gold_rows, top3_rows


def record(category, value, **overrides):
    return {"nivel_territorial": "3", "variavel_id": "12606", "periodo": "202501",
            "categoria_id": str(category), "categoria_nome": "Atividade", "localidade_id": "23",
            "localidade_nome": "Ceará", "mes_referencia": date(2025, 1, 1),
            "valor": Decimal(str(value)) if value is not None else None,
            "status_valor": "valido" if value is not None else "sem_indice", "run_id": "fixture", **overrides}


def test_mean_differences_and_nulls():
    rows = gold_rows([record(129317, 80), record(129318, 100), record(129319, 120), record(129320, None)])
    assert [r["media_indices_uf_mes"] for r in rows] == [Decimal(100)] * 4
    assert [r["diferenca"] for r in rows] == [Decimal(-20), Decimal(0), Decimal(20), None]
    assert all(r["quantidade_atividades_validas"] == 3 for r in rows)
    assert [r["atividade_codigo"] for r in top3_rows(rows)] == ["129319", "129318", "129317"]


def test_excludes_aggregates_indicators_and_territories():
    rows = [record(c, 1000) for c in (129314, 129315, 129316)]
    rows.extend([record(129317, 999, nivel_territorial="1"), record(129317, 999, variavel_id="12607"),
                 record(129317, 999, periodo="202401"), record(56689, 90)])
    result = gold_rows(rows)
    assert len(result) == 1 and result[0]["indice_atividade"] == 90


def test_ties_and_small_groups():
    rows = [record(129317, 120), record(56689, 120), record(129318, 100), record(129319, 90)]
    assert [r["atividade_codigo"] for r in top3_rows(gold_rows(rows))] == ["56689", "129317", "129318"]
    assert len(top3_rows(gold_rows(rows[:1]))) == 1
    assert len(top3_rows(gold_rows(rows[:2]))) == 2
    assert top3_rows(gold_rows([record(129317, None)])) == []
    assert gold_rows([record(129317, None)])[0]["media_indices_uf_mes"] is None


def test_revision_recomputes_every_difference():
    result = gold_rows([record(129317, 90), record(129318, 100), record(129319, 120)])
    assert all(r["media_indices_uf_mes"] == Decimal("103.333333333") for r in result)
    assert [r["diferenca"] for r in result] == [Decimal("-13.333333333"), Decimal("-3.333333333"), Decimal("16.666666667")]
    with pytest.raises(ValueError, match="duplicada"):
        gold_rows([record(129317, 90), record(129317, 90)])


def test_sql_top3_window_executes_with_ties_locally():
    # SQLite runs the same window query; only the DATE literal needs syntax adaptation.
    # This checks ranking logic, not BigQuery's engine or NUMERIC precision.
    sql = (Path(__file__).parents[1] / "sql/top3_atividades.sql").read_text(encoding="utf-8")
    sql = sql.format(gold_table="gold").replace("DATE '2025-01-01'", "'2025-01-01'").replace("DATE '2026-01-01'", "'2026-01-01'")
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE gold (uf_codigo TEXT, mes_referencia TEXT, atividade_codigo TEXT, indice_atividade NUMERIC)")
        connection.executemany("INSERT INTO gold VALUES ('23','2025-01-01',?,?)", [("129317", 120), ("56689", 120), ("129318", 100), ("129319", 90), ("129320", None)])
        result = connection.execute(sql).fetchall()
    assert [r[2] for r in result] == ["56689", "129317", "129318"]
    assert [r[-1] for r in result] == [1, 2, 3]


def test_sql_activity_list_matches_contract():
    sql = (Path(__file__).parents[1] / "sql/create_gold.sql").read_text(encoding="utf-8")
    import re
    assert set(re.findall(r"'(129\d+|56689)'", sql)) == ACTIVITIES


def test_gold_sql_calculates_mean_difference_and_ignores_aggregates():
    sql = (Path(__file__).parents[1] / "sql/create_gold.sql").read_text(encoding="utf-8")
    sql = sql.format(silver_table="silver").replace("DATE '2025-01-01'", "'2025-01-01'").replace("DATE '2026-01-01'", "'2026-01-01'")
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE silver (localidade_id TEXT, localidade_nome TEXT, mes_referencia TEXT, categoria_id TEXT, categoria_nome TEXT, valor NUMERIC, status_valor TEXT, run_id TEXT, nivel_territorial TEXT, variavel_id TEXT)")
        connection.executemany("INSERT INTO silver VALUES ('23','Ceara','2025-01-01',?,'Atividade',?,'fixture','test','3','12606')", [("129317", 80), ("129318", 100), ("129319", 120), ("129320", None), ("129316", 9999)])
        result = {r[3]: r for r in connection.execute(sql).fetchall()}
    assert len(result) == 4
    assert all(r[-3] == 100 for r in result.values())
    assert all(r[-2] == 3 for r in result.values())
    assert [result[key][-1] for key in ("129317", "129318", "129319", "129320")] == [-20, 0, 20, None]
