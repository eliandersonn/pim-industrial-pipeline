"""Referência Decimal e resultados locais para validar a consulta BigQuery."""

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, localcontext

ACTIVITIES = {"129317", "129318", "129319", "129320", "129321", "129322", "129323", "129324",
              "129325", "129326", "56689", "129330", "129331", "129332", "129333", "129334",
              "129335", "129336", "129337", "129338", "129339", "129340", "129341", "129342"}
SCALE = Decimal("0.000000001")


def gold_rows(rows):
    """Reproduz os cálculos SQL com Decimal para conferir os resultados localmente."""
    eligible = [r for r in rows if r["nivel_territorial"] == "3" and r["variavel_id"] == "12606"
                and r["periodo"].startswith("2025") and r["categoria_id"] in ACTIVITIES]
    groups = defaultdict(list)
    keys = set()
    for row in eligible:
        key = row["localidade_id"], row["periodo"], row["categoria_id"]
        if key in keys:
            raise ValueError("Chave duplicada na entrada Gold")
        keys.add(key)
        if row["valor"] is not None:
            groups[key[:2]].append(row["valor"])
    output = []
    with localcontext() as context:
        context.prec = 50
        for row in eligible:
            values = groups[row["localidade_id"], row["periodo"]]
            average = (sum(values) / len(values)).quantize(SCALE, rounding=ROUND_HALF_UP) if values else None
            difference = (row["valor"] - average).quantize(SCALE, rounding=ROUND_HALF_UP) if row["valor"] is not None else None
            output.append({"uf_codigo": row["localidade_id"], "uf_nome": row["localidade_nome"],
                           "mes_referencia": row["mes_referencia"], "atividade_codigo": row["categoria_id"],
                           "atividade_nome": row["categoria_nome"], "indice_atividade": row["valor"],
                           "media_indices_uf_mes": average, "diferenca": difference,
                           "quantidade_atividades_validas": len(values), "status_valor": row["status_valor"],
                           "run_id": row["run_id"]})
    return sorted(output, key=lambda r: (r["uf_codigo"], r["mes_referencia"], int(r["atividade_codigo"])))


def top3_rows(rows):
    """Escolhe até três índices válidos por UF/mês; o código resolve os empates."""
    groups = defaultdict(list)
    for row in rows:
        if row["indice_atividade"] is not None:
            groups[row["uf_codigo"], row["mes_referencia"]].append(row)
    result = []
    for key in sorted(groups):
        ordered = sorted(groups[key], key=lambda r: (-r["indice_atividade"], int(r["atividade_codigo"])))
        result.extend({**r, "posicao": position} for position, r in enumerate(ordered[:3], 1))
    return result
