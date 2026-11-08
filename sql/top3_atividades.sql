-- BigQuery Standard SQL; no máximo três atividades válidas por UF/mês.
WITH ranking AS (
    -- Cada UF/mês recebe seu próprio ranking; o código da atividade desempata.
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY uf_codigo, mes_referencia
        ORDER BY indice_atividade DESC, CAST(atividade_codigo AS INT64) ASC
    ) AS posicao
    FROM `{gold_table}`
    WHERE mes_referencia >= DATE '2025-01-01' AND mes_referencia < DATE '2026-01-01'
      AND indice_atividade IS NOT NULL
)
SELECT * FROM ranking WHERE posicao <= 3
ORDER BY uf_codigo, mes_referencia, posicao
