-- Validação independente dos campos calculados e da chave da candidata.
WITH expected AS (
    SELECT *, AVG(indice_atividade) OVER (PARTITION BY uf_codigo, mes_referencia) AS expected_mean,
           COUNT(indice_atividade) OVER (PARTITION BY uf_codigo, mes_referencia) AS expected_count,
           COUNT(*) OVER (PARTITION BY uf_codigo, mes_referencia, atividade_codigo) AS key_count
    FROM `{gold_table}`
)
SELECT COUNT(*) AS rows_count,
       COUNTIF(key_count != 1
           OR quantidade_atividades_validas != expected_count
           OR (media_indices_uf_mes IS NULL) != (expected_mean IS NULL)
           OR ABS(media_indices_uf_mes - expected_mean) > NUMERIC '0.000000001'
           OR (diferenca IS NULL) != (indice_atividade IS NULL)
           OR ABS(diferenca - (indice_atividade - expected_mean)) > NUMERIC '0.000000001'
       ) AS invalid_rows
FROM expected
