-- BigQuery Standard SQL. {silver_table} é renderizado somente após validar os identificadores.
WITH atividades AS (
    -- O histórico é completo; a análise recorta somente UF, 2025 e atividades sem agregados.
    SELECT localidade_id AS uf_codigo, localidade_nome AS uf_nome,
           mes_referencia, categoria_id AS atividade_codigo, categoria_nome AS atividade_nome,
           valor AS indice_atividade, status_valor, run_id
    FROM `{silver_table}`
    WHERE nivel_territorial = '3'
      AND variavel_id = '12606'
      AND mes_referencia >= DATE '2025-01-01' AND mes_referencia < DATE '2026-01-01'
      AND categoria_id IN (
          '129317','129318','129319','129320','129321','129322','129323','129324',
          '129325','129326','56689','129330','129331','129332','129333','129334',
          '129335','129336','129337','129338','129339','129340','129341','129342'
      )
), medias AS (
    -- A janela mantém uma linha por atividade; AVG ignora índices NULL.
    SELECT *, AVG(indice_atividade) OVER (PARTITION BY uf_codigo, mes_referencia) AS media_indices_uf_mes,
           COUNT(indice_atividade) OVER (PARTITION BY uf_codigo, mes_referencia) AS quantidade_atividades_validas
    FROM atividades
)
SELECT *, indice_atividade - media_indices_uf_mes AS diferenca
FROM medias
