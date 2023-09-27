CREATE OR REPLACE TABLE
  `project.b3_refined_zone.b3_2019_2023`
PARTITION BY
  data_pregao
CLUSTER BY
  cod_negociacao,
  codigo_isin OPTIONS ( require_partition_filter = FALSE) AS (
  SELECT
    data_pregao,
    cod_negociacao,
    noma_empresa,
    especificacao_papel,
  IF
    (prazo_dias_merc_termo IN ('nan'), NULL, prazo_dias_merc_termo) AS prazo_dias_merc_termo,
    SAFE_CAST(preco_abertura AS float64) AS preco_abertura,
    SAFE_CAST(preco_maximo AS float64) AS preco_maximo,
    SAFE_CAST(preco_minimo AS float64) AS preco_minimo,
    SAFE_CAST(preco_medio AS float64) AS preco_medio,
    SAFE_CAST(preco_ultimo_negocio AS float64) AS preco_ultimo_negocio,
    SAFE_CAST(preco_melhor_oferta_compra AS float64) AS preco_melhor_oferta_compra,
    SAFE_CAST(preco_melhor_oferta_venda AS float64) AS preco_melhor_oferta_venda,
    SAFE_CAST(REPLACE(numero_negocios, '.0', '') AS int64) AS numero_negocios,
    SAFE_CAST(REPLACE(quantidade_papeis_negociados, '.0', '') AS int64) AS quantidade_papeis_negociados,
    SAFE_CAST(REPLACE(volume_total_negociado, '.0', '') AS int64) AS volume_total_negociado,
    SAFE_CAST(preco_exercicio AS float64) AS preco_exercicio,
    SAFE_CAST(indicador_correcao_precos AS float64) AS indicador_correcao_precos,
    data_vencimento,
    SAFE_CAST(fator_cotacao AS float64) AS fator_cotacao,
    preco_exercicio_pontos,
    codigo_isin,
    SAFE_CAST(REPLACE(num_distribuicao_papel, '.0', '') AS int64) AS num_distribuicao_papel,
  FROM
    `project.b3_raw_zone.b3_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '2019'
    AND '2023' )