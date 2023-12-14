CREATE OR REPLACE VIEW
  `datapipeline-405000.views.top5_var_pos` AS (
  WITH
    variation AS (
    SELECT
      data_pregao,
      cod_negociacao,
      nome_empresa,
      cod_especificacao_papel,
      ROUND(((preco_ultimo_negocio - preco_abertura) / preco_abertura) * 100, 2) AS var_percentage,
      ROUND(preco_ultimo_negocio - preco_abertura, 2) AS var_value
    FROM
      `datapipeline-405000.b3_trusted_zone.fato_negociacao`
    WHERE
      cod_bdi IN ('2',
        '8',
        '50') )
  SELECT
    *
  FROM
    variation
  WHERE
    data_pregao = "2023-11-17"
    AND var_percentage > 0
  ORDER BY
    5 DESC
  LIMIT
    5 )
