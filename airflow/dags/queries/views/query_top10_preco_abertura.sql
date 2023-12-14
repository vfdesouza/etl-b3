CREATE OR REPLACE VIEW
  `datapipeline-405000.views.top10_preco_abertura` AS (
  WITH
    query_top_10 AS (
    SELECT
      nome_empresa,
      desc_cod_bdi,
      desc_tipo_mercado,
      desc_especificacao_papel,
      MAX(preco_abertura) AS preco_abertura
    FROM
      `datapipeline-405000.b3_trusted_zone.fato_negociacao`
    WHERE
      data_pregao >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
      AND cod_bdi IN ('2',
        '8',
        '50')
    GROUP BY
      1,
      2,
      3,
      4
    ORDER BY
      preco_abertura DESC
    LIMIT
      10)
  SELECT
    nome_empresa,
    FORMAT('R$ %.2f', preco_abertura) AS preco_abertura,
    * EXCEPT (nome_empresa,
      preco_abertura)
  FROM
    query_top_10 )
