CREATE OR REPLACE VIEW
  `datapipeline-405000.views.top5_mais_negociados` AS (
  SELECT
    cod_negociacao,
    SUM(quantidade_papeis_negociados) AS quantidade_papeis_negociados
  FROM
    `datapipeline-405000.b3_trusted_zone.fato_negociacao`
  WHERE
    data_pregao = '2023-11-17'
    AND cod_bdi IN ('2',
      '8',
      '50')
  GROUP BY
    1
  ORDER BY
    2 DESC
  LIMIT
    5 )
