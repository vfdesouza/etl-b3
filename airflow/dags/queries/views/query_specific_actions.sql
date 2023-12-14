CREATE OR REPLACE VIEW
  `datapipeline-405000.views.specific_actions` AS(
  SELECT
    cod_negociacao,
    nome_empresa,
    MIN(SAFE_CAST(preco_minimo AS float64)) AS min_price,
    MAX(SAFE_CAST(preco_maximo AS float64)) AS max_price
  FROM
    `datapipeline-405000.b3_raw_zone.b3_retroactive`
  WHERE
    cod_negociacao IN ( 'NUBR33',
      'ROXO34' )
  GROUP BY
    1,
    2 )
