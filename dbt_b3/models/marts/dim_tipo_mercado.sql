{{ config(
    materialized = "table",
    tags = ["dim"]
) }}

WITH source AS (

    SELECT
        DISTINCT tipo_mercado AS cod_tipo_mercado,
        CASE
            WHEN tipo_mercado = '10' THEN 'VISTA'
            WHEN tipo_mercado = '12' THEN 'EXERCICIO DE OPCOESES DE COMPRA'
            WHEN tipo_mercado = '13' THEN 'EXERCICIO DE OPCOES DE VENDA'
            WHEN tipo_mercado = '17' THEN 'LEILAO'
            WHEN tipo_mercado = '20' THEN 'FRACIONARIO'
            WHEN tipo_mercado = '30' THEN 'TERMO'
            WHEN tipo_mercado = '50' THEN 'FUTURO COM RETENCAO DE GANHO'
            WHEN tipo_mercado = '60' THEN 'FUTURO COM MOVIMENTACAO CONTINUA'
            WHEN tipo_mercado = '70' THEN 'OPCOES DE COMPRA'
            WHEN tipo_mercado = '80' THEN 'OPCOES DE VENDA'
            ELSE NULL
        END AS desc_tipo_mercado
    FROM
        {{ ref("stg_b3_raw_zone") }}
)
SELECT
    *
FROM
    source
