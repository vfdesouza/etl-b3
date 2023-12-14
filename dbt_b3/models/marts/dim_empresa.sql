{{ config(
    materialized = "table",    
    tags = ["dim"]
) }}

WITH source AS (

    SELECT
        DISTINCT cod_negociacao,
        REPLACE(
            cod_bdi,
            ".0",
            ""
        ) AS cod_bdi,
        REGEXP_REPLACE(nome_empresa, r" .*", "") AS nome_empresa,
        tipo_mercado,
        especificacao_papel,
        data_pregao
    FROM
        {{ ref("stg_b3_raw_zone") }}
    WHERE
        EXTRACT(
            YEAR
            FROM
                data_pregao
        ) = EXTRACT(
            YEAR
            FROM
                CURRENT_DATE()
        )
)
SELECT
    DISTINCT * EXCEPT(data_pregao)
FROM
    source
