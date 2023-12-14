{{ config(
    materialized = "view",
    tags = ["stg"]
) }}

WITH source AS (

    SELECT
        DISTINCT *
    EXCEPT(especificacao_papel),
        REPLACE(
            REPLACE(
                especificacao_papel,
                " ",
                ""
            ),
            "*",
            ""
        ) AS especificacao_papel
    FROM
        {{ source(
            "b3_raw_zone",
            "b3_retroactive"
        ) }}
)
SELECT
    *
EXCEPT
    (
        codigo_isin,
        data_vencimento,
        prazo_dias_merc_termo
    )
FROM
    source
