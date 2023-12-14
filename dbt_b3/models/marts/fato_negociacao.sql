{{ config(
    tags = ["fact"],
    materialized = "incremental",
    cluster_by = ["cod_negociacao"],
    unique_key = ["cod_negociacao", "cod_especificacao_papel", "data_pregao"],
    partition_by ={ "field": "data_pregao",
    "data_type": "date",
    "granularity": "day",},
) }}

WITH cod_bdi AS (

    SELECT
        *
    FROM
        {{ ref("dim_cod_bdi") }}
),
empresa AS (
    SELECT
        *
    FROM
        {{ ref("dim_empresa") }}
),
especificacao_papel AS (
    SELECT
        *
    FROM
        {{ ref("dim_especificacao_papel") }}
),
tipo_mercado AS (
    SELECT
        *
    FROM
        {{ ref("dim_tipo_mercado") }}
),
stg_b3 AS (
    SELECT
        data_pregao,
        cod_negociacao,
        especificacao_papel,
        "BRL" AS moeda_referencia,
        preco_abertura,
        preco_minimo,
        preco_medio,
        preco_maximo,
        numero_negocios,
        preco_ultimo_negocio,
        quantidade_papeis_negociados,
        volume_total_negociado,
        REPLACE(
            fator_cotacao,
            ".0",
            ""
        ) AS fator_cotacao,
        REPLACE(
            num_distribuicao_papel,
            ".0",
            ""
        ) AS num_distribuicao_papel
    FROM
        {{ ref("stg_b3_raw_zone") }}
),
joined AS (
    SELECT
        cod_bdi.desc_cod_bdi,
        empresa.*
    EXCEPT(cod_negociacao),
        especificacao_papel.*,
        tipo_mercado.desc_tipo_mercado,
        valores_negociados.*
    EXCEPT(especificacao_papel)
    FROM
        stg_b3
        LEFT JOIN empresa
        ON valores_negociados.cod_negociacao = empresa.cod_negociacao
        AND valores_negociados.especificacao_papel = empresa.especificacao_papel
        LEFT JOIN cod_bdi
        ON cod_bdi.cod_bdi = empresa.cod_bdi
        LEFT JOIN tipo_mercado
        ON empresa.tipo_mercado = tipo_mercado.cod_tipo_mercado
        LEFT JOIN especificacao_papel
        ON empresa.especificacao_papel = especificacao_papel.cod_especificacao_papel
)
SELECT
    cod_negociacao,
    nome_empresa,
    cod_bdi,
    desc_cod_bdi,
    tipo_mercado AS cod_tipo_mercado,
    desc_tipo_mercado,
    especificacao_papel AS cod_especificacao_papel,
    desc_especificacao_papel,
    data_pregao,
    safe_cast(
        quantidade_papeis_negociados AS float64
    ) AS quantidade_papeis_negociados,
    moeda_referencia,
    safe_cast(
        preco_abertura AS float64
    ) AS preco_abertura,
    safe_cast(
        preco_minimo AS float64
    ) AS preco_minimo,
    safe_cast(
        preco_medio AS float64
    ) AS preco_medio,
    safe_cast(
        preco_maximo AS float64
    ) AS preco_maximo,
    safe_cast(
        preco_ultimo_negocio AS float64
    ) AS preco_ultimo_negocio,
    REPLACE(
        fator_cotacao,
        ".0",
        ""
    ) AS fator_cotacao
FROM
    joined

{% if is_incremental() %}
WHERE
    data_pregao > (
        SELECT
            MAX(data_pregao)
        FROM
            {{ source(
                "b3_raw_zone",
                "b3_retroactive"
            ) }}
    )
{% endif %}
