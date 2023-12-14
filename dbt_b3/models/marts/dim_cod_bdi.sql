{{ config(
    materialized = "table",
    tags = ["dim"]
) }}

WITH replaced AS (

    SELECT
        DISTINCT REPLACE(
            cod_bdi,
            ".0",
            ""
        ) AS cod_bdi
    FROM
        {{ ref("stg_b3_raw_zone") }}
)
SELECT
    replaced.cod_bdi,
    CASE
        WHEN replaced.cod_bdi = '2' THEN 'LOTE PADRAO'
        WHEN replaced.cod_bdi = '5' THEN 'SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA'
        WHEN replaced.cod_bdi = '6' THEN 'CONCORDATARIAS'
        WHEN replaced.cod_bdi = '7' THEN 'RECUPERACAO EXTRAJUDICIAL'
        WHEN replaced.cod_bdi = '8' THEN 'RECUPERAÇÃO JUDICIAL'
        WHEN replaced.cod_bdi = '9' THEN 'RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA'
        WHEN replaced.cod_bdi = '10' THEN 'DIREITOS E RECIBOS'
        WHEN replaced.cod_bdi = '11' THEN 'INTERVENCAO'
        WHEN replaced.cod_bdi = '12' THEN 'FUNDOS IMOBILIARIOS'
        WHEN replaced.cod_bdi = '14' THEN 'CERT.INVEST/TIT.DIV.PUBLICA'
        WHEN replaced.cod_bdi = '18' THEN 'OBRIGACOES'
        WHEN replaced.cod_bdi = '22' THEN 'BONUS (PRIVADOS)'
        WHEN replaced.cod_bdi = '26' THEN 'APOLICES/BONUS/TITULOS PUBLICOS'
        WHEN replaced.cod_bdi = '32' THEN 'EXERCICIO DE OPCOES DE COMPRA DE INDICES'
        WHEN replaced.cod_bdi = '33' THEN 'EXERCICIO DE OPCOES DE VENDA DE INDICES'
        WHEN replaced.cod_bdi = '38' THEN 'EXERCICIO DE OPCOES DE COMPRA'
        WHEN replaced.cod_bdi = '42' THEN 'EXERCICIO DE OPCOES DE VENDA'
        WHEN replaced.cod_bdi = '46' THEN 'LEILAO DE NAO COTADOS'
        WHEN replaced.cod_bdi = '48' THEN 'LEILAO DE PRIVATIZACAO'
        WHEN replaced.cod_bdi = '49' THEN 'LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANT'
        WHEN replaced.cod_bdi = '50' THEN 'LEILAO'
        WHEN replaced.cod_bdi = '51' THEN 'LEILAO FINOR'
        WHEN replaced.cod_bdi = '52' THEN 'LEILAO FINAM'
        WHEN replaced.cod_bdi = '53' THEN 'LEILAO FISET'
        WHEN replaced.cod_bdi = '54' THEN 'LEILAO DE ACOES EM MORA'
        WHEN replaced.cod_bdi = '56' THEN 'VENDAS POR ALVARA JUDICIAL'
        WHEN replaced.cod_bdi = '58' THEN 'OUTROS'
        WHEN replaced.cod_bdi = '60' THEN 'PERMUTA POR ACOES'
        WHEN replaced.cod_bdi = '61' THEN 'META'
        WHEN replaced.cod_bdi = '62' THEN 'MERCADO A TERMO'
        WHEN replaced.cod_bdi = '66' THEN 'DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS'
        WHEN replaced.cod_bdi = '68' THEN 'DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS'
        WHEN replaced.cod_bdi = '70' THEN 'FUTURO COM RETENCAO DE GANHOS'
        WHEN replaced.cod_bdi = '71' THEN 'MERCADO DE FUTURO'
        WHEN replaced.cod_bdi = '74' THEN 'OPCOES DE COMPRA DE INDICES'
        WHEN replaced.cod_bdi = '75' THEN 'OPCOES DE VENDA DE INDICES'
        WHEN replaced.cod_bdi = '78' THEN 'OPCOES DE COMPRA'
        WHEN replaced.cod_bdi = '82' THEN 'OPCOES DE VENDA'
        WHEN replaced.cod_bdi = '83' THEN 'BOVESPAFIX'
        WHEN replaced.cod_bdi = '84' THEN 'SOMA FIX'
        ELSE NULL
    END AS desc_cod_bdi
FROM
    replaced
