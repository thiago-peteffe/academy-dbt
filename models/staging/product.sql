with
    source_data as (
        select
            productid as id_produto
            , name as nome_produto
            , productnumber as numero_produto
            , makeflag as origem_produto
            , finishedgoodsflag as vendavel
            , color as cor_produto
            , standardcost as custo_padrao
            , listprice as preco_venda
            , productline as linha
            , class as classe
            , style as estilo
            , productsubcategoryid as id_subcategoria_produto
            , productmodelid as id_modelo_produto
        from {{ source('raw_sap_adw', 'product') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from source_data
    )
select *
from source_with_sk