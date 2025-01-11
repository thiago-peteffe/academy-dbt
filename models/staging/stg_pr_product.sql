with
    source_data as (
        select
            productid as id_produto
            , name as nome_produto
            , productnumber as numero_produto
            , cast(makeflag as int) as origem_produto
            , cast(finishedgoodsflag as int) as vendavel
            , color as cor_produto
            , standardcost as custo_padrao
            , listprice as preco_venda
            , trim(cast(productline as string)) as linha
            , trim(cast(class as string)) as classe
            , trim(cast(style as string)) as estilo
            , productsubcategoryid as id_subcategoria_produto
            , productmodelid as id_modelo_produto
        from {{ source('raw_sap_adw', 'product') }}
    )
select * from source_data