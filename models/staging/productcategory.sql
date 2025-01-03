with
    source_data as (
        select
            productcategoryid as id_categoria_produto
            , name as nome_categoria
        from {{ source('raw_sap_adw', 'productcategory') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_categoria_produto']) }} as sk_categoria_produto
            , *
        from source_data
    )
select *
from source_with_sk