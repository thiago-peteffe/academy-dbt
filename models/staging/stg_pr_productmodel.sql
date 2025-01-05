with
    source_data as (
        select
            productmodelid as id_modelo_produto
            , name as nome_modelo
        from {{ source('raw_sap_adw', 'productmodel') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_modelo_produto']) }} as sk_modelo_produto
            , *
        from source_data
    )
select *
from source_with_sk