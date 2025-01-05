with
    source_data as (
        select
            businessentityid as id_entidade
            , name as nome_fornecedor
        from {{ source('raw_sap_adw', 'vendor') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_entidade']) }} as sk_entidade
            , *
        from source_data
    )
select *
from source_with_sk