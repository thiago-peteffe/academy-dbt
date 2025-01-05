with
    source_data as (
        select
            businessentityid as id_entidade
            , persontype as tipo_pessoa
            , firstname || ' ' || middlename || ' ' || lastname || ' ' || suffix as nome_completo
        from {{ source('raw_sap_adw', 'person') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_entidade']) }} as sk_entidade
            , *
        from source_data
    )
select *
from source_with_sk