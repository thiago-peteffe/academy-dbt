with
    source_data as (
        select
            businessentityid as id_entidade
            , addressid as id_endereco
            , addresstypeid as id_tipo_endereco
        from {{ source('raw_sap_adw', 'businessentityaddress') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_entidade']) }} as sk_entidade
            , {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , {{ numeric_surrogate_key(['id_tipo_endereco']) }} as sk_tipo_endereco
            , *
        from source_data
    )
select *
from source_with_sk