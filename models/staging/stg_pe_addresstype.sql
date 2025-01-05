with
    source_data as (
        select
            addresstypeid as id_tipo_endereco
            , name as nome_tipo_endereco
        from {{ source('raw_sap_adw', 'addresstype') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_tipo_endereco']) }} as sk_tipo_endereco
            , *
        from source_data
    )
select *
from source_with_sk