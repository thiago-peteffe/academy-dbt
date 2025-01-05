with
    source_data as (
        select
            addressid as id_endereco
            , city as cidade
            , stateprovinceid as id_estado_provincia
        from {{ source('raw_sap_adw', 'address') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from source_data
    )
select *
from source_with_sk