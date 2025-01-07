with
    source_data as (
        select
            territoryid as id_territorio
            , name as nome_territorio
            , countryregioncode as codigo_regiao
        from {{ source('raw_sap_adw', 'salesterritory') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_territorio']) }} as sk_territorio
            , *
        from source_data
    )
select *
from source_with_sk