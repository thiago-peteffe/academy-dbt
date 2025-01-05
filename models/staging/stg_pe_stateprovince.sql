with
    source_data as (
        select
            stateprovinceid as id_estado_provincia
            , stateprovincecode as codigo_estado_provincia
            , countryregioncode as codigo_regiao_pais
            , name as nome_regiao_pais
            , territoryid as id_territorio
        from {{ source('raw_sap_adw', 'stateprovince') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_estado_provincia']) }} as sk_estado_provincia
            , *
        from source_data
    )
select *
from source_with_sk