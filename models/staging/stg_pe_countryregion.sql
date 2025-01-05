with
    source_data as (
        select
            countryregioncode as codigo_regiao_pais
            , name as nome_regiao_pais
        from {{ source('raw_sap_adw', 'countryregion') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['codigo_regiao_pais']) }} as sk_codigo_regiao_pais
            , *
        from source_data
    )
select *
from source_with_sk