with
    source_data as (
        select
            countryregioncode as codigo_regiao_pais
            , name as nome_regiao_pais
        from {{ source('raw_aw_person', 'countryregion') }}
    )
select * from source_data