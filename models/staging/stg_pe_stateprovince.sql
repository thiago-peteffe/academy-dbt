with
    source_data as (
        select
            stateprovinceid as id_estado_provincia
            , stateprovincecode as codigo_estado_provincia
            , countryregioncode as codigo_regiao_pais
            , name as nome_estado_provincia
            , territoryid as id_territorio
        from {{ source('raw_aw_person', 'stateprovince') }}
    )
select * from source_data