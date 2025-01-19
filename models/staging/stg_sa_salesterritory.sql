with
    source_data as (
        select
            territoryid as id_territorio
            , name as nome_territorio
            , countryregioncode as codigo_regiao_pais
            , `group` as grupo_territorio
        from {{ source('raw_aw_sales', 'salesterritory') }}
    )
select * from source_data