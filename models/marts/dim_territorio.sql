with
    stg_sa_salesterritory as (
        select
            id_territorio
            , nome_territorio
            , codigo_regiao_pais
            , grupo_territorio
        from {{ ref('stg_sa_salesterritory') }}
    )
    , stg_sa_salesterritory_with_sk as (
        select
            {{ numeric_surrogate_key(['id_territorio']) }} as sk_territorio
            , *
        from stg_sa_salesterritory
    )
    , stg_pe_countryregion as (
        select
            codigo_regiao_pais
            , nome_regiao_pais
        from {{ ref('stg_pe_countryregion') }}
    )
    , stg_pe_stateprovince as (
        select
            id_estado_provincia
            , codigo_estado_provincia
            , codigo_regiao_pais
            , nome_estado_provincia
            , id_territorio
        from {{ ref('stg_pe_stateprovince') }}
    )
    , joined_region as (
        select
            stg_pe_countryregion.codigo_regiao_pais
            , stg_pe_countryregion.nome_regiao_pais
            , stg_pe_stateprovince.id_estado_provincia
            , stg_pe_stateprovince.codigo_estado_provincia
            , stg_pe_stateprovince.nome_estado_provincia
            , stg_pe_stateprovince.id_territorio
        from stg_pe_countryregion
        left join stg_pe_stateprovince on stg_pe_countryregion.codigo_regiao_pais = stg_pe_stateprovince.codigo_regiao_pais
    )
    , joined as (
        select
            stg_sa_salesterritory_with_sk.sk_territorio
            , stg_sa_salesterritory_with_sk.id_territorio
            , stg_sa_salesterritory_with_sk.nome_territorio
            , stg_sa_salesterritory_with_sk.codigo_regiao_pais
            , stg_sa_salesterritory_with_sk.grupo_territorio
            , joined_region.nome_regiao_pais
            , joined_region.id_estado_provincia
            , joined_region.codigo_estado_provincia
            , joined_region.nome_estado_provincia
        from stg_sa_salesterritory_with_sk
        left join joined_region on stg_sa_salesterritory_with_sk.id_territorio = joined_region.id_territorio
    )
select * from joined