with
    stg_pe_address as (
        select
            id_endereco
            , cidade
            , id_estado_provincia
        from {{ ref('stg_pe_address') }}
    )
    , stg_pe_address_with_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from stg_pe_address
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
    , stg_sa_salesterritory as (
        select
            id_territorio
            , nome_territorio
            , codigo_regiao_pais
            , grupo_territorio
        from {{ ref('stg_sa_salesterritory') }}
    )
    , stg_pe_countryregion as (
        select
            codigo_regiao_pais
            , nome_regiao_pais
        from {{ ref('stg_pe_countryregion') }}
    )
    , joined_state_territory_region as (
        select
            stg_pe_stateprovince.id_estado_provincia
            , stg_pe_stateprovince.codigo_estado_provincia
            , stg_pe_stateprovince.nome_estado_provincia
            , stg_pe_stateprovince.id_territorio
            , stg_sa_salesterritory.nome_territorio
            , stg_sa_salesterritory.grupo_territorio
            , stg_pe_countryregion.codigo_regiao_pais
            , stg_pe_countryregion.nome_regiao_pais
        from stg_pe_stateprovince
        left join stg_sa_salesterritory on stg_pe_stateprovince.id_territorio = stg_sa_salesterritory.id_territorio
        left join stg_pe_countryregion on stg_pe_stateprovince.codigo_regiao_pais = stg_pe_countryregion.codigo_regiao_pais
    )
    , joined as (
        select
            stg_pe_address_with_sk.sk_endereco
            , stg_pe_address_with_sk.id_endereco
            , stg_pe_address_with_sk.cidade
            , stg_pe_address_with_sk.id_estado_provincia
            , joined_state_territory_region.codigo_estado_provincia
            , joined_state_territory_region.nome_estado_provincia
            , joined_state_territory_region.id_territorio
            , joined_state_territory_region.nome_territorio
            , joined_state_territory_region.grupo_territorio
            , joined_state_territory_region.codigo_regiao_pais
            , joined_state_territory_region.nome_regiao_pais
        from stg_pe_address_with_sk
        left join joined_state_territory_region on stg_pe_address_with_sk.id_estado_provincia = joined_state_territory_region.id_estado_provincia
    )
select * from joined