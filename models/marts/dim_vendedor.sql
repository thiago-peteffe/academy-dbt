with
    stg_sa_sales_person as (
        select
            id_vendedor
            , id_territorio
            , comissao
        from {{ ref('stg_sa_sales_person') }}
    )
    , stg_sa_sales_person_sk as (
        select
            {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , *
        from stg_sa_sales_person
    )
    , stg_pe_businessentityaddress as (
        select
            id_entidade
            , id_endereco
        from {{ ref('stg_pe_businessentityaddress') }}
    )
    , stg_pe_businessentityaddress_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from stg_pe_businessentityaddress
    )
    , joined as (
        select
            stg_sa_sales_person_sk.sk_vendedor
            , stg_sa_sales_person_sk.id_vendedor
            , stg_sa_sales_person_sk.id_territorio
            , stg_sa_sales_person_sk.comissao
            , stg_pe_businessentityaddress_sk.sk_endereco
            , stg_pe_businessentityaddress_sk.id_endereco
        from stg_sa_sales_person_sk
        left join stg_pe_businessentityaddress_sk on stg_sa_sales_person_sk.id_vendedor = stg_pe_businessentityaddress_sk.id_entidade
    )
select * from joined
