with
    stg_pu_vendor as (
        select
            id_entidade
            , id_fornecedor
            , nome_fornecedor
            , case
                when status_fornecedor = 0 then 'Inativo'
                when status_fornecedor = 1 then 'Ativo'
                else 'Não especificado'
            end as status_fornecedor
        from {{ ref('stg_pu_vendor') }}
    )
    , stg_pu_vendor_with_sk as (
        select
            {{ numeric_surrogate_key(['id_fornecedor']) }} as sk_fornecedor
            , *
        from stg_pu_vendor
    )
    , stg_pe_businessentityaddress as (
        select
            id_entidade
            , id_endereco
        from {{ ref('stg_pe_businessentityaddress') }}
    )
    , stg_pe_businessentityaddress_with_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from stg_pe_businessentityaddress
    )
    , joined as (
        select
            stg_pu_vendor_with_sk.sk_fornecedor
            , stg_pu_vendor_with_sk.id_fornecedor
            , stg_pu_vendor_with_sk.nome_fornecedor
            , stg_pu_vendor_with_sk.status_fornecedor
            , stg_pe_businessentityaddress_with_sk.sk_endereco
        from stg_pu_vendor_with_sk
        left join stg_pe_businessentityaddress_with_sk on stg_pu_vendor_with_sk.id_entidade = stg_pe_businessentityaddress_with_sk.id_entidade
    )
select * from joined