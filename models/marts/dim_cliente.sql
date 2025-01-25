with
    stg_sa_customer as (
        select
            id_cliente
            , id_pessoa
            , id_loja
            , id_territorio
            , case
                when id_loja is not null 
                    and id_pessoa is not null then 'PJ'
                when id_pessoa is not null then 'PF'
                when id_loja is not null then 'PJ'
            end as tipo_cliente
        from {{ ref('stg_sa_customer') }}
    )
    , stg_sa_customer_with_sk as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , *
        from stg_sa_customer
    )
    , stg_pe_person as (
        select
            id_entidade
            , tipo_pessoa
            , case
                when tipo_pessoa = 'SC' then 'Contato da loja'
                when tipo_pessoa = 'IN' then 'Cliente individual (varejo)'
                when tipo_pessoa = 'SP' then 'Vendedor'
                when tipo_pessoa = 'EM' then 'Funcionário (não vendas)'
                when tipo_pessoa = 'VC' then 'Contato do fornecedor'
                when tipo_pessoa = 'GC' then 'Contato geral'
                else 'Não especificado'
            end as tipo_contato
            , nome_completo
        from {{ ref('stg_pe_person') }}
    )
    , stg_sa_store as (
        select
            id_entidade
            , nome_loja
            , id_vendedor
        from {{ ref('stg_sa_store') }}
    )
    , joined as (
        select
            stg_sa_customer_with_sk.sk_cliente
            , stg_sa_customer_with_sk.id_cliente
            , stg_sa_customer_with_sk.id_pessoa
            , stg_sa_customer_with_sk.id_loja
            , stg_sa_store.id_vendedor
            , case
                when stg_sa_customer_with_sk.tipo_cliente = 'PF' then nome_completo
                else stg_sa_store.nome_loja
            end as nome_cliente
            , stg_sa_customer_with_sk.id_territorio
            , stg_sa_customer_with_sk.tipo_cliente
            , stg_pe_person.tipo_contato
        from stg_sa_customer_with_sk
        left join stg_pe_person on stg_sa_customer_with_sk.id_pessoa = stg_pe_person.id_entidade
        left join stg_sa_store on stg_sa_customer_with_sk.id_loja = stg_sa_store.id_entidade
    )
select * from joined