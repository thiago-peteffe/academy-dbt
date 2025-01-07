with
    stg_sa_customer as (
        select
            sk_cliente
            , id_cliente
            , id_pessoa
            , id_loja
            , id_territorio
            , case
                when id_pessoa is null then 'PJ'
                else 'PF'
            end as tipo_cliente
        from {{ ref('stg_sa_customer') }}
    )
    , stg_pe_person as (
        select
            sk_entidade
            , id_entidade
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
            sk_entidade
            , id_entidade
            , nome_loja
            , id_vendedor
        from {{ ref('stg_sa_store') }}
    )
    , stg_sa_salesterritory as (
        select
            sk_territorio
            , id_territorio
            , nome_territorio
            , codigo_regiao
        from {{ ref('stg_sa_salesterritory') }}
    )
    , stg_pe_businessentity as (
        select id_entidade
        from {{ ref('stg_pe_businessentity') }}
    )
    , stg_pe_businessentityaddress as (
        select
            id_entidade
            , id_endereco
            , id_tipo_endereco
        from {{ ref('stg_pe_businessentityaddress') }}
    )
    , joined as (
        select
            stg_sa_customer.sk_cliente
            , stg_sa_customer.id_cliente
            , stg_sa_customer.id_pessoa
            , stg_sa_customer.id_loja
            , stg_sa_customer.tipo_cliente
            , stg_sa_customer.id_territorio
            , stg_pe_person.tipo_contato
            , stg_pe_person.nome_completo
            , stg_sa_store.nome_loja
            , stg_sa_store.id_vendedor
            , stg_sa_salesterritory.sk_territorio
            , stg_sa_salesterritory.nome_territorio
            , stg_sa_salesterritory.codigo_regiao
        from stg_sa_customer
        left join stg_pe_person on stg_sa_customer.id_pessoa = stg_pe_person.id_entidade
        left join stg_sa_store on stg_sa_customer.id_loja = stg_sa_store.id_entidade
        left join stg_sa_salesterritory on stg_sa_customer.id_territorio = stg_sa_salesterritory.id_territorio
    )
select * from joined