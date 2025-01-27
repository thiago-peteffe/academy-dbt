with
    fact_pedidos as (
        select
            sk_pedido
            , sk_cliente
            , sk_endereco
            , data_pedido
            , status_pedido
            , flag_pedido
            , id_vendedor
            , venda_subtotal
            , valor_imposto
            , custo_envio
            , venda_total
        from {{ ref('fact_pedidos') }}
    )
    , dim_cliente as (
        select
            sk_cliente
            , id_cliente
            , id_pessoa
            , id_loja
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , tipo_contato
            , id_vendedor
        from {{ ref('dim_cliente') }}
    )
    , dim_territorio as (
        select
            sk_endereco
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
        from {{ ref('dim_territorio') }}
    )
    , stg_sa_sales_person as (
        select
            id_vendedor
            , id_territorio
            , comissao
        from {{ ref('stg_sa_sales_person') }}
    )
    , stg_pe_person as (
        select
            id_entidade
            , tipo_pessoa
            , nome_completo
        from {{ ref('stg_pe_person') }}
    )
    , vendedor as (
        select
            stg_sa_sales_person.id_vendedor
            , stg_pe_person.nome_completo
        from stg_sa_sales_person
        left join stg_pe_person on stg_sa_sales_person.id_vendedor = stg_pe_person.id_entidade
    )
    , fact_pedidos_territory as (
        select
            fact_pedidos.sk_pedido
            , fact_pedidos.sk_cliente
            , fact_pedidos.sk_endereco
            , fact_pedidos.data_pedido
            , fact_pedidos.status_pedido
            , fact_pedidos.flag_pedido
            , fact_pedidos.venda_subtotal
            , fact_pedidos.valor_imposto
            , fact_pedidos.custo_envio
            , fact_pedidos.venda_total
            , dim_territorio.nome_estado_provincia
            , dim_territorio.nome_territorio
            , dim_territorio.grupo_territorio
            , dim_territorio.nome_regiao_pais
            , vendedor.id_vendedor
            , vendedor.nome_completo
        from fact_pedidos
        left join dim_territorio on fact_pedidos.sk_endereco = dim_territorio.sk_endereco
        left join vendedor on fact_pedidos.id_vendedor = vendedor.id_vendedor
    )
    , joined as (
        select
            fact_pedidos_territory.data_pedido
            , case
                when fact_pedidos_territory.nome_completo is null then 'Pedido on-line'
                else fact_pedidos_territory.nome_completo
            end as vendedor
            , fact_pedidos_territory.nome_regiao_pais
            , fact_pedidos_territory.venda_subtotal
            , fact_pedidos_territory.valor_imposto
            , fact_pedidos_territory.custo_envio
            , fact_pedidos_territory.venda_total
        from fact_pedidos_territory
        left join dim_cliente on fact_pedidos_territory.sk_cliente = dim_cliente.sk_cliente
    )
    , agg_joined as (
        select
            nome_regiao_pais
            , vendedor
            , extract(year from data_pedido) as ano
            , extract(month from data_pedido) as mes
            , round(sum(venda_subtotal), 2) as venda_subtotal
            , round(sum(valor_imposto), 2) as valor_imposto
            , round(sum(custo_envio), 2) as custo_envio
            , round(sum(venda_total), 2) as venda_total
        from joined
        group by
            nome_regiao_pais
            , vendedor
            , ano
            , mes
        order by
            nome_regiao_pais
            , vendedor
            , ano
            , mes
    )
select * from agg_joined