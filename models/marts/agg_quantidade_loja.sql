with
    fact_pedidos_item as (
        select
            sk_pedido_item
            , sk_pedido
            , sk_produto
            , quantidade_ordem
        from {{ ref('fact_pedidos_item') }}
    )
    , fact_pedidos as (
        select
            sk_pedido
            , sk_cliente
            , sk_endereco
            , id_territorio
            , data_pedido
        from {{ ref('fact_pedidos') }}
    )
    , dim_produto as (
        select
            sk_produto
            , nome_produto
        from {{ ref('dim_produto') }}
    )
    , dim_territorio as (
        select distinct
            sk_endereco
            , nome_estado_provincia
            , nome_regiao_pais
        from {{ ref('dim_territorio') }}
    )
    , dim_cliente as (
        select
            sk_cliente
            , id_cliente
            , id_pessoa
            , id_loja
            , id_vendedor
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , tipo_contato
        from {{ ref('dim_cliente') }}
    )
    , joined_pedidos_item as (
        select
            fact_pedidos_item.sk_pedido_item
            , fact_pedidos_item.sk_pedido
            , fact_pedidos_item.sk_produto
            , fact_pedidos_item.quantidade_ordem
            , fact_pedidos.sk_cliente
            , fact_pedidos.sk_endereco
            , fact_pedidos.id_territorio
            , fact_pedidos.data_pedido
            , dim_produto.nome_produto
        from fact_pedidos_item
        left join fact_pedidos on fact_pedidos_item.sk_pedido = fact_pedidos.sk_pedido
        left join dim_produto on fact_pedidos_item.sk_produto = dim_produto.sk_produto
    )
    , joined_pedidos_cliente as (
        select
            joined_pedidos_item.sk_pedido_item
            , joined_pedidos_item.sk_pedido
            , joined_pedidos_item.sk_produto
            , joined_pedidos_item.quantidade_ordem
            , joined_pedidos_item.sk_cliente
            , joined_pedidos_item.sk_endereco
            , joined_pedidos_item.id_territorio
            , joined_pedidos_item.data_pedido
            , joined_pedidos_item.nome_produto
            , dim_cliente.nome_cliente
            , dim_cliente.tipo_cliente
        from joined_pedidos_item
        left join dim_cliente on joined_pedidos_item.sk_cliente = dim_cliente.sk_cliente
    )
    , agg_quantidade as (
        select
            nome_produto
            , nome_cliente
            , id_territorio
            , sk_endereco
            , extract(year from data_pedido) as ano
            , extract(month from data_pedido) as mes
            , date_trunc(data_pedido, day) as data_base
            , sum(quantidade_ordem) as quantidade
        from joined_pedidos_cliente
        where tipo_cliente = 'PJ'
        group by
            nome_produto
            , nome_cliente
            , id_territorio
            , sk_endereco
            , data_base
            , ano
            , mes       
    )
    , agg_quantidade_territorio as (
        select
            agg_quantidade.nome_produto
            , agg_quantidade.nome_cliente
            , case
                when dim_territorio.nome_regiao_pais = 'United States' then nome_estado_provincia
                else nome_regiao_pais
            end as regiao
            , agg_quantidade.ano
            , agg_quantidade.mes
            , agg_quantidade.data_base
            , agg_quantidade.quantidade
        from agg_quantidade
        left join dim_territorio on agg_quantidade.sk_endereco = dim_territorio.sk_endereco
    )
select * from agg_quantidade_territorio
