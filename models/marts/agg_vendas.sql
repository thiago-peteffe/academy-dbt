with
    fact_pedidos as (
        select
            sk_pedido
            , sk_cliente
            , sk_endereco
            , data_pedido
            , status_pedido
            , flag_pedido
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
        from fact_pedidos
        left join dim_territorio on fact_pedidos.sk_endereco = dim_territorio.sk_endereco
    )
    , joined as (
        select
            fact_pedidos_territory.sk_pedido
            , fact_pedidos_territory.sk_cliente
            , fact_pedidos_territory.sk_endereco
            , fact_pedidos_territory.data_pedido
            , fact_pedidos_territory.status_pedido
            , fact_pedidos_territory.flag_pedido
            , dim_cliente.id_cliente
            , dim_cliente.nome_cliente
            , dim_cliente.id_territorio
            , dim_cliente.tipo_cliente
            , dim_cliente.id_vendedor
            , fact_pedidos_territory.nome_estado_provincia
            , fact_pedidos_territory.nome_territorio
            , fact_pedidos_territory.grupo_territorio
            , fact_pedidos_territory.nome_regiao_pais
            , fact_pedidos_territory.venda_subtotal
            , fact_pedidos_territory.valor_imposto
            , fact_pedidos_territory.custo_envio
            , fact_pedidos_territory.venda_total
        from fact_pedidos_territory
        left join dim_cliente on fact_pedidos_territory.sk_cliente = dim_cliente.sk_cliente
    )
select * from joined