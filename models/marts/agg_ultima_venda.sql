with
    agg_vendas as (
        select *
        from {{ ref('agg_vendas') }}
        where extract(year from data_pedido) <> 2014
    )
    , deduplicacao as (
        select
            sk_pedido
            , sk_cliente
            , sk_endereco
            , data_pedido
            , status_pedido
            , flag_pedido
            , id_cliente
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , id_vendedor
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
            , venda_subtotal
            , valor_imposto
            , custo_envio
            , venda_total
            , row_number() over (
                partition by sk_cliente
                order by data_pedido desc
            ) as index_tabela
        from agg_vendas
    )
    , ultima_venda as (
        select
            sk_pedido
            , sk_cliente
            , sk_endereco
            , data_pedido
            , status_pedido
            , flag_pedido
            , id_cliente
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , id_vendedor
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
            , venda_subtotal
            , valor_imposto
            , custo_envio
            , venda_total
        from deduplicacao
        where index_tabela = 1
    )
select * from ultima_venda