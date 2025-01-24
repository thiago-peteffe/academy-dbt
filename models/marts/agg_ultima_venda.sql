with
    agg_vendas as (
        select *
        from {{ ref('agg_vendas') }}
        where extract(year from data_pedido) <> 2014
    )
    , deduplicacao as (
        select
            *
            , row_number() over (
                partition by sk_cliente
                order by data_pedido desc
            ) as index_tabela
        from agg_vendas
    )
    , ultima_venda as (
        select
            *
        from deduplicacao
        where index_tabela = 1
    )
select * from ultima_venda