with
    stg_sa_salesorderdetail as (
        select
            id_pedido
            , id_pedido_item
            , quantidade_ordem
            , id_produto
            , round(preco_unitario, 2) as preco_unitario
            , round((quantidade_ordem * preco_unitario), 2) as preco_total 
            , round(desconto, 2) as desconto
        from {{ ref('stg_sa_salesorderdetail') }}
    )
    , stg_sa_salesorderdetail_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido_item']) }} as sk_pedido_item
            , {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido
            , {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from stg_sa_salesorderdetail
    )
select * from stg_sa_salesorderdetail_sk