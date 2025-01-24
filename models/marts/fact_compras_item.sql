with
    stg_pu_purchaseorderdetail as (
        select
            id_pedido_compra
            , id_pedido_compra_item
            , quantidade_ordem
            , id_produto
            , preco_unitario
            , quantidade_recebida
            , quantidade_rejeitada
        from {{ ref('stg_pu_purchaseorderdetail') }}
    )
    , stg_pu_purchaseorderdetail_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido_compra']) }} as sk_pedido_compra
            , {{ numeric_surrogate_key(['id_pedido_compra_item']) }} as sk_pedido_compra_item
            , {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from stg_pu_purchaseorderdetail
    )
select * from stg_pu_purchaseorderdetail_sk