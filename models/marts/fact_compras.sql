with
    stg_pu_purchaseorderheader as (
        select
            id_pedido_compra
            , status_pedido_compra
            , id_fornecedor
            , data_ordem
            , compra_subtotal
            , compra_imposto
            , frete
            , sum(compra_subtotal) + sum(compra_imposto) + sum(frete) as compra_total
        from {{ ref('stg_pu_purchaseorderheader') }}
        group by
            id_pedido_compra
            , status_pedido_compra
            , id_fornecedor
            , data_ordem
            , compra_subtotal
            , compra_imposto
            , frete
    )
    , stg_pu_purchaseorderheader_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido_compra']) }} as sk_pedido_compra
            , {{ numeric_surrogate_key(['id_fornecedor']) }} as sk_fornecedor
            , *
        from stg_pu_purchaseorderheader
    )
select * from stg_pu_purchaseorderheader_sk