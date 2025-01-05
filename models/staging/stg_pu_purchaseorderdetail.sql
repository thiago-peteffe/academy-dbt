with
    source_data as (
        select
            purchaseorderid as id_pedido_compra
            , purchaseorderdetailid as id_pedido_compra_item
            , orderqty as quantidade_ordem
            , productid as id_produto
            , unitprice as preco_unitario
            , receivedqty as quantidade_recebida
            , rejectedqty as quantidade_rejeitada
        from {{ source('raw_sap_adw', 'purchaseorderdetail') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido_compra_item']) }} as sk_pedido_compra_item
            , *
        from source_data
    )
select *
from source_with_sk