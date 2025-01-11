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
select * from source_data