with
    source_data as (
        select
            salesorderid as id_pedido
            , salesorderdetailid as id_pedido_item
            , orderqty as quantidade_ordem
            , productid as id_produto
            , specialofferid as id_cupom
            , unitprice as preco_unitario
            , unitpricediscount as desconto
        from {{ source('raw_sap_adw', 'salesorderdetail') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido_item']) }} as sk_pedido_item
            , *
        from source_data
    )
select *
from source_with_sk