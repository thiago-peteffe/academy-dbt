with
    source_data as (
        select
            salesorderid as id_pedido
            , date(timestamp(orderdate)) as data_pedido
            , date(timestamp(shipdate)) as data_envio_pedido
            , cast(status as int) as status_pedido
            , cast(onlineorderflag as int) as flag_pedido
            , purchaseordernumber as numero_pedido
            , customerid as id_cliente
            , salespersonid as id_vendedor
            , territoryid as id_territory
            , shiptoaddressid as id_endereco
            , shipmethodid as metodo_envio
            , round(subtotal, 2) AS venda_subtotal
            , round(taxamt, 2) AS valor_imposto
            , round(freight, 2) AS custo_envio
            , round(totaldue, 2) AS venda_total
        from {{ source('raw_aw_sales', 'salesorderheader') }}
    )
select * from source_data