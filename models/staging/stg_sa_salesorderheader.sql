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
            , creditcardid as id_cartao_credito
            , salespersonid as id_vendedor
            , territoryid as id_territorio
            , shiptoaddressid as id_endereco
            , shipmethodid as metodo_envio
            , round(subtotal, 2) as venda_subtotal
            , round(taxamt, 2) as valor_imposto
            , round(freight, 2) as custo_envio
            , round(totaldue, 2) as venda_total
        from {{ source('raw_aw_sales', 'salesorderheader') }}
    )
select * from source_data