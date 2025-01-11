with
    source_data as (
        select
            salesorderid as id_pedido
            , orderdate as data_pedido
            , shipdate as data_envio_pedido
            , status as status_pedido
            , onlineorderflag as flag_pedido
            , purchaseordernumber as numero_pedido
            , customerid as id_cliente
            , salespersonid as id_vendedor
            , territoryid as id_territory
            , shiptoaddressid as endereco_entrega
            , shipmethodid as metodo_envio
            , subtotal as venda_subtotal
            , taxamt as valor_imposto
            , freight as custo_envio
            , totaldue as venda_total
        from {{ source('raw_sap_adw', 'salesorderheader') }}
    )
select * from source_data