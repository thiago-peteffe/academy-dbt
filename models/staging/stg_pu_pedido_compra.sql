with
    source_data as (
        select
            purchaseorderid as id_pedido_compra
            , status as status_pedido_compra
            , employeeid as id_colaborador
            , vendorid as id_fornecedor
            , shipmethodid as metodo_envio
            , orderdate as data_ordem
            , shipdate as data_envio
            , subtotal as compra_subtotal
            , taxamt as compra_imposto
            , freight as frete
        from {{ source('raw_sap_adw', 'purchaseorderheader') }}
    )
select * from source_data