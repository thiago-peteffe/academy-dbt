with
    source_data as (
        select
            customerid as id_cliente
            , personid as id_pessoa
            , storeid as id_loja
            , territoryid as id_territorio
        from {{ source('raw_sap_adw', 'customer') }}
    )
select * from source_data