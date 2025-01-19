with
    source_data as (
        select
            customerid as id_cliente
            , personid as id_pessoa
            , storeid as id_loja
            , territoryid as id_territorio
        from {{ source('raw_aw_sales', 'customer') }}
    )
select * from source_data