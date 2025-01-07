with
    source_data as (
        select
            customerid as id_cliente
            , personid as id_pessoa
            , storeid as id_loja
            , territoryid as id_territorio
        from {{ source('raw_sap_adw', 'customer') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , *
        from source_data
    )
select *
from source_with_sk