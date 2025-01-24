with
    source_data as (
        select
            businessentityid as id_vendedor
            , territoryid as id_territorio
            , commissionpct as comissao
        from {{ source('raw_aw_sales', 'salesperson') }}
    )
select * from source_data