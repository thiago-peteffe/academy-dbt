with
    source_data as (
        select
            salesorderid as id_pedido
            , salesreasonid as id_motivo_venda
        from {{ source('raw_aw_sales', 'salesorderheadersalesreason') }}
    )
select * from source_data