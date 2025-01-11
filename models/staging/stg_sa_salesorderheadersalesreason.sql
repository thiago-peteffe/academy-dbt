with
    source_data as (
        select
            salesorderid as id_pedido
            , salesreasonid as id_motivo_venda
        from {{ source('raw_sap_adw', 'salesorderheadersalesreason') }}
    )
select * from source_data