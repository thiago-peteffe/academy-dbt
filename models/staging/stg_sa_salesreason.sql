with
    source_data as (
        select
            salesreasonid as id_motivo_venda
            , name as nome_motivo_venda
            , reasontype as tipo_motivo_venda
        from {{ source('raw_sap_adw', 'salesreason') }}
    )
select * from source_data