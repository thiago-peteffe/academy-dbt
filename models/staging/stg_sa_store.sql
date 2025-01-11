with
    source_data as (
        select
            businessentityid as id_entidade
            , name as nome_loja
            , salespersonid as id_vendedor
        from {{ source('raw_sap_adw', 'store') }}
    )
select * from source_data