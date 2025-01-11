with
    source_data as (
        select
            businessentityid as id_entidade
            , name as nome_fornecedor
        from {{ source('raw_sap_adw', 'vendor') }}
    )
select * from source_data