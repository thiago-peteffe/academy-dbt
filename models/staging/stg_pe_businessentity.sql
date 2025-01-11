with
    source_data as (
        select businessentityid as id_entidade
        from {{ source('raw_sap_adw', 'businessentity') }}
    )
select * from source_data