with
    source_data as (
        select businessentityid as id_entidade
        from {{ source('raw_aw_person', 'businessentity') }}
    )
select * from source_data