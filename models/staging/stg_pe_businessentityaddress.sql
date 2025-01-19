with
    source_data as (
        select
            businessentityid as id_entidade
            , addressid as id_endereco
            , addresstypeid as id_tipo_endereco
        from {{ source('raw_aw_person', 'businessentityaddress') }}
    )
select * from source_data