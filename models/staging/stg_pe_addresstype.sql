with
    source_data as (
        select
            addresstypeid as id_tipo_endereco
            , name as nome_tipo_endereco
        from {{ source('raw_aw_person', 'addresstype') }}
    )
select * from source_data