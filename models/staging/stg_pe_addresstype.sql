with
    source_data as (
        select
            addresstypeid as id_tipo_endereco
            , name as nome_tipo_endereco
        from {{ source('raw_sap_adw', 'addresstype') }}
    )
select * from source_data