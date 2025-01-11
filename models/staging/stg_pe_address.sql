with
    source_data as (
        select
            addressid as id_endereco
            , city as cidade
            , stateprovinceid as id_estado_provincia
        from {{ source('raw_sap_adw', 'address') }}
    )
select * from source_data