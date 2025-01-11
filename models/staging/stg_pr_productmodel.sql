with
    source_data as (
        select
            productmodelid as id_modelo_produto
            , name as nome_modelo
        from {{ source('raw_sap_adw', 'productmodel') }}
    )
select * from source_data