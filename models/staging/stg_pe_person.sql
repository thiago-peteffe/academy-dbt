with
    source_data as (
        select
            businessentityid as id_entidade
            , persontype as tipo_pessoa
            , coalesce(firstname, '') || ' ' || coalesce(lastname, '') as nome_completo
        from {{ source('raw_sap_adw', 'person') }}
    )
select * from source_data