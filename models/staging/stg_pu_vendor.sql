with
    source_data as (
        select
            businessentityid as id_entidade
            , accountnumber as id_fornecedor
            , name as nome_fornecedor
            , cast(activeflag as int) as status_fornecedor
        from {{ source('raw_aw_purchasing', 'vendor') }}
    )
select * from source_data