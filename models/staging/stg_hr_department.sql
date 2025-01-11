with
    source_data as (
        select
            departmentid as id_departamento
            , name as nome_departamento
            , groupname as grupo_departamento
        from {{ source('raw_sap_adw', 'department') }}
    )
select * from source_data