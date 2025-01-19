with
    source_data as (
        select
            departmentid as id_departamento
            , name as nome_departamento
            , groupname as grupo_departamento
        from {{ source('raw_aw_humanresources', 'department') }}
    )
select * from source_data