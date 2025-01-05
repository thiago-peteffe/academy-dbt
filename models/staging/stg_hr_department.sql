with
    source_data as (
        select
            departmentid as id_departamento
            , name as nome_departamento
            , groupname as grupo_departamento
        from {{ source('raw_sap_adw', 'department') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_departamento']) }} as sk_departamento
            , *
        from source_data
    )
select *
from source_with_sk