with
    source_data as (
        select
            businessentityid as id_entidade
            , nationalidnumber as id_documento
            , jobtitle as cargo
            , birthdate as data_nascimento
            , gender as sexo
            , hiredate as data_contratacao
            , currentflag as situacao
        from {{ source('raw_sap_adw', 'employee') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_entidade']) }} as sk_entidade
            , *
        from source_data
    )
select *
from source_with_sk