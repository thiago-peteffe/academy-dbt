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
select * from source_data