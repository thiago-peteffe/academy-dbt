with
    source_data as (
        select
            salesreasonid as id_motivo_venda
            , name as nome_motivo_venda
            , reasontype as tipo_motivo_venda
        from {{ source('raw_sap_adw', 'salesreason') }}
    )
    , source_with_sk as (
        select
            {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from source_data
    )
select *
from source_with_sk