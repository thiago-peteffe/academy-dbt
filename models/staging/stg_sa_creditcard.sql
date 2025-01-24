with
    source_data as (
        select
            creditcardid as id_cartao_credito
            , cardtype as tipo_cartao
        from {{ source('raw_aw_sales', 'creditcard') }}
    )
select * from source_data