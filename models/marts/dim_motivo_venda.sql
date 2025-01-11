with
    stg_sa_salesreason as (
        select
            id_motivo_venda
            , nome_motivo_venda
            , tipo_motivo_venda
        from {{ ref('stg_sa_salesreason') }}
    )
    , stg_sa_salesreason_sk as (
        select
            {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from stg_sa_salesreason
    )
select * from stg_sa_salesreason_sk