with
    stg_sa_salesorderheadersalesreason as (
        select
            id_pedido
            , id_motivo_venda
        from {{ ref('stg_sa_salesorderheadersalesreason') }}
    )
    , stg_sa_salesorderheadersalesreason_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido', 'id_motivo_venda']) }} as sk_pedido_motivo_venda
            , {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from stg_sa_salesorderheadersalesreason
    )
select * from stg_sa_salesorderheadersalesreason_sk