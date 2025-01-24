with
    stg_sa_salesorderheader as (
        select
            id_pedido
            , data_pedido
            , data_envio_pedido
            , case
                when status_pedido = 1 then 'Em processamento'
                when status_pedido = 2 then 'Aprovado'
                when status_pedido = 3 then 'Em espera'
                when status_pedido = 4 then 'Rejeitado'
                when status_pedido = 5 then 'Enviado'
                when status_pedido = 6 then 'Cancelado'
                else 'Não especificado'
            end as status_pedido
            , case
                when flag_pedido = 1 then 'Pedido feito pelo vendedor'
                when flag_pedido = 2 then 'Pedido feito on-line pelo cliente'
                else 'Não especificado'
            end as flag_pedido
            , numero_pedido
            , id_cliente
            , case
                when id_cartao_credito is not null then 'Cartão de Crédito'
                else 'Outros Meios'
            end as metodo_pagamento
            , id_vendedor
            , id_territory
            , id_endereco
            , venda_subtotal
            , valor_imposto
            , custo_envio
            , venda_total
        from {{ ref('stg_sa_salesorderheader') }}
    )
    , stg_sa_salesorderheader_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido
            , {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , *
        from stg_sa_salesorderheader
    )
select
    sk_pedido
    , sk_cliente
    , sk_endereco
    , sk_vendedor
    , id_pedido
    , id_cliente
    , id_endereco
    , id_vendedor
    , id_territory
    , data_pedido
    , status_pedido
    , flag_pedido
    , numero_pedido
    , metodo_pagamento
    , venda_subtotal
    , valor_imposto
    , custo_envio
    , venda_total
from stg_sa_salesorderheader_sk