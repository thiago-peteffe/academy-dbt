with
    fact_pedidos as (
        select
            sk_cliente
            , sk_endereco
            , data_pedido
            , status_pedido
            , id_cliente
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
            , venda_subtotal
            , valor_imposto
            , custo_envio
            , venda_total
        from {{ ref('agg_vendas') }}
        where extract(year from data_pedido) = 2013
    )
    , top10 as (
        select
            sk_cliente
            , sk_endereco
            , id_cliente
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
            , sum(venda_subtotal) as venda_liquida
            , sum(venda_total) as faturamento
        from fact_pedidos
        group by
            sk_cliente
            , sk_endereco
            , id_cliente
            , nome_cliente
            , id_territorio
            , tipo_cliente
            , nome_estado_provincia
            , nome_territorio
            , grupo_territorio
            , nome_regiao_pais
        order by faturamento desc
        limit 10
    )
select
    sk_cliente
    , sk_endereco
    , id_cliente
    , nome_cliente
    , id_territorio
    , tipo_cliente
    , nome_estado_provincia
    , nome_territorio
    , grupo_territorio
    , nome_regiao_pais
    , venda_liquida
    , faturamento
from top10