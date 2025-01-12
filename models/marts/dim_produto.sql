with
    stg_pr_product as (
        select
            id_produto
            , nome_produto
            , numero_produto
            , case
                when origem_produto = 0 then 'Comprado'
                when origem_produto = 1 then 'Manufaturado'
                else 'Não especificado'
            end as tipo_produto
            , case
                when vendavel = 0 then 'Não vendável'
                when vendavel = 1 then 'Vendável'
                else 'Não especificado'
            end as tipo_comercial
            , cor_produto
            , custo_padrao
            , preco_venda
            , case
                when linha = 'R' then 'Estrada'
                when linha = 'M' then 'Montanha'
                when linha = 'T' then 'Turismo'
                when linha = 'S' then 'Padrão'
                else 'Não especificado'
            end as linha_produto
            , case
                when classe = 'H' then 'Alto'
                when classe = 'M' then 'Médio'
                when classe = 'L' then 'Baixo'
                else 'Não especificado'
            end as classe_produto
            , case
                when estilo = 'W' then 'Feminino'
                when estilo = 'M' then 'Masculino'
                when estilo = 'U' then 'Universal'
                else 'Não especificado'
            end as estilo_produto
            , id_subcategoria_produto
            , id_modelo_produto
        from {{ ref('stg_pr_product') }}
    )
    , stg_pr_product_with_sk as (
        select
            {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from stg_pr_product
    )
    , stg_pr_productcategory as (
        select
            id_categoria_produto
            , nome_categoria
        from {{ ref('stg_pr_productcategory') }}
    )
    , stg_pr_productmodel as (
        select
            id_modelo_produto
            , nome_modelo
        from {{ ref('stg_pr_productmodel') }}
    )
    , stg_pr_productsubcategory as (
        select
            id_subcategoria_produto
            , id_categoria_produto
            , nome_subcategoria
        from {{ ref('stg_pr_productsubcategory') }}
    )
    , joined_category as (
        select
            stg_pr_productcategory.id_categoria_produto
            , stg_pr_productcategory.nome_categoria
            , stg_pr_productsubcategory.id_subcategoria_produto
            , stg_pr_productsubcategory.nome_subcategoria
        from stg_pr_productcategory
        left join stg_pr_productsubcategory on stg_pr_productsubcategory.id_categoria_produto = stg_pr_productcategory.id_categoria_produto
    )
    , joined as (
        select
            stg_pr_product_with_sk.sk_produto
            , stg_pr_product_with_sk.id_produto
            , stg_pr_product_with_sk.nome_produto
            , stg_pr_product_with_sk.numero_produto
            , stg_pr_product_with_sk.tipo_produto
            , stg_pr_product_with_sk.tipo_comercial
            , stg_pr_product_with_sk.cor_produto
            , stg_pr_product_with_sk.custo_padrao
            , stg_pr_product_with_sk.preco_venda
            , stg_pr_product_with_sk.linha_produto
            , stg_pr_product_with_sk.classe_produto
            , stg_pr_product_with_sk.estilo_produto
            , stg_pr_productmodel.nome_modelo
            , joined_category.nome_categoria
            , joined_category.nome_subcategoria
        from stg_pr_product_with_sk
        left join stg_pr_productmodel on stg_pr_product_with_sk.id_modelo_produto = stg_pr_productmodel.id_modelo_produto
        left join joined_category on stg_pr_product_with_sk.id_subcategoria_produto = joined_category.id_subcategoria_produto
    )
select * from joined