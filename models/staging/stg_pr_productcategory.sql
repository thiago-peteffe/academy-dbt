with
    source_data as (
        select
            productcategoryid as id_categoria_produto
            , name as nome_categoria
        from {{ source('raw_aw_production', 'productcategory') }}
    )
select * from source_data