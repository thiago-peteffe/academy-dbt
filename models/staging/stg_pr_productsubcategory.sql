with
    source_data as (
        select
            productsubcategoryid as id_subcategoria_produto
            , productcategoryid as id_categoria_produto
            , name as nome_subcategoria
        from {{ source('raw_sap_adw', 'productsubcategory') }}
    )
select * from source_data