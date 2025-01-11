with
    source_data as (
        select
            salesreasonid as id_motivo_venda
            , case
                when salesreasonid = 1 then 'Preço'
                when salesreasonid = 2 then 'Em promoção'
                when salesreasonid = 3 then 'Anúncio em revista'
                when salesreasonid = 4 then 'Anúncio em televisão'
                when salesreasonid = 5 then 'Fabricante'
                when salesreasonid = 6 then 'Avaliação'
                when salesreasonid = 7 then 'Evento de demonstração'
                when salesreasonid = 8 then 'Patrocínio'
                when salesreasonid = 9 then 'Qualidade'
                when salesreasonid = 10 then 'Outro'
            end as nome_motivo_venda
            , case
                when salesreasonid = 1 then 'Outro'
                when salesreasonid = 2 then 'Promoção'
                when salesreasonid = 3 then 'Marketing'
                when salesreasonid = 4 then 'Marketing'
                when salesreasonid = 5 then 'Outro'
                when salesreasonid = 6 then 'Outro'
                when salesreasonid = 7 then 'Marketing'
                when salesreasonid = 8 then 'Marketing'
                when salesreasonid = 9 then 'Outro'
                when salesreasonid = 10 then 'Outro'
            end as tipo_motivo_venda
        from {{ source('raw_sap_adw', 'salesreason') }}
    )
select * from source_data