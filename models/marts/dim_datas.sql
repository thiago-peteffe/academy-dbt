{{
    config(
        materialized = "table"
    )
}}

with
    raw_generated_data as (
        {{ dbt_date.get_date_dimension("2010-01-01", "2015-01-01") }}
    )
    , generated_date as (
        select
            date_day as data
            , day_of_week as dia_da_semana
            , day_of_week_iso as dia_da_semana_iso
            , case
                when raw_generated_data.day_of_week = 1 then "Domingo"
                when raw_generated_data.day_of_week = 2 then "Segunda-feira"
                when raw_generated_data.day_of_week = 3 then "Terça-feira"
                when raw_generated_data.day_of_week = 4 then "Quarta-feira"
                when raw_generated_data.day_of_week = 5 then "Quinta-feira"
                when raw_generated_data.day_of_week = 6 then "Sexta-feira"
                when raw_generated_data.day_of_week = 7 then "Sábado"
            end as nome_dia_da_semana
            , day_of_month as dia_do_mes
            , week_of_year as semana_do_ano
            , month_of_year as mes
            , case
                when month_of_year = 1 then "Janeiro"
                when month_of_year = 2 then "Fevereiro"
                when month_of_year = 3 then "Março"
                when month_of_year = 4 then "Abril"
                when month_of_year = 5 then "Maio"
                when month_of_year = 6 then "Junho"
                when month_of_year = 7 then "Julho"
                when month_of_year = 8 then "Agosto"
                when month_of_year = 9 then "Setembro"
                when month_of_year = 10 then "Outubro"
                when month_of_year = 11 then "Novembro"
                when month_of_year = 12 then "Dezembro"
            end as nome_do_mes
            , case
                when month_of_year in (1, 2) then 1
                when month_of_year in (3, 4) then 2
                when month_of_year in (5, 6) then 3
                when month_of_year in (7, 8) then 4
                when month_of_year in (9, 10) then 5
                when month_of_year in (11, 12) then 6
            end as bimestre
            , quarter_of_year as trimestre
            , case
                when month_of_year in (1, 2, 3, 4, 5, 6) then 1
                else 2
            end as semestre
            , year_number as ano
        from raw_generated_data
    )
select *
from generated_date
