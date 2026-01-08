-- Dimensional table for dates

{{ config(materialized='table') }}

with date_spine as (
    -- Génération de 1200 jours (+ de 3 ans) à partir de 2016
    select 
        dateadd(day, seq4(), '2016-01-01'::date) as date_day
    from table(generator(rowcount => 1200))
)

select
    date_day as date_id, -- PK
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name, -- Ex: 'Jan', 'Feb'
    dayofweek(date_day) as day_of_week, -- 0-6 (0=Dimanche, 6=Samedi)
    dayname(date_day) as day_name, -- Ex: 'Mon', 'Tue'
    -- Est-ce un weekend ?
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine