-- create a calendar date table to support date filtering and aggregation across models
{{ config(materialized="table") }}  -- build as table to optimise query performance

with dates as ({{ dbt_date.get_date_dimension("2023-06-01", "2023-09-01") }})

select *
from dates
order by date_day
