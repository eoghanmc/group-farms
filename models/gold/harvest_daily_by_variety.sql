-- aggregate harvest daily data into one row for each variety for each date
with

    harvest_daily as (select * from {{ ref("harvest_daily") }}),

    aggregate_by_variety as (
        select
            harvest_date,
            variety_name,
            cast(count(bench_number) as integer) as count_benches,
            round(sum(harvest_weight), 2) as total_harvest_weight,
            round(avg(harvest_weight), 2) as average_harvest_weight,
            round(min(harvest_weight), 2) as min_harvest_weight,
            round(max(harvest_weight), 2) as max_harvest_weight
        from harvest_daily
        group by harvest_date, variety_name
        order by variety_name, harvest_date
    ),

    prior_value_harvest as (
        select
            *,
            lag(average_harvest_weight, 1) over (
                partition by variety_name order by harvest_date asc
            ) as prior_average_harvest_weight
        from aggregate_by_variety
    )

select *
from prior_value_harvest
