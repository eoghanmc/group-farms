-- aggregate harvest daily data into one row for each date
with

    harvest_daily as (select * from {{ ref("harvest_daily") }}),

    aggregate_by_date as (
        select
            harvest_date,
            cast(count(bench_number) as integer) as count_benches,
            round(sum(harvest_weight), 2) as total_harvest_weight,
            round(avg(harvest_weight), 2) as average_harvest_weight,
            round(min(harvest_weight), 2) as min_harvest_weight,
            round(max(harvest_weight), 2) as max_harvest_weight
        from harvest_daily
        group by harvest_date
        order by harvest_date
    ),

    prior_value_harvest as (
        select
            *,
            lag(total_harvest_weight, 1) over (
                order by harvest_date
            ) as prior_total_harvest_weight,
            lag(average_harvest_weight, 1) over (
                order by harvest_date
            ) as prior_average_harvest_weight,
            round(
                avg(total_harvest_weight) over (order by harvest_date rows 7 preceding),
                2
            ) as average_harvest_7d_moving_average,
            (count_benches / 10) as bench_utilisation_rate
        from aggregate_by_date
    )

select *
from prior_value_harvest
