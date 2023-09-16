-- create analytics / BI ready dataset for dashboards and business users
with

    varieties as (select * from {{ ref("dim_variety") }}),

    benches as (select * from {{ ref("dim_bench") }}),

    harvest as (select * from {{ ref("fct_harvest_daily") }}),

    joined as (
        select
            harvest.harvest_id,
            harvest.harvest_date,
            harvest.batch_id,
            benches.bench_number,
            varieties.name as variety_name,
            harvest.harvest_weight,
            harvest.quality_score_yellowing,
            harvest.quality_score_glassiness,
            harvest.quality_score_tip_burn,
            harvest.quality_score_average
        from harvest
        inner join benches on harvest.bench_id = benches.bench_id
        inner join varieties on harvest.variety_id = varieties.variety_id
        order by variety_name, harvest_date asc
    ),

    enhanced as (
        select
            *,
            (
                case
                    when quality_score_average <= 15
                    then 'Good'
                    when quality_score_average > 15 and quality_score_average <= 35
                    then 'Average'
                    else 'Rubbish'
                end
            ) as quality_score_rag
        from joined
    )

select *
from enhanced
