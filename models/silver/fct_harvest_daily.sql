-- create daily harvest and leaf quality fact table,
-- and normalise the dimensional and fact tables to reduce data redundancy
with

    varieties as (select * from {{ ref("dim_variety") }}),

    benches as (select * from {{ ref("dim_bench") }}),

    raw_harvest_data as (
        select
            harvest_date,
            cast(batch_id as varchar) as batch_id,
            cast(bench_number as varchar) as bench_number,
            variety as variety_name,
            harvest_weight
        from {{ ref("harvest_raw") }}
    ),

    normalised_harvest as (
        select h.harvest_date, h.batch_id, b.bench_id, v.variety_id, h.harvest_weight
        from raw_harvest_data as h
        inner join varieties as v on h.variety_name = v.name
        inner join benches as b on h.bench_number = b.bench_number
    ),

    raw_leaf_quality_data as (
        select
            harvest_date,
            cast(batch_id as varchar) as batch_id,
            cast(bench_number as varchar) as bench_number,
            yellowing as quality_score_yellowing,
            glassiness as quality_score_glassiness,
            tip_burn as quality_score_tip_burn
        from {{ ref("leaf_quality_raw") }}
    ),

    normalised_leaf_quality as (
        select
            q.harvest_date,
            q.batch_id,
            b.bench_id,
            q.quality_score_yellowing,
            q.quality_score_glassiness,
            q.quality_score_tip_burn
        from raw_leaf_quality_data as q
        inner join benches as b on q.bench_number = b.bench_number
    ),

    enhanced_leaf_quality as (
        select
            *,
            round(
                (
                    quality_score_yellowing
                    + quality_score_glassiness
                    + quality_score_tip_burn
                )
                / 3,
                2
            ) as quality_score_average
        from normalised_leaf_quality
    ),

    joined as (
        select
            h.harvest_date,
            h.batch_id,
            h.bench_id,
            h.variety_id,
            h.harvest_weight,
            q.quality_score_yellowing,
            q.quality_score_glassiness,
            q.quality_score_tip_burn,
            q.quality_score_average
        from normalised_harvest as h
        inner join
            enhanced_leaf_quality as q
            on h.harvest_date = q.harvest_date
            and h.batch_id = q.batch_id
            and h.bench_id = q.bench_id
    )

select *
from joined
