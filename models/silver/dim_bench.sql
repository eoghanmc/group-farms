-- create Bench dimension view
with
    raw_benches as (
        select distinct cast(bench_number as varchar) as bench_number
        from {{ ref("harvest_raw") }}
    )

select
    {{ dbt_utils.generate_surrogate_key(["bench_number"]) }} as bench_id,  -- generate a unique key for each bench number
    bench_number
from raw_benches
