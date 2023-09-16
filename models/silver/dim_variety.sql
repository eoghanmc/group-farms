-- create Variety dimension view
with raw_varieties as (select distinct variety as name from {{ ref("harvest_raw") }})

select
    {{ dbt_utils.generate_surrogate_key(["name"]) }} as variety_id,  -- generate a unique key for each variety
    name
from raw_varieties
