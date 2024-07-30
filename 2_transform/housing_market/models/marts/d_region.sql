with dim_table as (
    select
        $1:region_id::NUMBER(8, 0) as id,
        $1:region::VARCHAR(255) as region,
        $1:state::VARCHAR(255) as state,
        $1:state_id::NUMBER(4, 0) as state_id,
        $1:region_type_id::NUMBER(2, 0) as region_type_id
    from '@housing_market_dw.public.s3_stage/processed_data.parquet'
    order by state, region
)

select distinct id, region, state, state_id, region_type_id
from dim_table