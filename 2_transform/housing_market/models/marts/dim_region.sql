{{
	config(
		database='housing_market_dw'
	)
}}
with source as (
    select
        $1:region_id::NUMBER(8, 0) as id,
        $1:region::VARCHAR(255) as region,
        $1:state::VARCHAR(255) as state,
        $1:state_id::NUMBER(4, 0) as state_id,
        $1:region_type_id::NUMBER(2, 0) as region_type_id
    from {{ source('external_stage', 's3_stage') }}
    order by state, region
)

select distinct id, region, state, state_id, region_type_id
from source