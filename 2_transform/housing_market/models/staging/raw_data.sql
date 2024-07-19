-- @block Load data from s3 bucket (see source.yml)
with
    raw_data as (
        select *
        from {{ source('external_source', 's3') }}
        order by region_type desc, region_name, period_begin
    ),
    -- Adjusting the duration column to be an integer and renaming it to
    -- duration_in_weeks
    step_1 as (
        select *, replace(duration, ' weeks', '')::int as duration_in_weeks
        from raw_data
    ),
    -- Adding a new id column to the dataset
    step_2 as (select *, row_number() over () + 1000000 as new_id from step_1)
-- Dropping the old id column and other unnecessary columns
select
    new_id as id,
    period_begin,
    period_end,
    duration_in_weeks,
    region_type,
    region_name,
    region_id,
    adjusted_average_new_listings,
    adjusted_average_new_listings_yoy,
    average_pending_sales_listing_updates,
    average_pending_sales_listing_updates_yoy,
    off_market_in_two_weeks,
    off_market_in_two_weeks_yoy,
    adjusted_average_homes_sold,
    adjusted_average_homes_sold_yoy,
    median_new_listing_price,
    median_new_listing_price_yoy,
    median_sale_price,
    median_sale_price_yoy,
    median_days_to_close,
    median_days_to_close_yoy,
    median_new_listing_ppsf,
    median_new_listing_ppsf_yoy,
    active_listings,
    active_listings_yoy,
    median_days_on_market,
    median_days_on_market_yoy,
    percent_active_listings_with_price_drops,
    percent_active_listings_with_price_drops_yoy,
    age_of_inventory,
    age_of_inventory_yoy,
    months_of_supply,
    months_of_supply_yoy,
    median_pending_sqft,
    median_pending_sqft_yoy,
    average_sale_to_list_ratio,
    average_sale_to_list_ratio_yoy,
    median_sale_ppsf,
    median_sale_ppsf_yoy
from step_2
