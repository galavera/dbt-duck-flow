
with
    fact_table as (
        select
            $1:id::NUMBER(38, 0) as id,
            $1:period_begin::DATE as period_begin,
            $1:period_end::DATE as period_end,
            $1:duration_in_weeks::NUMBER(8, 0) as duration_in_weeks,
            $1:adjusted_average_new_listings::NUMBER(18, 0) as adjusted_average_new_listings,
            $1:median_new_listing_price::NUMBER(18, 0) as median_new_listing_price,
            $1:median_new_listing_ppsf::NUMBER(18, 2) as median_new_listing_ppsf,
            $1:active_listings::NUMBER(18, 0) as active_listings,
            $1:percent_active_listings_with_price_drops::NUMBER(18, 6) as percent_active_listings_with_price_drops,
            $1:adjusted_average_homes_sold::NUMBER(18, 0) as adjusted_average_homes_sold,
            $1:median_sale_price::NUMBER(18, 0) as median_sale_price,
            $1:median_sale_ppsf::NUMBER(18, 2) as median_sale_ppsf,
            $1:average_sale_to_list_ratio::NUMBER(18, 6) as average_sale_to_list_ratio,
            $1:average_pending_sales_listing_updates::NUMBER(18, 0) as average_pending_sales_listing_updates,
            $1:median_pending_sqft::NUMBER(18, 2) as median_pending_sqft,
            $1:off_market_in_two_weeks::NUMBER(18, 0) as off_market_in_two_weeks,
            $1:median_days_to_close::NUMBER(18, 1) as median_days_to_close,
            $1:median_days_on_market::NUMBER(18, 1) as median_days_on_market,
            $1:age_of_inventory::NUMBER(18, 1) as age_of_inventory,
            $1:months_of_supply::NUMBER(18, 1) as months_of_supply,
            $1:region_id::NUMBER(8, 0) as region_id,
            $1:region_type_id::NUMBER(2, 0) as region_type_id,
            $1:state_id::NUMBER(4, 0) as state_id
        from '@housing_market_dw.public.s3_stage/processed_data.parquet'
    )

select *
from fact_table
