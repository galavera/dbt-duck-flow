with
    source as (
        select
            id,
            period_begin,
            period_end,
            duration_in_weeks,
            region_type,
            region_name as region,
            region_id,
            adjusted_average_new_listings,
            average_pending_sales_listing_updates,
            off_market_in_two_weeks,
            adjusted_average_homes_sold,
            median_new_listing_price,
            median_sale_price,
            median_days_to_close,
            median_new_listing_ppsf,
            active_listings,
            median_days_on_market,
            percent_active_listings_with_price_drops,
            age_of_inventory,
            months_of_supply,
            median_pending_sqft,
            average_sale_to_list_ratio,
            median_sale_ppsf
        from {{ ref('stg_housing_market__raw') }}
    ),
    states_data as (
        select *, dense_rank() over (order by state) as state_id
        from
            (
                select *, regexp_extract(region, '([A-Z]{2})') as state
                from source
                where region not in ('All Redfin Metros')
            ) as extraction
        order by region_type desc, region, period_begin
    )
select
    s.*,
    sd.state,
    sd.state_id,
    case
        when s.region_type = 'metro'
        then 1
        when s.region_type = 'county'
        then 2
        else null
    end as region_type_id,
    case
        when s.duration_in_weeks = 1
        then 1
        when s.duration_in_weeks = 4
        then 2
        when s.duration_in_weeks = 12
        then 3
        else null
    end as weeks_id
from source s
left join states_data sd on s.id = sd.id
