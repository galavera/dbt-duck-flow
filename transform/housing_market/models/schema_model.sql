CALL load_aws_credentials ();

-- save table to duckdb memory
CREATE TABLE preprocessed_data AS (
  SELECT
    *
  FROM
    's3://dataspec/preprocessed/*/*/*.parquet'
  ORDER BY
    region_type DESC,
    region_name,
    period_begin
);

-- some small adjustments
ALTER TABLE preprocessed_data
DROP COLUMN region_type_id;

ALTER TABLE preprocessed_data
RENAME COLUMN region_name TO region;

ALTER TABLE preprocessed_data
RENAME COLUMN duration TO rolling_window_weeks;

UPDATE preprocessed_data
SET
  rolling_window_weeks = REPLACE(rolling_window_weeks, ' weeks', '');

ALTER TABLE preprocessed_data
ALTER COLUMN rolling_window_weeks type INT;

CREATE SEQUENCE serial start 1000000;

UPDATE preprocessed_data
SET
  id = NEXTVAL('serial');

ALTER TABLE preprocessed_data
ALTER COLUMN id
SET NOT NULL;

ALTER TABLE preprocessed_data
ALTER COLUMN period_begin
SET NOT NULL;

ALTER TABLE preprocessed_data
ALTER COLUMN period_end
SET NOT NULL;

-- create fact table --
CREATE TABLE fact_table AS (
  SELECT
    id,
    period_begin,
    period_end,
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
  FROM
    preprocessed_data
);

-- dimension tables --
-- region_type
CREATE TABLE dim_region_type (
  region_type_id INTEGER PRIMARY KEY,
  region_type VARCHAR UNIQUE NOT NULL
);

INSERT INTO
  dim_region_type (region_type_id, region_type)
VALUES
  (1, 'county'),
  (2, 'metro');

--region
CREATE TABLE dim_region (
  region_id BIGINT PRIMARY KEY,
  region_type_id INTEGER NOT NULL,
  region VARCHAR,
  FOREIGN KEY (region_type_id) REFERENCES dim_region_type (region_type_id)
);

INSERT INTO
  dim_region (region_id, region_type_id, region)
SELECT DISTINCT
  p.region_id,
  CASE
    WHEN p.region_type = 'county' THEN 1
    WHEN p.region_type = 'metro' THEN 2
  END AS region_type_id,
  p.region
FROM
  preprocessed_data p
ORDER BY
  p.region;

--rolling window
CREATE TABLE dim_time_window (id INTEGER PRIMARY KEY, weeks INTEGER UNIQUE);

INSERT INTO
  dim_time_window
VALUES
  (1, 1),
  (2, 4),
  (3, 12);

--state
CREATE TABLE dim_state (state_id BIGINT PRIMARY KEY, state VARCHAR UNIQUE);

CREATE SEQUENCE state_seq START 10;

WITH
  state AS (
    SELECT
      SPLIT_PART(region, ',', 2) AS split_region
    FROM
      dim_region
    GROUP BY
      split_region
  )
INSERT INTO
  dim_state
SELECT
  NEXTVAL('state_seq'),
  split_region
FROM
  state;

DELETE FROM DIM_STATE
WHERE
  LEN (STATE) > 3;