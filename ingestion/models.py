from pydantic import BaseModel, Field
from typing import List, Optional, Annotated, Union
from dotenv import load_dotenv
import os
from pathlib import Path

path = Path('/root/env/.env')
load_dotenv(dotenv_path=path)


class JobParameters(BaseModel):
   # start_date: str = "2012-01-01"
   # end_date: str = "2024-06-24"
    table_variable: str = os.getenv("TABLE_VARIABLE")
    table_name: str = os.getenv("TABLE_NAME")
    redfin_data: Optional[str] = os.getenv("REDFIN_DATA")
    s3_path: Optional[str] = os.getenv("S3_PATH")


def duckdb_table(params: JobParameters):
    return f"""
    CREATE TABLE IF NOT EXISTS {params.table_name} (
        period_begin DATE,
        period_end DATE,
        region_type VARCHAR,
        region_type_id BIGINT,
        region_name VARCHAR,
        region_id BIGINT,
        duration VARCHAR,
        adjusted_average_new_listings DECIMAL(18, 6),
        adjusted_average_new_listings_yoy DECIMAL(18, 6),
        average_pending_sales_listing_updates DECIMAL(18, 6),
        average_pending_sales_listing_updates_yoy DECIMAL(18, 6),
        off_market_in_two_weeks DECIMAL(18, 6),
        off_market_in_two_weeks_yoy DECIMAL(18, 6),
        adjusted_average_homes_sold DECIMAL(18, 6),
        adjusted_average_homes_sold_yoy DECIMAL(18, 6),
        median_new_listing_price BIGINT,
        median_new_listing_price_yoy DECIMAL(18, 6),
        median_sale_price BIGINT,
        median_sale_price_yoy DECIMAL(18, 6),
        median_days_to_close DECIMAL(18, 6),
        median_days_to_close_yoy DECIMAL(18, 6),
        median_new_listing_ppsf DECIMAL(18, 6),
        median_new_listing_ppsf_yoy DECIMAL(18, 6),
        active_listings BIGINT,
        active_listings_yoy DECIMAL(18, 6),
        median_days_on_market DECIMAL(18, 6),
        median_days_on_market_yoy DECIMAL(18, 6),
        percent_active_listings_with_price_drops DECIMAL(18, 6),
        percent_active_listings_with_price_drops_yoy DECIMAL(18, 6),
        age_of_inventory DECIMAL(18, 6),
        age_of_inventory_yoy DECIMAL(18, 6),
        months_of_supply DECIMAL(18, 6),
        months_of_supply_yoy DECIMAL(18, 6),
        median_pending_sqft DECIMAL(18, 6),
        median_pending_sqft_yoy DECIMAL(18, 6),
        average_sale_to_list_ratio DECIMAL(18, 6),
        average_sale_to_list_ratio_yoy DECIMAL(18, 6),
        median_sale_ppsf DECIMAL(18, 6),
        median_sale_ppsf_yoy DECIMAL(18, 6)
    );
    """
