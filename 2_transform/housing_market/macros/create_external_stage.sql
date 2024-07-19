{% macro external_stage(stage_name, ext_location) %}
{% set db_schema = env_var('DB_SCHEMA', 'housing_market_dw.public') %}
USE SCHEMA {{ db_schema }};

CREATE OR REPLACE STAGE {{ stage_name }} 
	URL = '{{ ext_location }}' 
	STORAGE_INTEGRATION = S3_STORAGE
  FILE_FORMAT = parquet_format;
{% endmacro %}