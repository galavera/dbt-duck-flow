{% macro external_stage_export(table) %}
copy into @s3_stage/out/{{ table }}
from (select * from {{ ref('f_housing_market') }})
  FILE_FORMAT = (TYPE = 'PARQUET')
  header = true
  OVERWRITE_OR_IGNORE = TRUE
{% endmacro %}
