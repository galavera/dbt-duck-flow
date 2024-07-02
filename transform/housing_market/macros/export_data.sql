{% macro export_data(table) %}
{% set s3_path = env_var('S3_OUTPUT', 's3-bucket-path') %}
COPY {{ table }} TO '{{ s3_path }}/{{ table }}.parquet'
(FORMAT PARQUET, OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
{% endmacro %}