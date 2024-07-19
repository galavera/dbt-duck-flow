{% macro local_export(table, local_path) %}

COPY {{ table }} TO '{{ local_path }}/{{ table }}.parquet'
(FORMAT PARQUET, OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);

{% endmacro %}