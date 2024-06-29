from ingestion.query_dw import (
    get_redfin_data1
)
from ingestion.duck import (
    load_aws_secrets, 
    write_to_s3, 
    create_table_from_dataframe
    )
from ingestion.models import JobParameters, duckdb_table
import duckdb
#from typing import Any
from loguru import logger
import fire
import sys

logger.remove()  # Remove all other handlers
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
logger.add("debug_logs.log", rotation="100 MB", retention="10 days", level="DEBUG", format="{time} {level} {message}")

def main(params: JobParameters):
    with duckdb.connect(database=':memory:', read_only=False) as conn:
        df = get_redfin_data1(params)

        # DuckDB creates table and uploads to s3 as parquet file
        conn.register('df', df)

        load_aws_secrets(conn)
        create_table_from_dataframe(conn, df, duckdb_table(params), params)
        write_to_s3(conn, params)

        # Setup s3 as external table in snowflake
    #    get_snowflake_client(connection="myconnection2").sql(
    #        external_table_query(params)
    #    )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(JobParameters(**kwargs)))
