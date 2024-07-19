from .dataset import (
    get_redfin_data
)
from .duck import (
    load_aws_secrets, 
    write_to_s3, 
    create_table_from_dataframe
    )
from .models import JobParameters, duckdb_table
import duckdb
from loguru import logger
import fire
import sys

logger.remove()  # Remove all other handlers
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
logger.add("debug_logs.log", rotation="100 MB", retention="10 days", level="DEBUG", format="{time} {level} {message}")

def main(params: JobParameters):
    with duckdb.connect(database=':memory:', read_only=False) as conn:
        df = get_redfin_data(params)

        # DuckDB creates table and uploads to s3 as parquet file
        conn.register('df', df)

        load_aws_secrets(conn)
        create_table_from_dataframe(conn, df, duckdb_table(params), params)
        write_to_s3(conn, params)


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(JobParameters(**kwargs)))
