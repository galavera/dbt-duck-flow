from loguru import logger
from .models import JobParameters


def create_table_from_dataframe(duckdb_con, df, table, params: JobParameters):
    dataframe = df
    try:
        logger.info(f"Creating table '{params.table_name}'")
        duckdb_con.execute(
            f"{table}"
            f"INSERT OR REPLACE INTO {params.table_name} SELECT * FROM dataframe"
            )
    except Exception as e:
        logger.error(f"Error creating table {params.table_name}: {e}")


def load_aws_secrets(duckdb_con):
    """
    This loads AWS credentials from ~/.aws folder into your DuckDB connection.
    
    Note: You can also set them as environment variables.
    """
    logger.info("loading AWS credentials")
    duckdb_con.sql("CALL load_aws_credentials();")


def write_to_s3(duckdb_con, params: JobParameters):
    """Write data to S3 bucket as parquet file"""
    logger.info(f"Writing data to {params.s3_path}")
    duckdb_con.execute("SET threads = 4;")
    duckdb_con.execute(
        f"""
        COPY {params.table_name} TO '{params.s3_path}/{params.table_name}'
        (FORMAT PARQUET, PARTITION_BY (duration), OVERWRITE_OR_IGNORE true, 
        COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000, FILENAME_PATTERN "redfin");
        """
    )
