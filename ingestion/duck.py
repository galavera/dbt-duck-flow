from loguru import logger
from ingestion.models import JobParameters


def create_table_from_dataframe(duckdb_con, df, params: JobParameters):
    dataframe = df
    logger.info(f"Creating table {params.table_name} from dataframe")
    duckdb_con.execute(f"create table {params.table_name} as select * from dataframe;")


def load_aws_secrets(duckdb_conn):
    logger.info("loading AWS credentials")
    duckdb_conn.sql("CALL load_aws_credentials();")


def write_to_s3(duckdb_conn, params: JobParameters):
    """Write data to S3 bucket as parquet file"""
    logger.info(f"Writing data to s3 {params.s3_path}/{params.table_name}.parquet")
    duckdb_conn.execute(
        f"""
        COPY {params.table_name} TO '{params.s3_path}/{params.table_name}.parquet'
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
        """
    )
