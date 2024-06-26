
from loguru import logger


def create_table_from_dataframe(duckdb_con, table_name: str, table_db: str, df):
    logger.info(f"Creating table {table_name} in local DuckDB")
    duckdb_con.sql(table_db)
    logger.info("inserting data into table")
    duckdb_con.sql(
        f"""
        INSERT INTO {table_name} 
            SELECT *
            FROM {df};
        """
    )


def load_aws_secrets(duckdb_conn):
    """Load AWS Credentials into DuckDB from .aws/credentials file"""
    if duckdb_conn.sql("CREATE SECRET secret2 (TYPE S3, PROVIDER CREDENTIAL_CHAIN);"):
        logger.info("Credentials loaded successfully")
    else:
        logger.error("Failed to load credentials")


def write_to_s3(
        duckdb_conn, table: str, s3_bucket: str
        ):
    """Write data to S3 bucket as parquet file"""
    logger.info(f"Writing data to s3 {s3_bucket}/{table}")
    duckdb_conn.sql(
        f"""
        COPY {table} TO 's3://{s3_bucket}/{table}.parquet'
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
        """
    )