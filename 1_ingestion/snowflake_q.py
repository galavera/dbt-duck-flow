# setup connection to snowflake using snowpark API
from snowflake.snowpark import Session, DataFrameWriter
from .models import JobParameters
import pandas as pd
import time
from loguru import logger



def get_snowflake_client(params: JobParameters) -> Session:
    """Create a connection to Snowflake"""
    try:
        # setup connection to snowflake using connections.toml
        # see snowflake documentation for more information on setting up connections.toml
        session = Session.builder.config("connection_name", params.connection.str).create()
        return session
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def query_to_df(query_str: str, session) -> pd.DataFrame:
    """Get query results from Snowflake"""
    try:
        start_time = time.time()
        logger.info(f"Executing query: {query_str}")
        dataframe = session.sql(query_str)
        elapsed_time = time.time() - start_time
        logger.info(f"Query executed and data fetched in {elapsed_time:.2f} seconds")
        return dataframe

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


def build_snowflake_query(params: JobParameters) -> str:
    # Query the fred database
    return f"""
    SELECT ts.date, at.variable_name, 
        ts.value, at.frequency,
        at.release_name
    FROM financial_fred_timeseries as ts
    JOIN financial_fred_attributes as at ON (ts.variable = at.variable)
    WHERE at.variable_name like '{params.table_variable}%'
        AND ts.date between '{params.start_date}' and '{params.end_date}'
    ORDER BY ts.date
    LIMIT 5
    """

def external_table_query(params: JobParameters) -> str:
    return f"""
    CREATE OR REPLACE EXTERNAL TABLE {params.table_name}
        USING (TYPE = s3)
        WITH LOCATION = @s3://{params.s3_path}
        FILE_FORMAT = (TYPE = PARQUET)
        COMPRESSION = AUTO;
        """

def s3_to_snowflake(session: Session, df):
    """Load data into Snowflake stage"""
    try:
        # Upload file to Snowflake stage
        dataframe = session.create_dataframe(df)
        dataframe.write.copy_into_location(
            "@dw/state_data",
            partition_by=None,
            file_format_name="my_tsv_format",
            file_format_type="csv",
            overwrite=True,
            header=True,
        )
        logger.debug("(Result)")
    except Exception as e:
        logger.error(f"Failed to load data into Snowflake: {e}")
        raise
    finally:
        session.close()
