from ingestion.snowflake_query import (
    query_to_df,
    get_snowflake_client,
    build_snowflake_query,
    csv_to_snowflake,
)
from ingestion.models import FredParameters
from dotenv import load_dotenv
from pathlib import Path
import os

# set environment variables
path = Path("/root/env/.env")
load_dotenv(dotenv_path=path)
link = os.getenv("REDFIN_LINK")


def main(params: FredParameters):
    df = query_to_df(build_snowflake_query(params), get_snowflake_client())
    # Upload Redfin csv data to stage
    csv_to_snowflake(get_snowflake_client(connection="myconnection2"), link)
    return df


if __name__ == "__main__":
    df = main(params=FredParameters())
    df.show()
