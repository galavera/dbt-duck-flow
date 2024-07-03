import pandas as pd
import requests
from ingestion.models import JobParameters
from ingestion.snowflake_q import (
    query_to_df, get_snowflake_client, 
    build_snowflake_query
    )
from io import StringIO
from tqdm import tqdm
from loguru import logger


def get_redfin_data(params: JobParameters) -> pd.DataFrame:
    try:
        response = requests.get(params.redfin_data, stream=True)
        response.raise_for_status()  # Check that the request was successful
        total_size = int(response.headers.get('content-length', 0))

        with tqdm(total=total_size, desc="Downloading Redfin Data", unit='MB', unit_scale=True, ncols=80,
                  bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            binary_data = bytearray()
            count = 0
            for chunk in response.iter_content(chunk_size=4096):
                binary_data.extend(chunk)
                pbar.update(len(chunk))
                count += 1
                
                #if count == 100: # For testing purposes
                    #break

            # Decode the entire binary data at once
            decoded_data = binary_data.decode('utf-8')
            df = pd.read_csv(StringIO(decoded_data), sep='\t', encoding='utf-8', index_col=False)
            df = df.sort_values(['region_type', 'region_name', 'period_begin'])
            df.reset_index(inplace=True)
            df.index = df.index + 1
            df.index.name = 'id'
            df.drop(columns=['index'], inplace=True)
            df.reset_index(inplace=True)
            return df
    except Exception as e:
        logger.error(f"Failed to get Redfin data: {e}")
        raise   

def fin_data_source(params: JobParameters):
    # Add a new source to the pipeline from the snowflake marketplace
    df = query_to_df(build_snowflake_query(params), get_snowflake_client())
    return df