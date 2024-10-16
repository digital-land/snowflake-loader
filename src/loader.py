import requests
import click
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from snowflake.snowpark import Session
from snowflake.connector import ProgrammingError
from io import StringIO
# manually define datasets for now
DATASETS = [
    'title-boundary',
    'flood-risk-zone',
    'article-4-direction-area',
    'conservation-area'
]

def remove_dir_contents(path):
    for item in path.iterdir():
        if item.is_dir():
            remove_dir_contents(item)  # Recursively delete subdirectories
            item.rmdir()  # Remove the now-empty subdirectory
        else:
            item.unlink()

# Download CSV in streaming mode and write directly to Parquet
def download_and_convert_to_parquet(url, output_file, chunksize=1000000):
    Path(output_file).parent.mkdir(parents=True,exist_ok=True)
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Prepare the Parquet writer
        parquet_writer = None
        
        # Stream the CSV data in chunks
        for chunk in pd.read_csv(StringIO(response.text), chunksize=chunksize):
            table = pa.Table.from_pandas(chunk)
            
            # Initialize the Parquet writer on the first chunk
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(output_file, table.schema)
            
            # Write the chunk to Parquet
            parquet_writer.write_table(table)
        
        if parquet_writer:
            parquet_writer.close()
        
        print(f"Data successfully converted to {output_file}.")
        return output_file
    
    except requests.RequestException as e:
        print(f"Failed to download CSV: {e}")
        return None

# Create Snowflake session
def create_snowflake_session(conn_details):
    try:
        session = Session.builder.configs(conn_details).create()
        print("Snowflake session created successfully.")
        return session
    except ProgrammingError as e:
        print(f"Error connecting to Snowflake: {e}")
        raise e

# Upload Parquet data in batches to Snowflake
def upload_parquet_to_snowflake(parquet_file,conn_details,table_name=None, batch_size=1000):
    if table_name is None:
        table_name = Path(parquet_file).stem.replace('-','_')

    session = create_snowflake_session(conn_details)
    
    if session is None:
        return

    try:
        # Read Parquet file in batches and write to Snowflake
        parquet_reader = pq.ParquetFile(parquet_file)
        num_rows = parquet_reader.metadata.num_rows
        
        for batch_start in range(0, num_rows, batch_size):
            batch_end = min(batch_start + batch_size, num_rows)
            table = parquet_reader.read_row_group(batch_start // batch_size)
            batch_df = table.to_pandas()
            
            # Write to Snowflake DataFrame
            session.write_pandas(batch_df, table_name, overwrite=False,auto_create_table=True)
            print(f"Uploaded rows {batch_start} to {batch_end} to Snowflake.")
        
    except Exception as e:
        print(f"Failed to upload data to Snowflake: {e}")
    finally:
        session.close()
        print("Snowflake session closed.")



@click.command()
@click.option("--user", required=True)
@click.option("--password", required=True)
@click.option("--account", required=True)
@click.option("--database", required=True)
@click.option("--schema", required=True)
@click.option("--warehouse", required=True)
@click.option("--chunksize",type=click.types.IntParamType(),default=1000000)
@click.option("--use-cache",is_flag=True,default=True)
def load(user,password,account,database,schema,warehouse,chunksize,use_cache):
    cache_dir = Path('./cache')

    connection_details = {
        "user":user,
        "password":password,
        "account":account,
        "database":database,
        "schema":schema,
        "warehouse":warehouse,
    }
    if cache_dir.exists() and use_cache is False:
        remove_dir_contents(cache_dir)
        cache_dir.rmdir()
        cache_dir.mkdir(parents=True)


    for dataset in DATASETS:
        output_file=f'./cache/{dataset}.parquet'
        if not Path(output_file).exists():
            csv_url = f"https://files.planning.data.gov.uk/dataset/{dataset}.csv"
            output_file = download_and_convert_to_parquet(csv_url,output_file=output_file,chunksize=chunksize)

        if output_file:
            upload_parquet_to_snowflake(output_file,connection_details)


if __name__ == "__main__":
    load()