import requests
import click
import json
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

def get_planning_data_datasets():

    response = requests.get('https://www.planning.data.gov.uk/dataset.json')
    response.raise_for_status()
    content = response.json()
    datasets = [dataset['dataset'] for dataset in content['datasets'] if dataset['entity-count'] > 0]
    return datasets

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
def create_snowflake_session(conn_params):
    try:
        session = Session.builder.configs(conn_params).create()
        print("Snowflake session created successfully.")
        return session
    except ProgrammingError as e:
        print(f"Error connecting to Snowflake: {e}")
        raise e

# Upload Parquet data in batches to Snowflake
def upload_parquet_to_snowflake(parquet_file,conn_params,table_name=None, batch_size=1000000):
    if table_name is None:
        table_name = Path(parquet_file).stem.replace('-','_')

    session = create_snowflake_session(conn_params)

    # remove current data
    check_sql = f"SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{conn_params['schema']}');"
    table_exists = session.sql(check_sql).collect()[0][0]

    if table_exists:
        truncate_sql = f"TRUNCATE TABLE {conn_params['database']}.{conn_params['schema']}.\"{table_name}\";"
        session.sql(truncate_sql).collect()

    if session is None:
        return

    try:
        # Read Parquet file in batches and write to Snowflake
        parquet_reader = pq.ParquetFile(parquet_file)
        num_row_groups = parquet_reader.num_row_groups
        
        for row_group in range(0, num_row_groups):
            table = parquet_reader.read_row_group(row_group)
            batch_df = table.to_pandas()
            
            # Write to Snowflake DataFrame
            session.write_pandas(batch_df, table_name, overwrite=False,auto_create_table=True)
            print(f"Uploaded row group {row_group} to Snowflake.")
        
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
    # organise connection details 
    # TODO look at a better way of getting this
    conn_params = {
        "user":user,
        "password":password,
        "account":account,
        "database":database,
        "schema":schema,
        "warehouse":warehouse,
    }


    datasets = get_planning_data_datasets()


    if cache_dir.exists() and use_cache is False:
        remove_dir_contents(cache_dir)
        cache_dir.rmdir()
        cache_dir.mkdir(parents=True)

    dataset_list_file = cache_dir / 'dataset_list.json'
    if dataset_list_file.exists():
        with open(dataset_list_file, 'r') as json_file:
            datasets = json.load(json_file)

    else:
        datasets = get_planning_data_datasets()
        with open(dataset_list_file, 'w') as json_file:
                json.dump(datasets, json_file, indent=4)

    for dataset in datasets:
        output_file=f'./cache/{dataset}.parquet'
        if not Path(output_file).exists():
            csv_url = f"https://files.planning.data.gov.uk/dataset/{dataset}.csv"
            output_file = download_and_convert_to_parquet(csv_url,output_file=output_file,chunksize=chunksize)

        if output_file:
            upload_parquet_to_snowflake(output_file,conn_params)


if __name__ == "__main__":
    load()