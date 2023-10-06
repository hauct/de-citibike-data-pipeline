import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow.parquet as pq
from zipfile import ZipFile
from io import BytesIO
import requests
import os
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

# Define constants
PROJECT_ID = "hauct-de-citibike-pipeline"
BQ_DATASET = "nyc_citibike_data"
BQ_TABLENAME = "citibike_tripsdata_raw"
PARTITIONED_TABLE = "citibike_tripsdata"
PARTITION_COL = "started_at"
GCS_BUCKET = "prefect-citibike-bucket"
GCP_CREDS = "citibike-pipeline-credentials"


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Fetches citi bike data from a given URL and returns it as a pandas DataFrame.
    
    Args:
    - dataset_url: The URL to fetch data from.
    
    Returns:
    - DataFrame containing the fetched data.
    """
    resp = requests.get(dataset_url)
    zipfile = ZipFile(BytesIO(resp.content))
    for file_name in zipfile.namelist():
        print(file_name)
        file = zipfile.open(file_name)
        df = pd.read_csv(file)
        return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the DataFrame by setting correct data types for each column.
    
    Args:
    - df: DataFrame to be cleaned.
    
    Returns:
    - Cleaned DataFrame.
    """
    df["ride_id"] = df["ride_id"].astype('str')
    df["rideable_type"] = df["rideable_type"].astype('str')
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])
    df["start_station_name"] = df["start_station_name"].astype('str')
    df["start_station_id"] = df["start_station_id"].astype('str')
    df["end_station_name"] = df["end_station_name"].astype('str')
    df["end_station_id"] = df["end_station_id"].astype('str')
    df["start_lat"] = df["start_lat"].astype('float')
    df["start_lng"] = df["start_lng"].astype('float')
    df["end_lat"] = df["end_lat"].astype('float')
    df["end_lng"] = df["end_lng"].astype('float')
    df["member_casual"] = df["member_casual"].astype('str')
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, year: int, dataset_file: str) -> Path:
    """
    Writes the DataFrame locally as a gzip-compressed parquet file.
    
    Args:
    - df: DataFrame to be written to disk.
    - year: Year of the dataset.
    - dataset_file: Name of the dataset.
    
    Returns:
    - Path where the file was saved.
    """
    path = f"{dataset_file}.parquet"
    df.to_parquet(path, compression='gzip')
    return path


@task(log_prints=True)
def write_to_gcs(file, dest_file) -> None:
    """
    Writes a local file to a Google Cloud Storage (GCS) bucket.
    
    Args:
    - file: Path of the local file to be uploaded.
    - dest_file: Path in the GCS where the file will be saved.
    """
    gcs_block = GcsBucket.load(GCS_BUCKET)
    gcs_block.upload_from_path(from_path=file, to_path=dest_file)
    return


@task(log_prints=True)
def write_gbq(df):
    """
    Writes the DataFrame to a Google BigQuery table.
    
    Args:
    - df: DataFrame to be written.
    """
    # Load GCP credentials
    gcp_credentials_block = GcpCredentials.load(GCP_CREDS)
    credentials = gcp_credentials_block.get_credentials_from_service_account()

    # Write DataFrame to BigQuery
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLENAME}",
        project_id=PROJECT_ID,
        credentials=credentials,
        chunksize=500000,
        if_exists="append"
    )


@task(log_prints=True)
def remove_file(local_path):
    """
    Removes a local file.
    
    Args:
    - local_path: Path of the file to be removed.
    """
    if os.path.isfile(local_path):
        os.remove(local_path)
    else:
        print(f"Error: {local_path} file not found")


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """
    Main ETL function that orchestrates the entire data processing pipeline, from fetching the data
    to writing it to GCS and BigQuery.
    
    Args:
    - year: Year of the dataset.
    - month: Month of the dataset.
    """
    # Solve data name error at 2022-06/07
    if year == '2022' and month in ['06','07']:
        dataset_file = f"{year}{month:02}-citbike-tripdata"
        dest_file = f"{year}{month:02}-citibike-tripdata.parquet"
    else:
        dataset_file = f"{year}{month:02}-citibike-tripdata"
        dest_file = f"{dataset_file}.parquet"

    # Construct the dataset URL and fetch the data
    dataset_url = f"https://s3.amazonaws.com/tripdata/{dataset_file}.csv.zip"
    df = fetch(dataset_url)

    # Clean the data and write it locally
    clean_df = clean(df)
    local_path = write_local(clean_df, year, dataset_file)

    # Write the cleaned data to GCS and BigQuery
    write_to_gcs(local_path, dest_file)
    write_gbq(clean_df)

    # Remove the local file
    remove_file(local_path)


@flow()
def etl_parent_flow(year: int = 2021, months: list[int] = [2, 3]):
    """
    Wrapper flow that runs the main ETL function for a list of months.
    
    Args:
    - year: Year of the datasets.
    - months: List of months for which to run the ETL.
    """
    for month in months:
        etl_web_to_gcs(year, month)


@flow()
def create_BQpartitioned_table():
    """
    Creates a partitioned table in Google BigQuery.
    """
    # Load GCP credentials
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    credentials = gcp_credentials_block.get_credentials_from_service_account()

    # Create a BigQuery client and run the SQL to create the partitioned table
    client = bigquery.Client(credentials=credentials)
    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.{PARTITIONED_TABLE}`
    PARTITION BY DATE({PARTITION_COL}) AS
    SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLENAME}`;
    """
    query_job = client.query(sql)
    print("created the partitioned table")


if __name__ == '__main__':
    # Define the years and months for which to run the ETL
    years = 2022
    months = [1, 2, 3, 4, 5, 5, 7, 8, 9, 10, 11, 12]

    # Run the ETL for the defined years and months
    etl_parent_flow(years, months)

    # Create the partitioned table in BigQuery
    create_BQpartitioned_table()
