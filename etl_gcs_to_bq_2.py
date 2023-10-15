# we are grabbing data from Google Cloud Storage & putting it into BQ
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def load(path: Path) -> pd.DataFrame:
    """Extract data from the file path in Google Cloud Storage"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    # a lot of ways to get data into BigQuery
    # we are going to use a native write to BQ method in pandas
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="de_zoom_camp.rides",
        project_id="banded-cumulus-398617",
        # https://prefecthq.github.io/prefect-gcp/
        # calling a method to supply the credentials from GCP
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )


@flow(name='etl-gcs-to-bq-flow')
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = load(path)
    write_bq(df)
    
    
    
@flow(name='etl-parent-bq')
def etl_parent_flow(
    months: list[int],
    year: int = 2021,
    color: str = "yellow"):
    for month in months:
        etl_gcs_to_bq(year, month, color)




def bq_function():
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
    
    
if __name__ == "__main__":
    bq_function()   
