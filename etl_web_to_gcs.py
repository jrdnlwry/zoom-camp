from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # Create the directory structure if it doesn't exist
    output_directory = Path(f"data/{color}")
    output_directory.mkdir(parents=True, exist_ok=True)

    path = output_directory / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


# SUBFLOW
@flow(name='etl-web-to-gcs-flow')
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    #color = "yellow"
    #year = 2020
    #month = 1
    dataset_file = f"{color}_tripdata_{year}-0{month}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


# this parent flow will trigger the above subflow to trigger x times
@flow(name='etl-parent-web_gcs')
def etl_parent_flow(
    # default months are January, February
    months: list[int], 
    year: int = 2021, 
    color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

# refactor code to be a bit more flexible 
    
def web_function():
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)

if __name__ == "__main__":
    web_function()
    
  
