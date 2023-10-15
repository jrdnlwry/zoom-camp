from pathlib import Path
import pandas as pd
from prefect import Flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

# Import the functions from the two Python files
# Import the functions from the two Python files
from etl_gcs_to_bq_2 import bq_function as gcs_to_bq
from etl_web_to_gcs import web_function as web_to_gcs

# Call the functions to execute them
if __name__ == "__main__":
    web_to_gcs()  # Call the web_to_gcs function to fetch and clean data
    gcs_to_bq()    # Call the gcs_to_bq function to load data into BigQuery

