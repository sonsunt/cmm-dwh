# `pip install google-cloud-storage`
# `pip install google-cloud-bigquery`
# `pip install requests`
# `pip install prefect`
# `pip install prefect-gcp`

from datetime import datetime
import io
import csv
import json

from prefect import flow, task
from prefect_gcp import GcpCredentials
from google.cloud import storage
from google.cloud import bigquery
import requests

def get_bq_cred(block_name="gcp-storage-cred"):
    """ Fetch Credential from Prefect Cloud Block """
    gcp_credentials_block = GcpCredentials.load(block_name)
    credentials = gcp_credentials_block.get_credentials_from_service_account()
    return credentials

@task
def load_config(config_filepath):
    """ Get API header from json config file."""
    with open(config_filepath, "r") as fp:
        config = json.load(fp)
    return config

@task(retries=3)
def get_latest_coin_data(api_header: dict, currency: str = "USD"):
    """ Retrieve Latest Price of Cryptocurrency coins in JSON. """
    response = requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                            headers=api_header,
                            params={"cryptocurrency_type": "coins",
                                    "limit": 400,
                                    "convert": currency})
    print("Requested json from API ...")
    return response.json()

@task(retries=3)
def write_json_to_gcs(source, bucket_name, save_fp, cred):
    """ Upload json output from API to Google Cloud Storage. """
    storage_client = storage.Client(credentials=cred)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(save_fp)

    with blob.open(mode="w", content_type="application/json") as fp:
        fp.write(str(source))
    
    print("Uploaded json source to GCS ...")

def transform_json_to_list(source):
    pricelist = []
    headers = ["request_time", "id", "name", "symbol", "currency", "price", "market_cap", "price_updated_time", "coin_updated_time"]

    # Normalize JSON output
    c1 = source["status"]["timestamp"]
    for row in source["data"]:
        c2 = row.get("id", "N/A")
        c3 = row.get("name", "N/A")
        c4 = row.get("symbol", "N/A")
        c5 = "N/A"
        c6 = "N/A"
        c7 = "N/A"
        c8 = "N/A"
        c9 = row.get("last_updated")
        currencies = list(row.get("quote"))
        if currencies is not None:
            for currency in currencies:
                c5 = currency
                c6 = row["quote"][currency].get("price", "N/A")
                c7 = row["quote"][currency].get("market_cap", "N/A")
                c8 = row["quote"][currency].get("last_updated", "N/A")
                pricelist.append([c1, c2, c3, c4, c5, c6, c7, c8, c9])
        else:
            pricelist.append([c1, c2, c3, c4, c5, c6, c7, c8, c9])
    print("Flattened json to CSV ...")
    return headers, pricelist

@task(retries=3)
def write_csv_to_gcs(header, pricelist, bucket_name, save_fp, cred):
    storage_client = storage.Client(credentials=cred)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(save_fp)

    with blob.open(mode="w", content_type="text/csv") as fp:
        s = io.StringIO()
        csv.writer(s).writerow(header)
        csv.writer(s).writerows(pricelist)
        fp.write(s.getvalue())
    
    print("Uploaded CSV to GCS ...")

@task(retries=3)
def load_to_bigquery(csv_uri, table_id, cred):
    client = bigquery.Client(credentials=cred)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("request_time", "TIMESTAMP"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("price", "STRING"),
            bigquery.SchemaField("market_cap", "STRING"),
            bigquery.SchemaField("price_updated_time", "TIMESTAMP"),
            bigquery.SchemaField("coin_updated_time", "TIMESTAMP"),
        ],
        skip_leading_rows=1,
    )

    load_job = client.load_table_from_uri(
        csv_uri, table_id, job_config=job_config
    ) 

    load_job.result()  # Waits for the job to complete.
    print("Loaded Successfully!")
    return True

@flow(name="Cryptocurrency Data Ingestion Subflow", log_prints=True)
def main():
    cfg = load_config("config.json")
    cred = get_bq_cred()
    bucket_name = "de-general-bucket-1"
    table_id = "de-zoomcamp-400010.coin_price_data.latest_prices"
    file_name = f"crypto-prices-{datetime.now():%Y-%m-%d-%H%M%S}"
    json_fp = f"lake-coin/data/json/{file_name}.json"
    csv_fp = f"lake-coin/data/csv/{file_name}.csv"
    csv_full_fp = f"gs://{bucket_name}/{csv_fp}"

    latest_coin_data = get_latest_coin_data(cfg["api_header"])
    write_json_to_gcs(latest_coin_data, bucket_name, json_fp, cred)
    header, coin_current_data = transform_json_to_list(latest_coin_data)
    write_csv_to_gcs(header, coin_current_data, bucket_name, csv_fp, cred)
    load_to_bigquery(csv_full_fp, table_id, cred)

if __name__ == "__main__":
    main()
