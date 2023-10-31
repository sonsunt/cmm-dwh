# `pip install google-cloud-storage`
# `pip install google-cloud-bigquery`
# `pip install pyyaml`
# `pip install requests`

from datetime import datetime
import os
import io
import csv
import json
from pathlib import Path
import requests
from google.cloud import storage
from google.cloud import bigquery


def load_config(config_fp):
    with open(config_fp, "r") as fp:
        config = json.load(fp)
    return config

def get_latest_coin_data(api_header):
    response = requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                            headers=api_header,
                            params={"cryptocurrency_type": "coins"})
    return response.json()

def write_json_to_gcs(source, bucket_name, save_fp, cred):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred  # Google GCS Credential

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(save_fp)

    with blob.open(mode="w", content_type="application/json") as fp:
        fp.write(str(source))

def transform_json_to_list(source):
    pricelist = []
    headers = ["request_time", "id", "name", "symbol", "currency", "price", "market_cap", "price_updated_time"]

    c1 = source["status"]["timestamp"]

    for row in source["data"]:
        c2 = row.get("id", "N/A")
        c3 = row.get("name", "N/A")
        c4 = row.get("symbol", "N/A")
        c5 = "USD" if row.get("quote") else "N/A"
        if c5:
            c6 = row["quote"]["USD"].get("price", "N/A")
            c7 = row["quote"]["USD"].get("market_cap", "N/A")
            c8 = row["quote"]["USD"].get("last_updated", "N/A")
        else:
            c6 = "N/A"
            c7 = "N/A"
            c8 = "N/A"
        pricelist.append([c1, c2, c3, c4, c5, c6, c7, c8])

    return headers, pricelist

def write_csv_to_gcs(header, pricelist, bucket_name, save_fp, cred):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(save_fp)

    with blob.open(mode="w", content_type="text/csv") as fp:
        s = io.StringIO()
        csv.writer(s).writerow(header)
        csv.writer(s).writerows(pricelist)
        fp.write(s.getvalue())

def load_to_bigquery(csv_uri, table_id, cred):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred
    client = bigquery.Client()

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
        ],
        skip_leading_rows=1,
    )

    load_job = client.load_table_from_uri(
        csv_uri, table_id, job_config=job_config
    ) 

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {destination_table.num_rows} rows.")

def main():
    cfg = load_config("config.json")
    bucket_name = "de-general-bucket-1"
    table_id = "de-zoomcamp-400010.coin_price_data.latest_prices"
    file_name = f"crypto-prices-{datetime.now():%Y-%m-%d-%H%M%S}"
    json_fp = f"lake-coin/data/json/{file_name}.json"
    csv_fp = f"lake-coin/data/csv/{file_name}.csv"
    csv_full_fp = f"gs://{bucket_name}/{csv_fp}"

    latest_coin_data = get_latest_coin_data(cfg["api_header"])
    write_json_to_gcs(latest_coin_data, bucket_name, json_fp, cfg["gcloud"]["gcs_json_credential"])
    header, coin_current_data = transform_json_to_list(latest_coin_data)
    write_csv_to_gcs(header, coin_current_data, bucket_name, csv_fp, cfg["gcloud"]["gcs_json_credential"])
    load_to_bigquery(csv_full_fp, table_id, cfg["gcloud"]["bq_json_credential"])

if __name__ == "__main__":
    main()

