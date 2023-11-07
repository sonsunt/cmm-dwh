# `pip install google-cloud-storage`
# `pip install google-cloud-bigquery`
# `pip install requests`
# `pip install prefect`
# `pip install prefect-gcp`

from datetime import datetime
import json

from prefect import flow, task
from prefect_gcp import GcpCredentials
from bs4 import BeautifulSoup
from google.cloud import bigquery
import requests

def get_bq_cred():
    
    gcp_credentials_block = GcpCredentials.load("gcp-bq-cred")
    return gcp_credentials_block

@task
def get_config(config_filepath):
    with open(config_filepath, "r") as fp:
        config = json.load(fp)
    return config

@task(retries=3)
def get_holiday_json():
    year = f"{datetime.now():%Y}"
    months = {
        "มกราคม": "1",
        "กุมภาพันธ์": "2",
        "มีนาคม": "3",
        "เมษายน": "4",
        "พฤษภาคม": "5",
        "มิถุนายน": "6",
        "กรกฎาคม": "7",
        "สิงหาคม": "8",
        "กันยายน": "9",
        "ตุลาคม": "10",
        "พฤศจิกายน": "11",
        "ธันวาคม": "12"
    }

    # Scraping holidays from a web page
    soup = BeautifulSoup(requests.get("https://www.infoquest.co.th/holidays").text, "html.parser")

    # Extracting the table to get a day and month, then formatting them in YYYY-MM-DD 
    # Then extract the holiday description, combine day and description in json format
    holiday_table = soup.find("tbody").find_all("tr")
    holidays = []
    for holiday in holiday_table:
        holiday_detail = holiday.find_all("td")
        day = holiday_detail[1].text.replace(u"\xa0", " ")
        for thai_month in months:
            if thai_month in day:
                day = day.replace(thai_month, months[thai_month]).replace(" ", "-") + f"-{year}"
                day = datetime.strptime(day, "%d-%m-%Y").strftime("%Y-%m-%d")
                break
        desc = holiday_detail[2].text.replace(u"\xa0", " ")
        holidays.append({"holiday": day, "description": desc})
    print("Retrieved Holidays ...")
    return holidays

@task(retries=3)
def write_holiday_json_to_bq(src, dest_table_id, cred):
    # Get credential from prefect block
    credentials = cred.get_credentials_from_service_account()
    client = bigquery.Client(credentials=credentials)

    # Set configuration for the load job (full load)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("holiday", "DATE"),
            bigquery.SchemaField("description", "STRING")
        ],
        write_disposition="WRITE_TRUNCATE"
        )
    
    # Load json to bigquery table with above config
    load_job = client.load_table_from_json(
        src, dest_table_id, job_config=job_config
    ) 

    # Wait for the job to complete
    load_job.result()
    print("Loaded Successfully!")
    return True

@flow(name="Thailand public holidays Subflow", log_prints=True)
def main():
    table_id = "de-zoomcamp-400010.coin_price_data.holidays"
    bq_cred = get_bq_cred()
    holidays = get_holiday_json()
    write_holiday_json_to_bq(holidays, table_id, bq_cred)

if __name__ == "__main__":
    main()
