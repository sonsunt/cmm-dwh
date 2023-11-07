from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
from prefect import flow

def run_dbt(job_id):
    dbt_cloud_credentials = DbtCloudCredentials.load("de-cmc")
    trigger_dbt_cloud_job_run(dbt_cloud_credentials=dbt_cloud_credentials, job_id=job_id)

@flow(name="dbt Transformation Subflow")
def main():
    job_id = ...
    run_dbt(job_id)

if __name__ == "__main__":
    main()
