from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

API_URL = "https://lldev.thespacedevs.com/2.2.0/launch/"
LOCAL_STORAGE_PATH = "/tmp/rocket_launches.parquet"
GCS_BUCKET = "your-gcs-bucket"
BQ_DATASET = "rocket_data"
BQ_TABLE = "launches"
POSTGRES_CONN_ID = "postgres_default"

# Function to fetch launch data from API
def fetch_launch_data(**kwargs):
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=60)
    response = requests.get(f"{API_URL}?window_start__gte={start_date}&window_end__gte={today}")
    
    if response.status_code != 200:
        raise Exception("API request failed!")
    
    launches = response.json()["results"]
    if not launches:
        return "NO_LAUNCHES"
    
    df = pd.DataFrame([{ 
        "id": launch["id"],
        "name": launch["name"],
        "mission": launch.get("mission", {}).get("name", "Unknown"),
        "status": launch["status"]["name"],
        "country": launch["pad"]["location"]["country_code"],
        "service_provider": launch["launch_service_provider"]["name"],
        "provider_type": launch["launch_service_provider"]["type"],
        "window_start": launch["window_start"]
    } for launch in launches])
    
    df.to_parquet(LOCAL_STORAGE_PATH, index=False)
    return "LAUNCHES_FOUND"

# Function to check if launches are scheduled today
def check_today_launches(**kwargs):
    ti = kwargs['ti']
    status = ti.xcom_pull(task_ids='fetch_launch_data')
    if status == "NO_LAUNCHES":
        return "stop_pipeline"
    return "continue_pipeline"

# Define DAG
default_args = {"owner": "airflow", "depends_on_past": False, "start_date": datetime(2024, 1, 1), "retries": 1}
dag = DAG("rocket_launch_pipeline", default_args=default_args, schedule_interval="@daily", catchup=False)

fetch_task = PythonOperator(
    task_id="fetch_launch_data",
    python_callable=fetch_launch_data,
    provide_context=True,
    dag=dag
)

check_launch_task = PythonOperator(
    task_id="check_today_launches",
    python_callable=check_today_launches,
    provide_context=True,
    dag=dag
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src=LOCAL_STORAGE_PATH,
    dst="rocket_launches.parquet",
    bucket=GCS_BUCKET,
    dag=dag
)

load_to_bq = BigQueryInsertJobOperator(
    task_id="load_to_bq",
    configuration={
        "query": {
            "query": f"""
