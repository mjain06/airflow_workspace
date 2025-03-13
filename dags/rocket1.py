from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.http_sensor import HTTPSensor


API_URL = "https://lldev.thespacedevs.com/2.2.0/launch/"

wait_for_data = FTPSensor(
    task_id="wait_for_data",
    path="foobar.json",
    ftp_conn_id="bob_ftp",

def fetch_launch_data(**kwargs):
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=60)
    response = requests.get(f"{API_URL}?window_start__gte={start_date}&window_end__gte={today}")
    
    if response.status_code != 200:
        raise Exception("API request failed!")
    
    launches = response.json()["results"]
    if not launches:
        return "NO_LAUNCHES"
      
# Function to check if there are launches today
def check_today_launch(**kwargs):
    currentdatetime = datetime.utcnow().date()
    response = requests.get(API_URL, params={"window_start": currentdatetime.isoformat()})
    if response.status_code == 200 and response.json()["count"] > 0:
        return "fetch_data_task"
    return "skip_task"
  
with DAG(
    dag_id="rocket_launching",
    start_date=datetime(year=2025, month=3, day=3) - timedelta(days=10),
    schedule="@daily",
):

  
