from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


API_URL = "https://lldev.thespacedevs.com/2.2.0/launch/"


# def fetch_launch_data(**kwargs):
#     today = datetime.utcnow().date()
#     start_date = today - timedelta(days=60)
#     response = requests.get(f"{API_URL}?window_start__gte={start_date}&window_end__gte={today}")
    
#     if response.status_code != 200:
#         raise Exception("API request failed!")
    
#     launches = response.json()["results"]
#     if not launches:
#         return "NO_LAUNCHES"
      
# # Function to check if there are launches today
# def check_today_launch(**kwargs):
#     currentdatetime = datetime.utcnow().date()
#     response = requests.get(API_URL, params={"window_start": currentdatetime.isoformat()})
#     if response.status_code == 200 and response.json()["count"] > 0:
#         return "fetch_data_task"
#     return "skip_task"
  
with DAG(
    dag_id="rocket_launching",
    start_date=datetime(year=2025, month=3, day=3),
    schedule="@daily",
):
    
    check_api = HttpSensor(
    task_id="check_api_availability",
    http_conn_id="thespacedevs_api",
    endpoint="",
    method="GET",
    response_check=lambda response: response.status_code == 200,
    poke_interval=60, 
    timeout=600, 
)
    get_launches = SimpleHttpOperator(
    task_id="fecth_data",
    http_conn_id="thespacedevs_api",
    endpoint="",
    method="GET",
    data={"window_start": (datetime.utcnow() - timedelta(days=60)).isoformat(), "limit": 100},
    response_filter=lambda response: response.text,
    response_check=lambda response: response.status_code == 200,
    poke_interval=60, 
    timeout=600, 
)
    check_api >> get_launches

#     fetch_data = PythonOperator(
#     task_id="fetch_data_task",
#     python_callable=fetch_launch_data
# )


  
