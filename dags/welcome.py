from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="Welcome",
    start_date=datetime(year=2025, month=1, day=1),
    end_date=datetime(year=2030, month=1, day=5),
    schedule="@daily",
):
    welcome = BashOperator(task_id="welkom terug", bash_command="echo 'hello'")
    
    monika = PythonOperator(task_id="welcome monika", python_callable=lambda: print("monika"))

    welcome >> monika