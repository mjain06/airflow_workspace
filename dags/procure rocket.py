from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_rocket",
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
):
    task1 = EmptyOperator(task_id="procure_rocket_material")
    task2 = EmptyOperator(task_id="procure_fail")
    task3 = EmptyOperator(task_id="build_stage1")
    task4 = EmptyOperator(task_id="build_stage2")
    task5 = EmptyOperator(task_id="build_stage3")
    task6 = EmptyOperator(task_id="launch")
    

    [task1 , task2] >> [task3 , task4, task5] >> task6
