from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define a proper Python function
def print_message():
    print("monika")

# Define the DAG
with DAG(
    dag_id="welcome",  # Changed to lowercase
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2030, 1, 5),
    schedule="@daily",
    catchup=False,  # Avoid unnecessary backfills
):
    welcome = BashOperator(
        task_id="welkom_terug",  # Changed task ID (no spaces)
        bash_command="echo 'hello'"
    )
    
    monika = PythonOperator(
        task_id="welcome_monika",  # Changed task ID (no spaces)
        python_callable=print_message  # Referencing a proper function
    )

    welcome >> monika  # Task dependency

