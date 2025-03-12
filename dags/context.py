from datetime import datetime
import pprint
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define the function to print the execution context
def print_execution_context(**kwargs):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(kwargs)

# Define the DAG and its tasks
with DAG(
    dag_id="context_dag",
    start_date=datetime(year=2019, month=1, day=1),
    end_date=datetime(year=2019, month=1, day=5),
    schedule="@daily",
) as dag:

    # Define the tasks
    print_context = PythonOperator(
        task_id="print_execution_context",
        python_callable=print_execution_context,
        provide_context=True,  
    )

    bash_task = BashOperator(
        task_id="echo_task_message",
        bash_command="echo '[{{ task.task_id }}] is running in the {{ dag.dag_id }} pipeline'",
    )


    print_context >> bash_task
