 from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pprint
 
 print_context = PythonOperator(
        task_id="print_execution_context",
        python_callable=print_execution_context,
        provide_context=True,  
    )

    print_context
    
 
    bash_task = BashOperator(
        task_id="echo_task_message",
        bash_command="echo '[{{ task_instance.task_id }}] is running in the DAG pipeline'",
    )

    print_context >> bash_task