from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'task_2_5',
    default_args=default_args,
    description='DAG for running tasks 1-6 in parallel',
    schedule_interval=None,
)

task1 = BashOperator(
    task_id='task_1',
    bash_command='spark-submit <path_to_task_1>',
    dag=dag,
)

task2 = BashOperator(
    task_id='task_2',
    bash_command='spark-submit <path_to_task_2>',
    dag=dag,
)

task3 = BashOperator(
    task_id='task_3',
    bash_command='spark-submit <path_to_task_3>',
    dag=dag,
)

task4 = BashOperator(
    task_id='task_4',
    bash_command='spark-submit <path_to_task_4>',
    dag=dag,
)

task5 = BashOperator(
    task_id='task_5',
    bash_command='spark-submit <path_to_task_5>',
    dag=dag,
)

task6 = BashOperator(
    task_id='task_6',
    bash_command='spark-submit <path_to_task_6>',
    dag=dag,
)

task1 >> [task2, task3]
[task2, task3] >> [task4, task5, task6]
