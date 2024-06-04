from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


def create_bash_task(task_id, bash_command, dag, task_group=None):
    if task_group:
        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
            task_group=task_group,
        )
    else:
        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 30),  # Set to a past date for testing
    "retries": 1,
}

dag = DAG(
    "tpa_13_map_reduce",
    default_args=default_args,
    description="A big data pipeline",
    schedule_interval=None,  # Ensure the DAG is only run manually
    catchup=False,  # Avoid backfilling
)


map_reduce_task = create_bash_task(
    "map_reduce_task",
    "sh /vagrant/tpa_13/scripts/1_data_source/map_reduce/map_reduce_catalog_co2.sh ",
    dag,
)
