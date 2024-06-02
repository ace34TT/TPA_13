from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 30),
    "retries": 1,
}

dag = DAG(
    "big_data_pipeline",
    default_args=default_args,
    description="A big data pipeline",
    schedule_interval="@daily",
)


def create_bash_task(task_id, bash_command, task_group=None):
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


with dag:
    # Task Group 1: Data Import Tasks
    with TaskGroup(group_id="data_import_group") as data_import_group:
        mongo_import_task = create_bash_task(
            "mongoimport_task",
            "sh /vagrant/TPA_13/scripts/1_data_source/mongo_db.sh ",
            task_group=data_import_group,
        )
        hdfs_import_task = create_bash_task(
            "hdfsimport_task",
            "sh /vagrant/TPA_13/scripts/1_data_source/hdfs/hdfs_import.sh ",
            task_group=data_import_group,
        )
        populate_marketing_oracle_nosql_task = create_bash_task(
            "populate_marketing_oracle_nosql_task",
            "java -jar /vagrant/TPA_13/scripts/1_data_source/oracle_nosql/marketing_to_oracle_nosql.jar ",
            task_group=data_import_group,
        )
    # Task 2: Running map reduce
    map_reduce_task = create_bash_task(
        "map_reduce_task",
        "sh /vagrant/TPA_13/scripts/1_data_source/map_reduce/map_reduce_catalog_co2.sh ",
    )


# Task Group 2: Data Lake Ingestion Task
with TaskGroup(group_id="data_lake_ingestion_task") as data_lake_ingestion_task:
    hive_table_init_task = create_bash_task(
        data_lake_ingestion_task,
        "hive_table_init",
        "python3.9 /vagrant/TPA_13/scripts/2_data_lake/table_init.py ",
    )
    client_elt_task = create_bash_task(
        data_lake_ingestion_task,
        "client_elt_task",
        "python3.9 /vagrant/TPA_13/scripts/2_data_lake/clients_elt.py ",
    )

# Task Group 3: Data Analysis Task (Corrected Task Group Name)
# with TaskGroup(group_id='data_analysis_task') as data_analysis_task:
# Example task added for demonstration; adjust according to actual requirements
# analysis_task = create_bash_task(
#     data_analysis_task,
#     "analysis_task",
#     "echo 'Performing data analysis'",
# )

# Task 4: Data Lake Injection
# datalake_injection_task = create_bash_task(None, "datalake_injection_task", 'echo "Task 4"')
