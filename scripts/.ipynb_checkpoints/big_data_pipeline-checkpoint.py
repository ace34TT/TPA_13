from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

def run_java_program():
    subprocess.run(['java', '-jar', '/path/to/your_program.jar'])

def run_r_script():
    subprocess.run(['Rscript', '/path/to/your_ml_script.R'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'big_data_pipeline',
    default_args=default_args,
    description='A big data pipeline',
    schedule_interval='@daily',
)