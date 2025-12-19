"""
Airflow DAG for orchestrating ETL -> Iceberg Merge -> ML Training -> Model Registration
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 13),
    'retries': 1
}

dag = DAG(
    'iceberg_mlops_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

etl = BashOperator(
    task_id='run_etl',
    bash_command='python /path/to/etl/etl_iceberg.py',
    dag=dag
)

ml = BashOperator(
    task_id='run_ml',
    bash_command='python /path/to/ml/ml_train.py',
    dag=dag
)

etl >> ml
