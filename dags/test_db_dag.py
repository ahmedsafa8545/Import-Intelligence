import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/ingestion")   # makes test_db.py importable
from test_db import check_db

with DAG(
    dag_id="test_db",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,
) as dag:
    PythonOperator(task_id="check_db", python_callable=check_db)