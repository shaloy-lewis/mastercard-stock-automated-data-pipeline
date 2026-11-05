"""Manually triggered DAG that ingests and cleans one source window per run."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from mastercard_pipeline.cleaning import read_clean_stock_window
from mastercard_pipeline.ingestion import DEFAULT_WINDOW_SIZE


with DAG(
    dag_id="mastercard_stock_ingestion",
    description="Read and clean the next window of Mastercard stock records",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mastercard", "ingestion"],
) as dag:
    ingest_stock_data = PythonOperator(
        task_id="ingest_stock_data",
        python_callable=read_clean_stock_window,
        op_kwargs={"window_size": DEFAULT_WINDOW_SIZE},
        retries=0,
        show_return_value_in_logs=False,
    )
