"""Manually triggered DAG that ingests one CSV processing window per run."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from mastercard_pipeline.ingestion import DEFAULT_WINDOW_SIZE, read_stock_window


with DAG(
    dag_id="mastercard_stock_ingestion",
    description="Read the next window of Mastercard stock records",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mastercard", "ingestion"],
) as dag:
    ingest_stock_data = PythonOperator(
        task_id="ingest_stock_data",
        python_callable=read_stock_window,
        op_kwargs={"window_size": DEFAULT_WINDOW_SIZE},
        retries=0,
        show_return_value_in_logs=False,
    )
