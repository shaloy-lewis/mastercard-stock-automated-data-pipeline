"""Manually triggered DAG that cleans, analyzes, and exports one source window."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from mastercard_pipeline.analysis import analyze_stock_window
from mastercard_pipeline.cleaning import read_clean_stock_window
from mastercard_pipeline.export import export_stock_results
from mastercard_pipeline.ingestion import DEFAULT_WINDOW_SIZE


with DAG(
    dag_id="mastercard_stock_ingestion",
    description="Clean and analyze the next stock window, then export its price extrema",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mastercard", "ingestion", "analysis", "export"],
) as dag:
    ingest_stock_data = PythonOperator(
        task_id="ingest_stock_data",
        python_callable=read_clean_stock_window,
        op_kwargs={"window_size": DEFAULT_WINDOW_SIZE},
        retries=0,
        show_return_value_in_logs=False,
    )

    analyze_stock_data = PythonOperator(
        task_id="analyze_stock_data",
        python_callable=analyze_stock_window,
        op_kwargs={"rows": ingest_stock_data.output},
        retries=0,
        show_return_value_in_logs=False,
    )

    save_to_csv = PythonOperator(
        task_id="save_to_csv",
        python_callable=export_stock_results,
        op_kwargs={
            "results": analyze_stock_data.output,
            "rows": ingest_stock_data.output,
        },
        retries=0,
        show_return_value_in_logs=False,
    )

    ingest_stock_data >> analyze_stock_data >> save_to_csv
