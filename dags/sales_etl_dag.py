"""
Two Step pipeline
  extract_raw_sales - generates/loads rows into raw_sales
  transform_to_clean  - validates and casts them into clean_sales
"""
from datetime import datetime, timedelta
import random
import os
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2


AGENT_SERVICE_URL_ENV = os.environ.get("AGENT_SERVICE_URL", "http://localhost:8000/webhook/airflow-failure")


def notify_agent_of_failure(context):
    task_instance = context["task_instance"]
    exception = context.get("exception")

    payload = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": context["run_id"],
        "execution_date": str(context.get("execution_date")),
        "log_url": task_instance.log_url,
        "error_message": str(exception) if exception else "Unknown error",
        "try_number": task_instance.try_number,
    }

    try:
        response = requests.post(AGENT_SERVICE_URL_ENV,
            json=payload,
            headers={"X-Agent-Secret": os.environ.get("AGENT_WEBHOOK_SECRET", "")},
            timeout=5,)
        response.raise_for_status()
        logging.info("Notified agent service of failure: %s", payload)
    except Exception as e:
        logging.warning("Could not notify agent service (is it running yet?): %s", e)


def _get_conn():
    conn = BaseHook.get_connection("warehouse_db")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def extract_raw_sales(**context):
    """Pulling a batch of new orders from an upstream source."""
    rows = []
    for i in range(20):
        rows.append(
            (
                f"ORD-{random.randint(10000, 99999)}",
                f"customer{i}@example.com",
                str(round(random.uniform(10, 500), 2)),
                datetime.now().strftime("%Y-%m-%d"),
            )
        )

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw_sales (order_id, customer_email, amount, order_date) VALUES (%s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    finally:
        conn.close()

    print(f"Extracted {len(rows)} raw sales rows.")


def transform_to_clean(**context):
    """Validates and casts raw_sales rows that haven't been processed yet."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, order_id, customer_email, amount, order_date
                FROM raw_sales
                WHERE id NOT IN (
                    SELECT COALESCE((processed_at IS NOT NULL)::int, 0) FROM clean_sales LIMIT 0)
                ORDER BY id DESC LIMIT 20
                """
            )
            rows = cur.fetchall()

            inserted = 0
            for _id, order_id, email, amount, order_date in rows:
                # schema/data-quality assumptions enforcement.
                cur.execute(
                    """
                    INSERT INTO clean_sales (order_id, customer_email, amount, order_date)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (order_id, email, float(amount), order_date),
                )
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    print(f"Transformed {inserted} rows into clean_sales.")


default_args = {
    "owner": "you",
    "retries": 0,  #failures to surface immediatly
    "on_failure_callback": notify_agent_of_failure,
}

with DAG(
    dag_id="sales_etl_dag",
    description="Simple sales ETL to extract raw orders, transform into clean_sales",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "sales"],
) as dag:

    extract = PythonOperator( task_id="extract_raw_sales",python_callable=extract_raw_sales,)
    transform = PythonOperator(task_id="transform_to_clean",python_callable=transform_to_clean,)
    extract >> transform
