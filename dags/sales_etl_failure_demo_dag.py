"""
lets us choose a failure mode at trigger time via an Airflow Param.
"""
from datetime import datetime, timedelta
import os
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
import psycopg2
import random


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
        response = requests.post(AGENT_SERVICE_URL_ENV, json=payload, timeout=5)
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


def extract_raw_sales_demo(**context):
    """Generates rows in-memory and, depending on failure_mode, corrupts them before handing off to the transform step via XCom."""
    failure_mode = context["params"]["failure_mode"]

    if failure_mode == "timeout":
        #Simulates a failed call to an upstream API during extraction.
        raise TimeoutError(
            "Simulated upstream API timeout while extracting sales data "
            "(upstream service did not respond within 30s)."
        )

    rows = []
    for i in range(20):
        row = {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "customer_email": f"customer{i}@example.com",
            "amount": str(round(random.uniform(10, 500), 2)),
            "order_date": datetime.now().strftime("%Y-%m-%d"),
        }

        if failure_mode == "null_spike" and i % 3 == 0:
            #if ~1/3 of rows arrive with a missing required field, as if an upstream integration started allowing blank emails.
            row["customer_email"] = None

        if failure_mode == "schema_drift":
            #Simulates the upstream source renaming a column. The transform step still looks for "customer_email" and won't find it.
            row["email"] = row.pop("customer_email")

        rows.append(row)

    print(f"Extracted {len(rows)} raw sales rows (failure_mode={failure_mode}).")
    return rows  #automatically pushed to XCom


def transform_to_clean_demo(**context):
    """Pulls rows from XCom and inserts them into clean_sales, same validation assumptions as the real pipeline."""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="extract_raw_sales_demo")

    conn = _get_conn()
    try:
        inserted = 0
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO clean_sales (order_id, customer_email, amount, order_date)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        row["order_id"],
                        row["customer_email"],  #KeyErrors here if schema_drift renamed it
                        float(row["amount"]),
                        row["order_date"],
                    ),
                )
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    print(f"Transformed {inserted} rows into clean_sales.")


default_args = {
    "owner": "you",
    "retries": 0,
    "on_failure_callback": notify_agent_of_failure,
}

with DAG(
    dag_id="sales_etl_failure_demo_dag",
    description="Sales ETL with an on-demand failure mode, for generating real incidents",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "sales", "demo"],
    params={
        "failure_mode": Param(
            "none",
            type="string",
            enum=["none", "schema_drift", "null_spike", "timeout"],
            description="Pick a failure to simulate, or 'none' for a clean run.",
        )
    },
) as dag:

    extract = PythonOperator(
        task_id="extract_raw_sales_demo",
        python_callable=extract_raw_sales_demo,
    )

    transform = PythonOperator(
        task_id="transform_to_clean_demo",
        python_callable=transform_to_clean_demo,
    )

    extract >> transform