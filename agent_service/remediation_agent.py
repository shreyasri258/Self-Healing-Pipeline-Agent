from db import get_connection


def remediate_null_spike() -> dict:
    """Scans raw_sales for rows not yet in clean_sales. Rows with a missing
    customer_email get quarantined into clean_sales_dead_letter instead of
    blocking the whole batch; valid rows get inserted into clean_sales."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, order_id, customer_email, amount, order_date
                FROM raw_sales
                WHERE order_id NOT IN (SELECT order_id FROM clean_sales)
                """
            )
            rows = cur.fetchall()

            quarantined, inserted = 0, 0
            for row_id, order_id, email, amount, order_date in rows:
                if not email or not email.strip():
                    cur.execute(
                        """
                        INSERT INTO clean_sales_dead_letter (raw_row, reason)
                        VALUES (%s, %s)
                        """,
                        (
                            f'{{"order_id": "{order_id}", "raw_sales_id": {row_id}}}',
                            "missing customer_email",
                        ),
                    )
                    quarantined += 1
                else:
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

    return {
        "action": "quarantine_null_rows",
        "rows_quarantined": quarantined,
        "rows_inserted": inserted,
        "summary": f"Quarantined {quarantined} row(s) with missing email, inserted {inserted} valid row(s).",
    }


def escalate_schema_drift(dag_id: str, task_id: str, error_message: str) -> dict:
    """schema_drift is NOT auto-fixed, guessing a new column mapping could
    silently corrupt data. We log a clear escalation instead. This is
    stubbed to print/log for now; swap in a real Slack/email call later
    without changing anything upstream of this function."""
    message = (
        f"ESCALATION: schema_drift detected in {dag_id}.{task_id}. "
        f"A human should review the upstream schema change before any fix "
        f"is applied. Error: {error_message}"
    )
    print(message) 

    return {
        "action": "escalate_to_human",
        "summary": message,
    }


def remediate_api_timeout(dag_id: str, task_id: str) -> dict:
    """A real version would call Astro's Airflow API to retry the
    failed task."""
    message = f"STUB: would retry task '{task_id}' in DAG '{dag_id}' via Astro API."
    print(message)

    return {
        "action": "retry_task (stub, not actually executed)",
        "summary": message,
    }