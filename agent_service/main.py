"""the agent service's entry point."""

from fastapi import FastAPI,Header, HTTPException
from pydantic import BaseModel
from typing import Optional
from diagnosis_agent import diagnose_failure
from db import get_connection
from fastapi.responses import HTMLResponse
from orchestrator import handle_incident
from remediation_agent import remediate_null_spike, remediate_api_timeout
import json

import os

app = FastAPI(title="Self-Healing Pipeline Agent")


class AirflowFailurePayload(BaseModel):
    dag_id: str
    task_id: str
    run_id: str
    execution_date: Optional[str] = None
    log_url: Optional[str] = None
    error_message: str
    try_number: Optional[int] = 1


@app.get("/")
def health_check():
    return {"status": "agent service is running"}


@app.get("/stats")
def get_stats():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM incidents")
            total = cur.fetchone()[0]

            cur.execute("SELECT status, count(*) FROM incidents GROUP BY status")
            by_status = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(
                "SELECT failure_type, count(*) FROM incidents WHERE failure_type IS NOT NULL GROUP BY failure_type"
            )
            by_failure_type = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute(
                """
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                FROM incidents WHERE status IN ('auto_fixed', 'escalated')
                """
            )
            mttr_seconds = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "total_incidents": total,
        "by_status": by_status,
        "by_failure_type": by_failure_type,
        "mttr_seconds": round(float(mttr_seconds), 1) if mttr_seconds else None,
    }




@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    with open("dashboard.html") as f:
        return f.read()



@app.post("/webhook/airflow-failure")
def receive_airflow_failure(
    payload: AirflowFailurePayload,
    x_agent_secret: str = Header(None),
):
    expected_secret = os.environ.get("AGENT_WEBHOOK_SECRET")
    if not expected_secret or x_agent_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")

    result = handle_incident(
        dag_id=payload.dag_id,
        task_id=payload.task_id,
        run_id=payload.run_id,
        error_message=payload.error_message,
    )
    return result

@app.post("/incidents/{incident_id}/approve")
def approve_incident(incident_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT dag_id, task_id, failure_type FROM incidents WHERE id = %s", (incident_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Incident not found")
            dag_id, task_id, failure_type = row

        if failure_type == "null_spike":
            result = remediate_null_spike()
        elif failure_type == "api_timeout":
            result = remediate_api_timeout(dag_id, task_id)
        else:
            raise HTTPException(status_code=400, detail=f"No approvable remediation for failure_type={failure_type}")

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE incidents SET status = 'auto_fixed', resolution = %s, updated_at = now()
                WHERE id = %s
                """,
                (result["summary"], incident_id),
            )
            cur.execute(
                """
                INSERT INTO audit_log (incident_id, actor, action, details)
                VALUES (%s, 'human', 'approved_remediation', %s)
                """,
                (incident_id, json.dumps(result)),
            )
        conn.commit()
    finally:
        conn.close()

    return {"incident_id": incident_id, "status": "auto_fixed", "result": result}


@app.post("/incidents/{incident_id}/reject")
def reject_incident(incident_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE incidents SET status = 'rejected', updated_at = now() WHERE id = %s",
                (incident_id,),
            )
            cur.execute(
                """
                INSERT INTO audit_log (incident_id, actor, action, details)
                VALUES (%s, 'human', 'rejected_remediation', '{}')
                """,
                (incident_id,),
            )
        conn.commit()
    finally:
        conn.close()

    return {"incident_id": incident_id, "status": "rejected"}

@app.get("/incidents")
def list_incidents():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, dag_id, task_id, failure_type, status, resolution, created_at FROM incidents ORDER BY id DESC LIMIT 20
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "id": r[0], "dag_id": r[1], "task_id": r[2],
            "failure_type": r[3], "status": r[4], "resolution": r[5], "created_at": str(r[6]),
        }
        for r in rows
    ]