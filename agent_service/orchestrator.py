"""
the explicit state machine tying  agents together.
"""
import json
from db import get_connection
from memory import find_similar_incidents, store_incident_memory
from diagnosis_agent import diagnose_failure
from remediation_agent import (
    remediate_null_spike,
    escalate_schema_drift,
    remediate_api_timeout,
)

SAFE_TO_AUTO_FIX = {"null_spike", "api_timeout"}


def handle_incident(dag_id, task_id, run_id, error_message) -> dict:
    conn = get_connection()
    try:
        
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO incidents (dag_id, task_id, run_id, raw_log_excerpt, status)
                VALUES (%s, %s, %s, %s, 'received')
                RETURNING id
                """,
                (dag_id, task_id, run_id, error_message),
            )
            incident_id = cur.fetchone()[0]
        conn.commit()

        
        similar_incidents = find_similar_incidents(error_message)
        diagnosis = diagnose_failure(error_message, similar_incidents=similar_incidents)
       
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE incidents
                SET failure_type = %s, diagnosis_confidence = %s,
                    diagnosis_reasoning = %s, status = 'diagnosed', updated_at = now()
                WHERE id = %s
                """,
                (
                    diagnosis["failure_type"], diagnosis["confidence"],
                    diagnosis["reasoning"], incident_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO audit_log (incident_id, actor, action, details)
                VALUES (%s, 'diagnosis_agent', 'classified_failure', %s)
                """,
                (incident_id, json.dumps(diagnosis)),
            )
        conn.commit()

        failure_type = diagnosis["failure_type"]

      
        if failure_type == "null_spike":
            result = remediate_null_spike()
            new_status, actor = "auto_fixed", "remediation_agent"
        elif failure_type == "api_timeout":
            result = remediate_api_timeout(dag_id, task_id)
            new_status, actor = "auto_fixed", "remediation_agent"
        elif failure_type == "schema_drift":
            result = escalate_schema_drift(dag_id, task_id, error_message)
            new_status, actor = "escalated", "escalation_agent"
        else:
            result = {"action": "none", "summary": "Unrecognized failure type; escalating by default."}
            new_status, actor = "escalated", "escalation_agent"

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE incidents
                SET status = %s, resolution = %s, updated_at = now()
                WHERE id = %s
                """,
                (new_status, result["summary"], incident_id),
            )
            cur.execute(
                """
                INSERT INTO audit_log (incident_id, actor, action, details)
                VALUES (%s, %s, %s, %s)
                """,
                (incident_id, actor, result["action"], json.dumps(result)),
            )
        conn.commit()
     
        if diagnosis["confidence"] >= 0.5:
            store_incident_memory(
                incident_id=incident_id,
                error_message=error_message,
                failure_type=failure_type,
                resolution=result["summary"],
            )
        else:
            print(f"Skipping memory storage for incident {incident_id} with low confidence diagnosis ({diagnosis['confidence']}).")

    finally:
        conn.close()

      

    return {
        "incident_id": incident_id,
        "diagnosis": diagnosis,
        "remediation": result,
        "status": new_status,
    }