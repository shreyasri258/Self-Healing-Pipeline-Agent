
from unittest.mock import patch, MagicMock

import orchestrator


def _mock_conn_returning_incident_id(incident_id=1):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (incident_id,)
    return mock_conn


def test_null_spike_goes_to_pending_approval_not_auto_fixed():
    mock_conn = _mock_conn_returning_incident_id()
    fake_diagnosis = {"failure_type": "null_spike", "confidence": 0.95, "reasoning": "NOT NULL violation."}

    with patch.object(orchestrator, "get_connection", return_value=mock_conn), \
         patch.object(orchestrator, "diagnose_failure", return_value=fake_diagnosis), \
         patch.object(orchestrator, "find_similar_incidents", return_value=[]), \
         patch.object(orchestrator, "store_incident_memory"):
        result = orchestrator.handle_incident(
            dag_id="sales_etl_dag", task_id="transform", run_id="test-run",
            error_message="null value violates not-null constraint",
        )

    # Human-in-the-loop: even a "safe" failure type should NOT execute
    # automatically anymore — it should wait for explicit approval.
    assert result["status"] == "pending_approval"


def test_schema_drift_escalates_and_notifies():
    mock_conn = _mock_conn_returning_incident_id()
    fake_diagnosis = {"failure_type": "schema_drift", "confidence": 0.9, "reasoning": "Missing field."}

    with patch.object(orchestrator, "get_connection", return_value=mock_conn), \
         patch.object(orchestrator, "diagnose_failure", return_value=fake_diagnosis), \
         patch.object(orchestrator, "find_similar_incidents", return_value=[]), \
         patch.object(orchestrator, "store_incident_memory"), \
         patch.object(orchestrator, "escalate_schema_drift", return_value={"action": "escalate_to_human", "summary": "escalated"}) as mock_escalate:
        result = orchestrator.handle_incident(
            dag_id="sales_etl_dag", task_id="transform", run_id="test-run",
            error_message="KeyError: 'customer_email'",
        )

    assert result["status"] == "escalated"
    mock_escalate.assert_called_once()


def test_low_confidence_diagnosis_is_not_stored_in_memory():
    mock_conn = _mock_conn_returning_incident_id()
    fake_diagnosis = {"failure_type": "unknown", "confidence": 0.0, "reasoning": "Diagnosis agent unavailable."}

    with patch.object(orchestrator, "get_connection", return_value=mock_conn), \
         patch.object(orchestrator, "diagnose_failure", return_value=fake_diagnosis), \
         patch.object(orchestrator, "find_similar_incidents", return_value=[]), \
         patch.object(orchestrator, "store_incident_memory") as mock_store:
        orchestrator.handle_incident(
            dag_id="sales_etl_dag", task_id="transform", run_id="test-run",
            error_message="some transient issue",
        )

    # This is the regression test for the memory-poisoning bug we hit
    # earlier — a low-confidence/fallback diagnosis must NEVER be written
    # into long-term memory.
    mock_store.assert_not_called()