
from unittest.mock import patch, MagicMock

import remediation_agent


def _mock_conn_with_rows(rows):
    """Builds a fake db connection whose cursor's fetchall() returns the
    given rows, matching how our code uses `with conn.cursor() as cur:`."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = rows
    return mock_conn, mock_cursor


def test_remediate_null_spike_quarantines_bad_rows_only():
    rows = [
        (1, "ORD-BAD", None, "150.00", "2026-01-01"),      # missing email -> quarantine
        (2, "ORD-GOOD", "a@b.com", "50.00", "2026-01-01"),  # valid -> insert
    ]
    mock_conn, mock_cursor = _mock_conn_with_rows(rows)

    with patch.object(remediation_agent, "get_connection", return_value=mock_conn):
        result = remediation_agent.remediate_null_spike()

    assert result["rows_quarantined"] == 1
    assert result["rows_inserted"] == 1

    # Confirm the RIGHT kind of INSERT happened for each row, not just that
    # *some* INSERT happened — checks the SQL text of each execute() call.
    executed_sql = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert any("clean_sales_dead_letter" in sql for sql in executed_sql)
    assert any("INSERT INTO clean_sales " in sql for sql in executed_sql)


def test_remediate_null_spike_handles_no_bad_rows():
    rows = [(1, "ORD-GOOD", "a@b.com", "50.00", "2026-01-01")]
    mock_conn, _ = _mock_conn_with_rows(rows)

    with patch.object(remediation_agent, "get_connection", return_value=mock_conn):
        result = remediation_agent.remediate_null_spike()

    assert result["rows_quarantined"] == 0
    assert result["rows_inserted"] == 1


def test_escalate_schema_drift_sends_slack_message():
    with patch.object(remediation_agent, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/fake"), \
         patch.object(remediation_agent, "requests") as mock_requests:
        result = remediation_agent.escalate_schema_drift(
            "sales_etl_dag", "transform_to_clean", "KeyError: 'customer_email'"
        )

    assert result["action"] == "escalate_to_human"
    mock_requests.post.assert_called_once()
    sent_payload = mock_requests.post.call_args.kwargs["json"]
    assert "KeyError" in sent_payload["text"]


def test_send_slack_message_noop_when_not_configured():
    with patch.object(remediation_agent, "SLACK_WEBHOOK_URL", None), \
         patch.object(remediation_agent, "requests") as mock_requests:
        remediation_agent.send_slack_message("test message")

    mock_requests.post.assert_not_called()