
import json
from unittest.mock import patch, MagicMock

import diagnosis_agent


def _fake_response(payload: dict):
    """Builds a fake object shaped like what generate_content() returns —
    just needs a .text attribute holding a JSON string, since that's all
    diagnose_failure() actually reads from the response."""
    mock_response = MagicMock()
    mock_response.text = json.dumps(payload)
    return mock_response


def test_diagnose_failure_parses_response_correctly():
    fake_result = {
        "failure_type": "schema_drift",
        "confidence": 0.95,
        "reasoning": "Missing field indicates upstream schema change.",
    }
    with patch.object(diagnosis_agent.client.models, "generate_content", return_value=_fake_response(fake_result)):
        result = diagnosis_agent.diagnose_failure("KeyError: 'customer_email'")

    assert result["failure_type"] == "schema_drift"
    assert result["confidence"] == 0.95
    assert "schema change" in result["reasoning"]


def test_diagnose_failure_includes_memory_context_in_prompt():
    similar = [{
        "error_message": "KeyError: 'customer_email'",
        "failure_type": "schema_drift",
        "resolution": "Escalated to human.",
    }]
    fake_result = {"failure_type": "schema_drift", "confidence": 1.0, "reasoning": "Matches past incident."}

    with patch.object(diagnosis_agent.client.models, "generate_content", return_value=_fake_response(fake_result)) as mock_call:
        diagnosis_agent.diagnose_failure("KeyError: 'customer_email'", similar_incidents=similar)

    # Confirm the prompt we actually SENT to Gemini included the memory context,
    # not just that we got a response back — this is what proves the memory
    # wiring works, independent of what the (mocked) model says.
    sent_prompt = mock_call.call_args.kwargs["contents"]
    assert "similar past incidents" in sent_prompt
    assert "Escalated to human" in sent_prompt


def test_diagnose_failure_retries_then_succeeds():
    """Simulates: first call fails transiently, second call succeeds.
    We don't need Gemini's real ServerError class here — the `except`
    clause in diagnose_failure resolves genai_errors.ServerError at
    runtime, so patching that attribute to a plain test exception works
    just as well and avoids depending on that class's real constructor."""

    class FakeTransientError(Exception):
        pass

    fake_result = {"failure_type": "null_spike", "confidence": 0.9, "reasoning": "NOT NULL violation."}

    with patch.object(diagnosis_agent.genai_errors, "ServerError", FakeTransientError), \
         patch.object(diagnosis_agent, "time") as mock_time, \
         patch.object(
             diagnosis_agent.client.models,
             "generate_content",
             side_effect=[FakeTransientError("overloaded"), _fake_response(fake_result)],
         ):
        result = diagnosis_agent.diagnose_failure("null value in column violates not-null constraint")

    mock_time.sleep.assert_called_once()  # confirms it actually backed off before retrying
    assert result["failure_type"] == "null_spike"


def test_diagnose_failure_falls_back_after_exhausting_retries():
    class FakeTransientError(Exception):
        pass

    with patch.object(diagnosis_agent.genai_errors, "ServerError", FakeTransientError), \
         patch.object(diagnosis_agent, "time"), \
         patch.object(diagnosis_agent.client.models, "generate_content", side_effect=FakeTransientError("still overloaded")):
        result = diagnosis_agent.diagnose_failure("some error", max_retries=3)

    # After exhausting retries, diagnose_failure should return a SAFE
    # fallback (unknown, confidence 0.0) rather than raising and crashing
    # the caller — this is the behavior that later feeds our
    # confidence >= 0.5 memory-gating check in the orchestrator.
    assert result["failure_type"] == "unknown"
    assert result["confidence"] == 0.0