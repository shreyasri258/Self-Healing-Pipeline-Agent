"""
Takes a raw error message (and optionally similar past incidents retrieved from memory) and asks Gemini to classify it into one of our known failure
types, with a confidence score and a short explanation.
"""
import os
import json
import time
from google import genai
from google.genai import types
from google.genai import errors as genai_errors
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

DIAGNOSIS_PROMPT = """You are a data pipeline failure diagnosis agent. You will
be given a raw error message/traceback from a failed Airflow task in a sales
ETL pipeline. Classify it into exactly one of these categories:

- schema_drift: an upstream data source changed its structure (e.g. a renamed,
  missing, or unexpected field), typically shows up as a KeyError or similar
  attribute/field-access error.
- null_spike: required data is missing/null in a batch of otherwise valid
  rows, typically shows up as a NOT NULL constraint violation or similar
  database integrity error.
- api_timeout: a transient failure calling an external/upstream service,
  typically shows up as a timeout, connection error, or similar transient
  network issue.
- unknown: doesn't clearly match any of the above.

{memory_context}

Error message:
---
{error_message}
---

Respond with your classification, a confidence score between 0 and 1, and a
one-sentence reasoning for your choice. If similar past incidents were
provided above and they inform your answer, mention that in your reasoning.
"""

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "failure_type": {
            "type": "string",
            "enum": ["schema_drift", "null_spike", "api_timeout", "unknown"],
        },
        "confidence": {"type": "number"},
        "reasoning": {"type": "string"},
    },
    "required": ["failure_type", "confidence", "reasoning"],
}


def diagnose_failure(error_message: str, similar_incidents: list[dict] = None, max_retries: int = 3) -> dict:
    if similar_incidents:
        memory_lines = ["We have seen similar past incidents:"]
        for inc in similar_incidents:
            memory_lines.append(
                f"- Error: \"{inc['error_message']}\" -> classified as "
                f"{inc['failure_type']}, resolved as: {inc['resolution']}"
            )
        memory_context = "\n".join(memory_lines)
    else:
        memory_context = "No similar past incidents found in memory."

    prompt = DIAGNOSIS_PROMPT.format(memory_context=memory_context, error_message=error_message)

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=RESPONSE_SCHEMA,
                ),
            )
            return json.loads(response.text)
        except genai_errors.ServerError as e:
            last_exception = e
            wait_seconds = 2 ** attempt
            print(f"Gemini overloaded (attempt {attempt}/{max_retries}), retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)

    print(f"Diagnosis failed after {max_retries} attempts: {last_exception}")
    return {
        "failure_type": "unknown",
        "confidence": 0.0,
        "reasoning": f"Diagnosis agent unavailable after {max_retries} retries: {last_exception}",
    }


if __name__ == "__main__":
    test_error = "KeyError: 'customer_email'"
    result = diagnose_failure(test_error)
    print(json.dumps(result, indent=2))