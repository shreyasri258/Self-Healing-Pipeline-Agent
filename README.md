# Self-Healing-Pipeline-Agent

Agentic pipeline monitoring system that diagnoses Airflow task failures with an LLM, retrieves similar past incidents via RAG memory, and auto-remediates or escalates to Slack with human-in-the-loop approval.

## Features

- Real-time failure ingestion via authenticated Airflow webhook.
- LLM-based failure diagnosis (schema drift, null spikes, API timeouts) with retry/backoff resilience.
- Vector memory (RAG) that retrieves similar past incidents to improve diagnosis confidence over time.
- Human-in-the-loop approval workflow for automated remediations.
- Automated remediation playbooks (row quarantine, task retry) and Slack escalation for unsafe fixes.
- Full audit trail of every agent decision.
- Live dashboard with incident stats, MTTR, and approve/reject controls.
- pytest suite with all external calls (LLM, DB, Slack) mocked.


## API Reference

#### Receive a pipeline failure event

```http
  POST /webhook/airflow-failure
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| `X-Agent-Secret` | `header` | **Required**. Shared secret for webhook authentication |
| `dag_id` | `string` | **Required**. Airflow DAG that failed |
| `task_id` | `string` | **Required**. Airflow task that failed |
| `run_id` | `string` | **Required**. Airflow run ID |
| `error_message` | `string` | **Required**. Raw error/traceback text |
| `try_number` | `integer` | Task retry attempt number |

#### Get recent incidents

```http
  GET /incidents
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| — | — | Returns the 20 most recent incidents (id, dag_id, task_id, failure_type, status, resolution, created_at) |

#### Get aggregate stats

```http
  GET /stats
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| — | — | Returns total incidents, counts by status, counts by failure_type, and mean time to resolution (seconds) |

#### Approve a pending remediation

```http
  POST /incidents/${incident_id}/approve
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| `incident_id` | `integer` | **Required**. Id of the incident to approve |

#### Reject a pending remediation

```http
  POST /incidents/${incident_id}/reject
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| `incident_id` | `integer` | **Required**. Id of the incident to reject |

#### View live dashboard

```http
  GET /dashboard
```

| Parameter | Type | Description |
| :-------- | :------- | :------------------------- |
| — | — | Renders the HTML dashboard (stats + incident table + approve/reject controls) |

#### diagnose_failure(error_message, similar_incidents=None)

Classifies a raw error message into `schema_drift`, `null_spike`, `api_timeout`, or `unknown`, returning a confidence score and reasoning. Retries with exponential backoff on transient LLM errors.

#### remediate_null_spike()

Scans unprocessed rows in `raw_sales`, quarantines rows with missing `customer_email` into `clean_sales_dead_letter`, and inserts valid rows into `clean_sales`.

#### escalate_schema_drift(dag_id, task_id, error_message)

Sends a Slack notification flagging a schema change for human review. Never auto-applies a fix.## Environment Variables

To run this project, you will need to add the following environment variables to your `.env` file (inside `agent_service/`)

`DATABASE_URL`

`GEMINI_API_KEY`

`GEMINI_MODEL`

`AGENT_WEBHOOK_SECRET`

`SLACK_WEBHOOK_URL`## Installation

Clone the repo and set up the agent service

```bash
  git clone https://github.com/shreyasri258/Self-Healing-Pipeline-Agent.git
  cd Self-Healing-Pipeline-Agent/agent_service
  python -m venv venv
  venv\Scripts\activate
  pip install -r requirements.txt
```

Run the schema against your Postgres (Neon) database

```bash
  python -c "import psycopg2; conn = psycopg2.connect('YOUR_DATABASE_URL'); conn.cursor().execute(open('../db/init.sql').read()); conn.commit()"
```

Start the agent service

```bash
  uvicorn main:app --reload --port 8000
```
## Running Tests

To run tests, run the following command

```bash

  cd agent_service
  pytest -v

```

