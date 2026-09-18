CREATE TABLE IF NOT EXISTS raw_sales (
    id              SERIAL PRIMARY KEY,
    order_id        TEXT,
    customer_email  TEXT,
    amount          TEXT,
    order_date      TEXT,
    loaded_at       TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clean_sales (
    id              SERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL,
    customer_email  TEXT NOT NULL,
    amount          NUMERIC NOT NULL,
    order_date      DATE NOT NULL,
    processed_at    TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clean_sales_dead_letter (
    id              SERIAL PRIMARY KEY,
    raw_row         JSONB NOT NULL,
    reason          TEXT NOT NULL,
    quarantined_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incidents (
    id                  SERIAL PRIMARY KEY,
    dag_id              TEXT NOT NULL,
    task_id             TEXT NOT NULL,
    run_id              TEXT,
    raw_log_excerpt     TEXT,
    failure_type        TEXT,
    diagnosis_confidence NUMERIC,
    diagnosis_reasoning TEXT,
    status              TEXT NOT NULL DEFAULT 'received',
    resolution          TEXT,
    created_at          TIMESTAMP DEFAULT now(),
    updated_at          TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id           SERIAL PRIMARY KEY,
    incident_id  INTEGER REFERENCES incidents(id),
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    details      JSONB,
    created_at   TIMESTAMP DEFAULT now()
);