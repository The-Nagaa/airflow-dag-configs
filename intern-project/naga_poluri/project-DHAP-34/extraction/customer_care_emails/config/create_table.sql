-- create_table.sql
-- Author: naga_poluri
-- Project: DHAP-34
--
-- Creates the target table for the customer_care_emails pipeline.
-- Using IF NOT EXISTS so its safe to run multiple times.
-- This must match the columns defined in schema_expected.yaml exactly.

CREATE TABLE IF NOT EXISTS public.customer_care_emails (
    id                  INTEGER         NOT NULL,
    email_subject       TEXT            NULL,
    email_body          TEXT            NOT NULL,
    issue_type          TEXT            NULL,
    sentiment_score     FLOAT           NULL,
    sentiment_label     TEXT            NULL,
    resolution_status   TEXT            NULL,
    -- pipeline control flag, rows with status='done' are skipped by the DAG
    status              TEXT            NULL,
    created_at          TIMESTAMP       NULL,
    updated_at          TIMESTAMP       NULL,
    PRIMARY KEY (id)
);

-- adding indexes to speed up common queries on this table
CREATE INDEX IF NOT EXISTS idx_cce_status
    ON public.customer_care_emails (status);

CREATE INDEX IF NOT EXISTS idx_cce_issue_type
    ON public.customer_care_emails (issue_type);

CREATE INDEX IF NOT EXISTS idx_cce_sentiment_label
    ON public.customer_care_emails (sentiment_label);
