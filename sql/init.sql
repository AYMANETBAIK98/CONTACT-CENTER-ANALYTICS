-- Runs once, the first time the PostgreSQL container starts (empty volume).
-- One schema per layer of the pipeline.
CREATE SCHEMA IF NOT EXISTS realtime;    -- 15-minute KPIs computed from the Kafka stream (step 2)
CREATE SCHEMA IF NOT EXISTS staging;     -- cleaned models built by dbt (step 4)
CREATE SCHEMA IF NOT EXISTS marts;       -- star schema read by the dashboards (step 4)
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS lakehouse;   -- gold tables copied from the S3 lakehouse by Spark (step 3)  -- pipeline run logs and data-quality results
