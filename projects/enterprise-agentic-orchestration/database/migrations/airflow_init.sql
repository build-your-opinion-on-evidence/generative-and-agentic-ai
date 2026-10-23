-- database/migrations/airflow_init.sql
-- Creates the airflow_db database on the shared postgres-orchestrator instance.
-- This runs at container startup via docker-entrypoint-initdb.d.

SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow_db'
)\gexec
