-- 00_bootstrap.sql
-- Se ejecuta sobre ecommerce_cl, creada automáticamente por Docker.

SELECT 'CREATE DATABASE superset_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'superset_db'
) \gexec

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ecommerce') THEN
        CREATE ROLE ecommerce WITH LOGIN PASSWORD 'ecommerce123';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pbi_analytical') THEN
        CREATE ROLE pbi_analytical WITH LOGIN PASSWORD 'data_pbi_123';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pbi_executive') THEN
        CREATE ROLE pbi_executive WITH LOGIN PASSWORD 'exec_pbi_123';
    END IF;
END
$$;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";