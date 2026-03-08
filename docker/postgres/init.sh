#!/bin/bash
# ============================================================
# init.sh: PostgreSQL roles and permissions initialization
# Runs once on first container startup
# Environment variables injected by Docker at runtime
# ============================================================

set -e

echo "Creating roles and granting permissions..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- Writer role: used by Kestra and ETL scripts
    CREATE ROLE $POSTGRES_WRITER_USER WITH LOGIN
        PASSWORD '$POSTGRES_WRITER_PASSWORD';

    -- Reader role: used by Metabase (read-only)
    CREATE ROLE $POSTGRES_READER_USER WITH LOGIN
        PASSWORD '$POSTGRES_READER_PASSWORD';

    -- Kestra role: used by Kestra to store its own metadata
    CREATE ROLE $KESTRA_POSTGRES_USER WITH LOGIN
        PASSWORD '$KESTRA_POSTGRES_PASSWORD';

    -- Metabase role: manages its own schema for dashboard definition
    CREATE ROLE $METABASE_POSTGRES_USER WITH LOGIN
        PASSWORD '$METABASE_POSTGRES_PASSWORD';

    -- Grant connection rights
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $POSTGRES_WRITER_USER;
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $POSTGRES_READER_USER;
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $KESTRA_POSTGRES_USER;
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $METABASE_POSTGRES_USER;

    -- Schema usage rights
    GRANT USAGE ON SCHEMA public TO $POSTGRES_WRITER_USER;
    GRANT USAGE ON SCHEMA public TO $POSTGRES_READER_USER;

    -- Writer permissions
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO $POSTGRES_WRITER_USER;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_WRITER_USER;

    -- Reader permissions
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO $POSTGRES_READER_USER;

    -- Grant ability to create schemas inside our existing database
    GRANT CREATE ON DATABASE $POSTGRES_DB TO $KESTRA_POSTGRES_USER;
    GRANT CREATE ON DATABASE $POSTGRES_DB TO $METABASE_POSTGRES_USER;

    -- Future tables inherit same permissions automatically
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $POSTGRES_WRITER_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO $POSTGRES_READER_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT USAGE, SELECT ON SEQUENCES TO $POSTGRES_WRITER_USER;

EOSQL

echo "Roles and permissions created successfully."