#!/bin/bash
# ============================================================
# init.sh: PostgreSQL roles and permissions initialization
# Runs once on first container startup
# ============================================================
set -e

echo "Creating roles and granting permissions..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- --------------------------------------------------------
    -- Application roles
    -- --------------------------------------------------------

    -- Writer role: ETL scripts and data ingestion
    CREATE ROLE $POSTGRES_WRITER_USER WITH LOGIN
        PASSWORD '$POSTGRES_WRITER_PASSWORD';

    -- Reader role: Metabase (read-only)
    CREATE ROLE $POSTGRES_READER_USER WITH LOGIN
        PASSWORD '$POSTGRES_READER_PASSWORD';

    -- Kestra role: owns kestradb entirely
    CREATE ROLE $KESTRA_POSTGRES_USER WITH LOGIN
        PASSWORD '$KESTRA_POSTGRES_PASSWORD';

    -- Metabase role: connects to sportdb as reader
    CREATE ROLE $METABASE_POSTGRES_USER WITH LOGIN
        PASSWORD '$METABASE_POSTGRES_PASSWORD';

    -- --------------------------------------------------------
    -- sportdb permissions (our app data)
    -- --------------------------------------------------------
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $POSTGRES_WRITER_USER;
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $POSTGRES_READER_USER;
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO $METABASE_POSTGRES_USER;

    GRANT USAGE ON SCHEMA public TO $POSTGRES_WRITER_USER;
    GRANT USAGE ON SCHEMA public TO $POSTGRES_READER_USER;
    GRANT USAGE ON SCHEMA public TO $METABASE_POSTGRES_USER;

    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
        TO $POSTGRES_WRITER_USER;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public
        TO $POSTGRES_WRITER_USER;

    GRANT SELECT ON ALL TABLES IN SCHEMA public TO $POSTGRES_READER_USER;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO $METABASE_POSTGRES_USER;

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $POSTGRES_WRITER_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO $POSTGRES_READER_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO $METABASE_POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT USAGE, SELECT ON SEQUENCES TO $POSTGRES_WRITER_USER;

EOSQL

# --------------------------------------------------------
# Create dedicated kestradb — must run as separate command
# --------------------------------------------------------
echo "Creating kestradb for Kestra..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL

    CREATE DATABASE kestradb OWNER $KESTRA_POSTGRES_USER;

EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "kestradb" <<-EOSQL

    GRANT ALL PRIVILEGES ON SCHEMA public TO $KESTRA_POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON TABLES TO $KESTRA_POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON SEQUENCES TO $KESTRA_POSTGRES_USER;

EOSQL

# --------------------------------------------------------
# Create dedicated metabasedb — must run as separate command
# --------------------------------------------------------

echo "Creating metabasedb for Metabase..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE metabasedb OWNER $METABASE_POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "metabasedb" <<-EOSQL
    GRANT ALL PRIVILEGES ON SCHEMA public TO $METABASE_POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON TABLES TO $METABASE_POSTGRES_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON SEQUENCES TO $METABASE_POSTGRES_USER;
EOSQL

echo "All databases created successfully."
