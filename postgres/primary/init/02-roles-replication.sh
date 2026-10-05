#!/bin/bash
# Role untuk streaming replication (read replica) & CDC (Debezium)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Physical streaming replication -> pg-replica
    CREATE ROLE ${REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
    SELECT pg_create_physical_replication_slot('replica1_slot');

    -- Logical decoding -> Debezium (least privilege: REPLICATION + SELECT)
    CREATE ROLE ${DEBEZIUM_USER} WITH REPLICATION LOGIN PASSWORD '${DEBEZIUM_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${DEBEZIUM_USER};
    GRANT USAGE ON SCHEMA public TO ${DEBEZIUM_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${DEBEZIUM_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${DEBEZIUM_USER};
EOSQL

# Izinkan koneksi replikasi dari jaringan docker
echo "host replication ${REPLICATION_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
echo "host replication ${DEBEZIUM_USER} all scram-sha-256"   >> "$PGDATA/pg_hba.conf"
