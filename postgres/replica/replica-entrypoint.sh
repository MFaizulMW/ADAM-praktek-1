#!/bin/bash
# Read replica: physical streaming replication (asynchronous, lag < 1 detik).
# Saat volume masih kosong -> clone primary dengan pg_basebackup, lalu start sebagai hot standby.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "[replica] menunggu primary..."
    until pg_isready -h pg-primary -p 5432 -U "$REPLICATION_USER" >/dev/null 2>&1; do sleep 2; done

    echo "[replica] pg_basebackup dari pg-primary"
    export PGPASSWORD="$REPLICATION_PASSWORD"
    # -R  : tulis standby.signal + primary_conninfo (+ primary_slot_name karena -S)
    # -Xs : stream WAL selama backup
    pg_basebackup -h pg-primary -p 5432 -U "$REPLICATION_USER" \
        -D "$PGDATA" -Fp -Xs -P -R -S replica1_slot
    chmod 0700 "$PGDATA"
fi

echo "[replica] start hot standby"
exec postgres -c hot_standby=on -c hot_standby_feedback=on
