"""Observability: status replikasi Postgres & kesegaran layer gold."""
from fastapi import APIRouter

from ..db import primary_pool, replica_pool
from ..es import es

router = APIRouter(prefix="/api/system", tags=["system"])

GOLD_ALIASES = ["kunjungan", "mart_kunjungan_per_poli", "mart_kunjungan_per_faskes",
                "mart_diagnosis_wordcloud", "mart_ringkasan"]


@router.get("/replication")
def replication_status():
    with primary_pool.connection() as conn:
        senders = conn.execute(
            """SELECT application_name, client_addr::text, state, sync_state,
                      pg_wal_lsn_diff(sent_lsn, replay_lsn)::bigint AS lag_bytes,
                      EXTRACT(EPOCH FROM replay_lag)::float AS replay_lag_seconds
               FROM pg_stat_replication"""
        ).fetchall()
        slots = conn.execute(
            """SELECT slot_name, slot_type, active,
                      pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_bytes
               FROM pg_replication_slots"""
        ).fetchall()
    with replica_pool.connection() as conn:
        replica = conn.execute(
            """SELECT pg_is_in_recovery() AS in_recovery,
                      pg_last_wal_replay_lsn()::text AS replay_lsn,
                      pg_last_xact_replay_timestamp() AS last_replay_at"""
        ).fetchone()
    return {"primary_senders": senders, "replication_slots": slots, "replica": replica}


@router.get("/gold")
def gold_status():
    out = []
    for alias in GOLD_ALIASES:
        try:
            idx = list(es.indices.get_alias(name=alias).keys())[0]
            count = es.count(index=alias)["count"]
            out.append({"alias": alias, "index": idx, "docs": count})
        except Exception:
            out.append({"alias": alias, "index": None, "docs": 0})
    return {"items": out}
