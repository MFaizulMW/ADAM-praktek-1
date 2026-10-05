"""
Read/write splitting:
  * primary  -> semua INSERT/UPDATE/DELETE
  * replica  -> semua SELECT OLTP (list, detail, lookup)
Pencarian & dashboard tidak menyentuh Postgres sama sekali (dilayani Elasticsearch / gold).
"""
from contextlib import contextmanager

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import PRIMARY_DSN, REPLICA_DSN

primary_pool = ConnectionPool(PRIMARY_DSN, min_size=1, max_size=10, open=False,
                              kwargs={"row_factory": dict_row})
replica_pool = ConnectionPool(REPLICA_DSN, min_size=1, max_size=20, open=False,
                              kwargs={"row_factory": dict_row})


def open_pools():
    primary_pool.open(wait=True, timeout=60)
    replica_pool.open(wait=True, timeout=60)


def close_pools():
    primary_pool.close()
    replica_pool.close()


@contextmanager
def write_conn():
    with primary_pool.connection() as conn:   # commit otomatis saat keluar blok
        yield conn


@contextmanager
def read_conn():
    with replica_pool.connection() as conn:
        yield conn


def fetch_all(sql: str, params=None):
    with read_conn() as conn:
        return conn.execute(sql, params).fetchall()


def fetch_one(sql: str, params=None):
    with read_conn() as conn:
        return conn.execute(sql, params).fetchone()


def execute_returning(sql: str, params=None):
    with write_conn() as conn:
        return conn.execute(sql, params).fetchone()
