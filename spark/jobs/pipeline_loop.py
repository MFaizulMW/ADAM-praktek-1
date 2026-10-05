"""
Orkestrator micro-batch BRONZE -> SILVER -> GOLD.

Satu SparkSession long-running (hemat waktu start JVM). Setiap interval:
  * cek apakah ada event baru di bronze (jumlah record bronze berubah),
  * jika ya (atau index serving di ES belum ada) -> jalankan silver lalu gold.

Di produksi peran ini biasanya diambil Airflow / Dagster dengan DAG terjadwal.
"""
import os
import time
import traceback

from pyspark.sql.utils import AnalysisException

import gold
import silver
from common import BRONZE_CDC, es_request, get_spark, log

INTERVAL = int(os.getenv("PIPELINE_INTERVAL_SECONDS", "60"))


def bronze_signature(spark):
    try:
        return spark.read.parquet(BRONZE_CDC).count()
    except AnalysisException:
        return None  # bronze belum pernah ditulis


def serving_index_missing() -> bool:
    return es_request("GET", "/_alias/kunjungan", ok_404=True) is None


def main():
    spark = get_spark("bpjs-silver-gold-pipeline")
    last_sig = None
    log(f"pipeline loop started (interval {INTERVAL}s)")

    while True:
        started = time.time()
        try:
            sig = bronze_signature(spark)
            if sig is None:
                log("bronze belum berisi data, menunggu CDC...")
            elif sig != last_sig or serving_index_missing():
                log(f"bronze records={sig} (sebelumnya {last_sig}) -> run silver & gold")
                silver_dfs = silver.run(spark)
                gold.run(spark, silver_dfs)
                for df in silver_dfs.values():
                    df.unpersist()
                last_sig = sig
                log(f"batch selesai dalam {time.time() - started:.1f}s")
            else:
                log("tidak ada perubahan di bronze")
        except Exception:
            traceback.print_exc()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
