"""Konfigurasi & helper bersama untuk job medallion (bronze / silver / gold)."""
import json
import os
import urllib.error
import urllib.request

from pyspark.sql import SparkSession

BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
LAKE = f"s3a://{BUCKET}"

BRONZE_CDC = f"{LAKE}/bronze/cdc_events"
BRONZE_CHECKPOINT = f"{LAKE}/_checkpoints/bronze_cdc_events"
SILVER = f"{LAKE}/silver"
GOLD = f"{LAKE}/gold"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CDC_TOPIC_PATTERN = r"bpjs\.public\..*"

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")

TABLES = ("faskes", "peserta", "kunjungan")


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log(msg: str) -> None:
    print(f"[medallion] {msg}", flush=True)


# ---------------------------------------------------------------------
# Elasticsearch REST helper (stdlib saja, dipanggil dari driver)
# ---------------------------------------------------------------------
def es_request(method: str, path: str, body=None, ok_404: bool = False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{ES_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if ok_404 and e.code == 404:
            return None
        raise RuntimeError(f"ES {method} {path} -> {e.code}: {e.read().decode()}") from e
